"""SQLite state layer for the Hot Deal vehicle screening pipeline.

Persistent DB — VINs are tracked across daily runs. A VIN is added once
when it first appears on the VCH Marketing List and deleted when it drops
off (sold). The screening state survives across runs so we only screen
new VINs, not the entire list every day.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create tables if needed and return a connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA_SQL)
    return conn


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hot_deal_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'running',
    search_names    TEXT,
    total_vins      INTEGER DEFAULT 0,
    new_vins        INTEGER DEFAULT 0,
    passed_vins     INTEGER DEFAULT 0,
    rejected_vins   INTEGER DEFAULT 0,
    sold_vins       INTEGER DEFAULT 0,
    error_details   TEXT
);

CREATE TABLE IF NOT EXISTS hot_deal_vins (
    vin                 TEXT PRIMARY KEY,
    year                INTEGER,
    make                TEXT,
    model               TEXT,
    trim                TEXT,
    odometer            INTEGER,
    price               REAL,
    grade               TEXT,
    location            TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    tag                 TEXT,
    rejection_step      TEXT,
    rejection_reason    TEXT,
    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT NOT NULL,
    screened_at         TEXT,
    cr_data             TEXT,
    autocheck_data      TEXT,
    websearch_data      TEXT
);

CREATE INDEX IF NOT EXISTS idx_hd_vins_status ON hot_deal_vins(status);
"""


def create_run(conn: sqlite3.Connection, search_names: list[str]) -> str:
    """Start a new pipeline run and return its UUID."""
    run_id = uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO hot_deal_runs (run_id, started_at, status, search_names) VALUES (?, ?, 'running', ?)",
        (run_id, _utc_now_iso(), json.dumps(search_names)),
    )
    conn.commit()
    return run_id


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    status: str = "completed",
    *,
    new_vins: int = 0,
    sold_vins: int = 0,
    error_details: list[str] | None = None,
) -> None:
    """Mark a run as completed or failed and update aggregate counts."""
    cur = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status = 'hot_deal' THEN 1 ELSE 0 END) AS passed, "
        "SUM(CASE WHEN status LIKE '%_fail' THEN 1 ELSE 0 END) AS rejected "
        "FROM hot_deal_vins",
    )
    row = cur.fetchone()
    conn.execute(
        "UPDATE hot_deal_runs SET finished_at=?, status=?, total_vins=?, new_vins=?, "
        "passed_vins=?, rejected_vins=?, sold_vins=?, error_details=? WHERE run_id=?",
        (
            _utc_now_iso(),
            status,
            row["total"],
            new_vins,
            row["passed"],
            row["rejected"],
            sold_vins,
            json.dumps(error_details) if error_details else None,
            run_id,
        ),
    )
    conn.commit()


def get_active_vins(conn: sqlite3.Connection) -> set[str]:
    """Return all VINs currently in the database."""
    cur = conn.execute("SELECT vin FROM hot_deal_vins")
    return {row["vin"] for row in cur.fetchall()}


def delete_sold_vins(conn: sqlite3.Connection, vins: set[str]) -> int:
    """Hard-delete VINs no longer on today's list (sold/removed)."""
    if not vins:
        return 0
    placeholders = ",".join("?" for _ in vins)
    conn.execute(f"DELETE FROM hot_deal_vins WHERE vin IN ({placeholders})", list(vins))
    conn.commit()
    return len(vins)


def insert_new_vins(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert only VINs not already in the database. Returns count of new VINs."""
    now = _utc_now_iso()
    inserted = 0
    for r in rows:
        vin = r.get("vin", "").strip().upper()
        if not vin or len(vin) != 17:
            continue
        odometer = r.get("odometer")
        tag = "hot_deal_factory_warranty" if odometer and odometer < 35500 else "hot_deal_marketing"
        try:
            conn.execute(
                "INSERT OR IGNORE INTO hot_deal_vins "
                "(vin, year, make, model, trim, odometer, price, grade, location, "
                "status, tag, first_seen_at, last_seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
                (
                    vin,
                    r.get("year"),
                    r.get("make"),
                    r.get("model"),
                    r.get("trim"),
                    odometer,
                    r.get("price_asking") or r.get("price"),
                    r.get("condition_grade") or r.get("grade"),
                    r.get("location_state") or r.get("location"),
                    tag,
                    now,
                    now,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def touch_last_seen(conn: sqlite3.Connection, vins: set[str]) -> None:
    """Update last_seen_at for VINs that are still on today's list."""
    if not vins:
        return
    now = _utc_now_iso()
    placeholders = ",".join("?" for _ in vins)
    conn.execute(
        f"UPDATE hot_deal_vins SET last_seen_at=? WHERE vin IN ({placeholders})",
        [now, *list(vins)],
    )
    conn.commit()


def claim_next_pending(conn: sqlite3.Connection) -> dict | None:
    """Atomically claim the next pending VIN and set it to step1_running."""
    cur = conn.execute(
        "SELECT vin, year, make, model, trim, odometer, price, grade, location, tag "
        "FROM hot_deal_vins WHERE status = 'pending' LIMIT 1",
    )
    row = cur.fetchone()
    if row is None:
        return None
    vin = row["vin"]
    conn.execute(
        "UPDATE hot_deal_vins SET status='step1_running' WHERE vin=?",
        (vin,),
    )
    conn.commit()
    return dict(row)


def advance_status(
    conn: sqlite3.Connection,
    vin: str,
    new_status: str,
    *,
    rejection_step: str | None = None,
    rejection_reason: str | None = None,
    data_column: str | None = None,
    data_value: str | None = None,
) -> None:
    """Advance a VIN's status and optionally store step data."""
    fields = ["status=?"]
    params: list = [new_status]

    if rejection_step:
        fields.append("rejection_step=?")
        params.append(rejection_step)
    if rejection_reason:
        fields.append("rejection_reason=?")
        params.append(rejection_reason)
    if data_column and data_column in ("cr_data", "autocheck_data", "websearch_data"):
        fields.append(f"{data_column}=?")
        params.append(data_value)

    if new_status in ("hot_deal", "step1_fail", "step2_fail", "step3_fail"):
        fields.append("screened_at=?")
        params.append(_utc_now_iso())

    params.append(vin)
    conn.execute(
        f"UPDATE hot_deal_vins SET {', '.join(fields)} WHERE vin=?",
        params,
    )
    conn.commit()


def get_run_summary(conn: sqlite3.Connection, run_id: str) -> dict:
    """Return aggregate counts for a run."""
    cur = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM hot_deal_vins GROUP BY status",
    )
    counts = {row["status"]: row["cnt"] for row in cur.fetchall()}
    run_row = conn.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    return {
        "run_id": run_id,
        "status": run_row["status"] if run_row else "unknown",
        "started_at": run_row["started_at"] if run_row else None,
        "finished_at": run_row["finished_at"] if run_row else None,
        "total_vins": sum(counts.values()),
        "new_vins": run_row["new_vins"] if run_row else 0,
        "sold_vins": run_row["sold_vins"] if run_row else 0,
        "hot_deals": counts.get("hot_deal", 0),
        "step1_fail": counts.get("step1_fail", 0),
        "step2_fail": counts.get("step2_fail", 0),
        "step3_fail": counts.get("step3_fail", 0),
        "pending": counts.get("pending", 0),
        "in_progress": sum(v for k, v in counts.items() if k.endswith("_running")),
        "counts_by_status": counts,
    }


def get_hot_deals(conn: sqlite3.Connection) -> list[dict]:
    """Return all VINs that passed all 3 steps."""
    cur = conn.execute(
        "SELECT vin, year, make, model, trim, odometer, price, grade, location, tag "
        "FROM hot_deal_vins WHERE status='hot_deal' ORDER BY price",
    )
    return [dict(row) for row in cur.fetchall()]

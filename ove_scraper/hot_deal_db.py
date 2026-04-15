"""SQLite state layer for the Hot Deal vehicle screening pipeline."""
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
    conn.execute("PRAGMA foreign_keys=ON")
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
    passed_vins     INTEGER DEFAULT 0,
    rejected_vins   INTEGER DEFAULT 0,
    error_details   TEXT
);

CREATE TABLE IF NOT EXISTS hot_deal_vins (
    vin                 TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    year                INTEGER,
    make                TEXT,
    model               TEXT,
    trim                TEXT,
    odometer            INTEGER,
    price               REAL,
    grade               TEXT,
    location            TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    rejection_step      TEXT,
    rejection_reason    TEXT,
    step1_completed_at  TEXT,
    step2_completed_at  TEXT,
    step3_completed_at  TEXT,
    cr_data             TEXT,
    autocheck_data      TEXT,
    websearch_data      TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (vin, run_id),
    FOREIGN KEY (run_id) REFERENCES hot_deal_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_hd_vins_status ON hot_deal_vins(run_id, status);
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
    error_details: list[str] | None = None,
) -> None:
    """Mark a run as completed or failed and update aggregate counts."""
    cur = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status = 'hot_deal' THEN 1 ELSE 0 END) AS passed, "
        "SUM(CASE WHEN status LIKE '%_fail' THEN 1 ELSE 0 END) AS rejected "
        "FROM hot_deal_vins WHERE run_id = ?",
        (run_id,),
    )
    row = cur.fetchone()
    conn.execute(
        "UPDATE hot_deal_runs SET finished_at=?, status=?, total_vins=?, passed_vins=?, rejected_vins=?, error_details=? WHERE run_id=?",
        (
            _utc_now_iso(),
            status,
            row["total"],
            row["passed"],
            row["rejected"],
            json.dumps(error_details) if error_details else None,
            run_id,
        ),
    )
    conn.commit()


def insert_vins(conn: sqlite3.Connection, run_id: str, rows: list[dict]) -> int:
    """Bulk-insert VINs from CSV transform rows. Returns count inserted."""
    now = _utc_now_iso()
    inserted = 0
    for r in rows:
        vin = r.get("vin", "").strip().upper()
        if not vin or len(vin) != 17:
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO hot_deal_vins "
                "(vin, run_id, year, make, model, trim, odometer, price, grade, location, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (
                    vin,
                    run_id,
                    r.get("year"),
                    r.get("make"),
                    r.get("model"),
                    r.get("trim"),
                    r.get("odometer"),
                    r.get("price_asking") or r.get("price"),
                    r.get("condition_grade") or r.get("grade"),
                    r.get("location_state") or r.get("location"),
                    now,
                    now,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def claim_next_pending(conn: sqlite3.Connection, run_id: str) -> dict | None:
    """Atomically claim the next pending VIN and set it to step1_running."""
    cur = conn.execute(
        "SELECT vin, year, make, model, trim, odometer, price, grade, location "
        "FROM hot_deal_vins WHERE run_id = ? AND status = 'pending' LIMIT 1",
        (run_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    vin = row["vin"]
    conn.execute(
        "UPDATE hot_deal_vins SET status='step1_running', updated_at=? WHERE vin=? AND run_id=?",
        (_utc_now_iso(), vin, run_id),
    )
    conn.commit()
    return dict(row)


def advance_status(
    conn: sqlite3.Connection,
    vin: str,
    run_id: str,
    new_status: str,
    *,
    rejection_step: str | None = None,
    rejection_reason: str | None = None,
    data_column: str | None = None,
    data_value: str | None = None,
) -> None:
    """Advance a VIN's status and optionally store step data."""
    fields = ["status=?", "updated_at=?"]
    params: list = [new_status, _utc_now_iso()]

    if rejection_step:
        fields.append("rejection_step=?")
        params.append(rejection_step)
    if rejection_reason:
        fields.append("rejection_reason=?")
        params.append(rejection_reason)
    if data_column and data_column in ("cr_data", "autocheck_data", "websearch_data"):
        fields.append(f"{data_column}=?")
        params.append(data_value)

    # Set step completion timestamp
    step_ts_map = {
        "step1_pass": "step1_completed_at",
        "step1_fail": "step1_completed_at",
        "step2_pass": "step2_completed_at",
        "step2_fail": "step2_completed_at",
        "step3_pass": "step3_completed_at",
        "step3_fail": "step3_completed_at",
        "hot_deal": "step3_completed_at",
    }
    ts_col = step_ts_map.get(new_status)
    if ts_col:
        fields.append(f"{ts_col}=?")
        params.append(_utc_now_iso())

    params.extend([vin, run_id])
    conn.execute(
        f"UPDATE hot_deal_vins SET {', '.join(fields)} WHERE vin=? AND run_id=?",
        params,
    )
    conn.commit()


def get_run_summary(conn: sqlite3.Connection, run_id: str) -> dict:
    """Return aggregate counts for a run."""
    cur = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM hot_deal_vins WHERE run_id=? GROUP BY status",
        (run_id,),
    )
    counts = {row["status"]: row["cnt"] for row in cur.fetchall()}
    run_row = conn.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    return {
        "run_id": run_id,
        "status": run_row["status"] if run_row else "unknown",
        "started_at": run_row["started_at"] if run_row else None,
        "finished_at": run_row["finished_at"] if run_row else None,
        "total_vins": sum(counts.values()),
        "hot_deals": counts.get("hot_deal", 0),
        "step1_fail": counts.get("step1_fail", 0),
        "step2_fail": counts.get("step2_fail", 0),
        "step3_fail": counts.get("step3_fail", 0),
        "pending": counts.get("pending", 0),
        "in_progress": sum(v for k, v in counts.items() if k.endswith("_running")),
        "counts_by_status": counts,
    }


def get_hot_deals(conn: sqlite3.Connection, run_id: str) -> list[dict]:
    """Return all VINs that passed all 3 steps."""
    cur = conn.execute(
        "SELECT vin, year, make, model, trim, odometer, price, grade, location "
        "FROM hot_deal_vins WHERE run_id=? AND status='hot_deal' ORDER BY price",
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]

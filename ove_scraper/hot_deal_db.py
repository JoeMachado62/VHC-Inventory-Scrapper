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

    if new_status in ("hot_deal", "step1_fail", "step2_fail", "step3_fail", "scrape_failed"):
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
    # Surface the first error from error_details so the summary
    # template can show WHY a run failed — readers were getting
    # confused on 2026-04-25 when an export-step failure produced a
    # status='failed' summary that just printed lifetime DB counts.
    failure_reason: str | None = None
    if run_row and run_row["error_details"]:
        try:
            details = json.loads(run_row["error_details"])
            if isinstance(details, list) and details:
                failure_reason = str(details[0])[:200]
        except json.JSONDecodeError:
            failure_reason = str(run_row["error_details"])[:200]

    return {
        "run_id": run_id,
        "status": run_row["status"] if run_row else "unknown",
        "started_at": run_row["started_at"] if run_row else None,
        "finished_at": run_row["finished_at"] if run_row else None,
        "failure_reason": failure_reason,
        "total_vins": sum(counts.values()),
        "new_vins": run_row["new_vins"] if run_row else 0,
        "sold_vins": run_row["sold_vins"] if run_row else 0,
        "hot_deals": counts.get("hot_deal", 0),
        "step1_fail": counts.get("step1_fail", 0),
        "step2_fail": counts.get("step2_fail", 0),
        "step3_fail": counts.get("step3_fail", 0),
        "scrape_failed": counts.get("scrape_failed", 0),
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


def get_rejection_clusters_for_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    min_cluster_size: int = 10,
) -> list[dict]:
    """Return rejection-reason clusters within a run's time window.

    A "cluster" is a single ``rejection_reason`` string shared by
    ``min_cluster_size`` or more VINs that were screened during this
    run (between the run's ``started_at`` and ``finished_at``, or the
    current time if the run hasn't finished yet).

    The intent is to detect bug-pattern rejections — the 2026-04-26
    incident had 17 VINs falsely rejected with the identical reason
    "Structural damage reported" because of a parser regex that
    matched a UI label. Legit rejections rarely cluster this hard.

    Returns a list of {"reason": str, "count": int, "sample_vins":
    [vin, ...]} dicts, ordered by count desc.
    """
    started_row = conn.execute(
        "SELECT started_at, finished_at FROM hot_deal_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if not started_row:
        return []
    started_at = started_row["started_at"]
    finished_at = started_row["finished_at"] or _utc_now_iso()

    cur = conn.execute(
        """
        SELECT rejection_reason AS reason, COUNT(*) AS cnt
        FROM hot_deal_vins
        WHERE rejection_reason IS NOT NULL
          AND screened_at IS NOT NULL
          AND screened_at >= ?
          AND screened_at <= ?
        GROUP BY rejection_reason
        HAVING COUNT(*) >= ?
        ORDER BY cnt DESC
        """,
        (started_at, finished_at, min_cluster_size),
    )
    clusters = []
    for row in cur.fetchall():
        sample = conn.execute(
            "SELECT vin FROM hot_deal_vins WHERE rejection_reason=? "
            "AND screened_at >= ? AND screened_at <= ? LIMIT 5",
            (row["reason"], started_at, finished_at),
        ).fetchall()
        clusters.append({
            "reason": row["reason"],
            "count": row["cnt"],
            "sample_vins": [r["vin"] for r in sample],
        })
    return clusters


def reset_scrape_failed_to_pending(conn: sqlite3.Connection) -> int:
    """Reset any scrape_failed VINs to pending so they're re-claimed for
    screening.

    Called at the start of a run (and from the one-shot in-run retry
    pass) so a scraper-side failure — browser-level "couldn't open CR"
    or "VIN not found in OVE search" — gets another shot on the next
    daily run rather than being stuck forever. Distinct from step1_fail
    / step2_fail / step3_fail, which are real screener verdicts and
    stay terminal.

    Returns the number of VINs reset.
    """
    cur = conn.execute(
        "UPDATE hot_deal_vins SET status='pending', rejection_step=NULL, "
        "rejection_reason=NULL WHERE status='scrape_failed'"
    )
    conn.commit()
    return cur.rowcount or 0


def reclassify_scraper_failures_as_scrape_failed(conn: sqlite3.Connection) -> int:
    """One-time reclassifier: existing step1_fail rows whose
    rejection_reason looks like a scraper error (CR-click / VIN-not-
    found / page-load timeout) are moved to status='scrape_failed' so
    they become eligible for re-screening on the next run.

    Used by the hot-deal-reprocess CLI to recover from the 2026-04-23
    incident where ~69 VINs were terminally rejected because the CR
    popup couldn't be opened after the max-attempts cap was lowered
    to 2. Returns the number of rows reclassified.

    Patterns are anchored with leading '%' wildcards so they match
    regardless of any prefix the pipeline adds (e.g. the 2026-04-23
    "ConditionReportClickFailedError on retry: ..." prefix added by
    _screen_vin_with_classification on its second-strike path). The
    earlier prefix-anchored patterns missed those rows, leaving 68
    stuck in step1_fail through 2026-04-25.
    """
    cur = conn.execute(
        "UPDATE hot_deal_vins SET status='scrape_failed' "
        "WHERE status='step1_fail' "
        "AND ("
        "  rejection_reason LIKE '%Could not open OVE condition report%' "
        "  OR rejection_reason LIKE '%is not available in OVE search results%' "
        "  OR rejection_reason LIKE '%ConditionReportClickFailedError%' "
        "  OR rejection_reason LIKE '%ListingNotFoundError%' "
        "  OR rejection_reason LIKE '%Page.goto: Timeout%'"
        ")"
    )
    conn.commit()
    return cur.rowcount or 0

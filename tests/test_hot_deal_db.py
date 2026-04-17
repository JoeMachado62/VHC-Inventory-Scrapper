"""Tests for the Hot Deal SQLite state layer (persistent DB design)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ove_scraper.hot_deal_db import (
    advance_status,
    claim_next_pending,
    create_run,
    delete_sold_vins,
    finish_run,
    get_active_vins,
    get_hot_deals,
    get_run_summary,
    init_db,
    insert_new_vins,
    touch_last_seen,
)


@pytest.fixture()
def db(tmp_path: Path) -> sqlite3.Connection:
    return init_db(tmp_path / "test.db")


def test_init_creates_tables(db: sqlite3.Connection) -> None:
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "hot_deal_runs" in tables
    assert "hot_deal_vins" in tables


def test_create_and_finish_run(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["VCH Marketing List"])
    assert len(run_id) == 12
    row = db.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    assert row["status"] == "running"

    finish_run(db, run_id, "completed", new_vins=5, sold_vins=2)
    row = db.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    assert row["status"] == "completed"
    assert row["new_vins"] == 5
    assert row["sold_vins"] == 2


def test_insert_new_vins_and_dedup(db: sqlite3.Connection) -> None:
    rows = [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord", "odometer": 25000},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla", "odometer": 40000},
        {"vin": "INVALID", "year": 2023, "make": "Bad", "model": "VIN"},
    ]
    inserted = insert_new_vins(db, rows)
    assert inserted == 2  # INVALID skipped

    # Insert same VINs again — should not duplicate
    inserted2 = insert_new_vins(db, rows)
    assert inserted2 == 0
    count = db.execute("SELECT COUNT(*) FROM hot_deal_vins").fetchone()[0]
    assert count == 2


def test_auto_tagging_by_odometer(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "odometer": 25000},  # < 35500
        {"vin": "2T1BU4EE9DC123456", "odometer": 45000},  # >= 35500
    ])
    r1 = db.execute("SELECT tag FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    r2 = db.execute("SELECT tag FROM hot_deal_vins WHERE vin='2T1BU4EE9DC123456'").fetchone()
    assert r1["tag"] == "hot_deal_factory_warranty"
    assert r2["tag"] == "hot_deal_marketing"


def test_get_active_vins(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
    ])
    active = get_active_vins(db)
    assert active == {"1HGCG5655WA041389", "2T1BU4EE9DC123456"}


def test_delete_sold_vins(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
    ])
    deleted = delete_sold_vins(db, {"1HGCG5655WA041389"})
    assert deleted == 1
    remaining = get_active_vins(db)
    assert remaining == {"2T1BU4EE9DC123456"}


def test_claim_next_pending(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
    ])
    claimed = claim_next_pending(db)
    assert claimed is not None
    assert claimed["vin"] == "1HGCG5655WA041389"

    row = db.execute("SELECT status FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    assert row["status"] == "step1_running"

    # No more pending
    assert claim_next_pending(db) is None


def test_advance_status(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db)

    advance_status(db, "1HGCG5655WA041389", "step1_pass")
    row = db.execute("SELECT status FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    assert row["status"] == "step1_pass"


def test_advance_to_fail_with_reason(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db)

    advance_status(
        db, "1HGCG5655WA041389", "step1_fail",
        rejection_step="step1", rejection_reason="As-Is vehicle",
    )
    row = db.execute(
        "SELECT status, rejection_step, rejection_reason, screened_at FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'"
    ).fetchone()
    assert row["status"] == "step1_fail"
    assert row["rejection_reason"] == "As-Is vehicle"
    assert row["screened_at"] is not None


def test_advance_to_hot_deal(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db)
    advance_status(db, "1HGCG5655WA041389", "hot_deal")

    deals = get_hot_deals(db)
    assert len(deals) == 1
    assert deals[0]["vin"] == "1HGCG5655WA041389"


def test_touch_last_seen(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    row_before = db.execute("SELECT last_seen_at FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    import time; time.sleep(0.01)
    touch_last_seen(db, {"1HGCG5655WA041389"})
    row_after = db.execute("SELECT last_seen_at FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    assert row_after["last_seen_at"] >= row_before["last_seen_at"]


def test_reconciliation_flow(db: sqlite3.Connection) -> None:
    """Full reconciliation: old VINs sold, new VINs added, existing untouched."""
    # Day 1: initial load
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
    ])

    # Day 2: Honda sold, Kia added, Toyota still there
    today = {"2T1BU4EE9DC123456", "5XYP5DGC6SG648995"}
    stored = get_active_vins(db)

    sold = stored - today
    assert sold == {"1HGCG5655WA041389"}
    delete_sold_vins(db, sold)

    new = today - stored
    assert new == {"5XYP5DGC6SG648995"}
    insert_new_vins(db, [{"vin": "5XYP5DGC6SG648995", "year": 2025, "make": "Kia", "model": "Telluride"}])

    active = get_active_vins(db)
    assert active == {"2T1BU4EE9DC123456", "5XYP5DGC6SG648995"}


def test_rejected_vin_not_reclaimed(db: sqlite3.Connection) -> None:
    """A VIN rejected in a previous run should NOT be reclaimed as pending."""
    insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db)
    advance_status(db, "1HGCG5655WA041389", "step1_fail", rejection_step="step1", rejection_reason="As-Is")

    # Next day the same VIN is still on the list — insert_new_vins ignores it
    inserted = insert_new_vins(db, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    assert inserted == 0

    # claim_next_pending returns None — the rejected VIN is not pending
    assert claim_next_pending(db) is None

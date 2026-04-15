"""Tests for the Hot Deal SQLite state layer."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ove_scraper.hot_deal_db import (
    advance_status,
    claim_next_pending,
    create_run,
    finish_run,
    get_hot_deals,
    get_run_summary,
    init_db,
    insert_vins,
)


@pytest.fixture()
def db(tmp_path: Path) -> sqlite3.Connection:
    return init_db(tmp_path / "test.db")


def test_init_creates_tables(db: sqlite3.Connection) -> None:
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "hot_deal_runs" in tables
    assert "hot_deal_vins" in tables


def test_create_and_finish_run(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["Search A", "Search B"])
    assert len(run_id) == 12
    row = db.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    assert row["status"] == "running"

    finish_run(db, run_id, "completed")
    row = db.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    assert row["status"] == "completed"
    assert row["finished_at"] is not None


def test_insert_and_claim_vins(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    rows = [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
        {"vin": "INVALID", "year": 2023, "make": "Bad", "model": "VIN"},  # should be skipped
    ]
    inserted = insert_vins(db, run_id, rows)
    assert inserted == 2  # INVALID VIN skipped (< 17 chars)

    # Claim first
    claimed = claim_next_pending(db, run_id)
    assert claimed is not None
    assert claimed["vin"] in ("1HGCG5655WA041389", "2T1BU4EE9DC123456")

    # Check status changed to step1_running
    row = db.execute(
        "SELECT status FROM hot_deal_vins WHERE vin=? AND run_id=?",
        (claimed["vin"], run_id),
    ).fetchone()
    assert row["status"] == "step1_running"


def test_advance_status(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    insert_vins(db, run_id, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db, run_id)

    advance_status(db, "1HGCG5655WA041389", run_id, "step1_pass")
    row = db.execute(
        "SELECT status, step1_completed_at FROM hot_deal_vins WHERE vin=? AND run_id=?",
        ("1HGCG5655WA041389", run_id),
    ).fetchone()
    assert row["status"] == "step1_pass"
    assert row["step1_completed_at"] is not None


def test_advance_to_fail_with_reason(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    insert_vins(db, run_id, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    claim_next_pending(db, run_id)

    advance_status(
        db, "1HGCG5655WA041389", run_id, "step1_fail",
        rejection_step="step1", rejection_reason="As-Is vehicle",
    )
    row = db.execute(
        "SELECT status, rejection_step, rejection_reason FROM hot_deal_vins WHERE vin=? AND run_id=?",
        ("1HGCG5655WA041389", run_id),
    ).fetchone()
    assert row["status"] == "step1_fail"
    assert row["rejection_step"] == "step1"
    assert row["rejection_reason"] == "As-Is vehicle"


def test_get_run_summary(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    insert_vins(db, run_id, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
    ])
    claim_next_pending(db, run_id)
    advance_status(db, "1HGCG5655WA041389", run_id, "hot_deal")

    summary = get_run_summary(db, run_id)
    assert summary["hot_deals"] == 1
    assert summary["pending"] == 1
    assert summary["total_vins"] == 2


def test_get_hot_deals(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    insert_vins(db, run_id, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord", "price_asking": 15000},
    ])
    claim_next_pending(db, run_id)
    advance_status(db, "1HGCG5655WA041389", run_id, "hot_deal")

    deals = get_hot_deals(db, run_id)
    assert len(deals) == 1
    assert deals[0]["vin"] == "1HGCG5655WA041389"


def test_claim_returns_none_when_empty(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    assert claim_next_pending(db, run_id) is None


def test_duplicate_vin_ignored(db: sqlite3.Connection) -> None:
    run_id = create_run(db, ["test"])
    insert_vins(db, run_id, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    insert_vins(db, run_id, [{"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"}])
    count = db.execute("SELECT COUNT(*) FROM hot_deal_vins WHERE run_id=?", (run_id,)).fetchone()[0]
    assert count == 1

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
    get_rejection_clusters_for_run,
    get_run_summary,
    init_db,
    insert_new_vins,
    reclassify_scraper_failures_as_scrape_failed,
    reset_scrape_failed_to_pending,
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


def test_reset_scrape_failed_to_pending(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389", "year": 2023, "make": "Honda", "model": "Accord"},
        {"vin": "2T1BU4EE9DC123456", "year": 2024, "make": "Toyota", "model": "Corolla"},
        {"vin": "3FMCR9B62SRA21234", "year": 2025, "make": "Ford", "model": "Bronco"},
    ])
    # Simulate one scrape_failed, one step1_fail, one pending
    advance_status(db, "1HGCG5655WA041389", "scrape_failed",
                   rejection_step="scraper_error",
                   rejection_reason="Could not open OVE condition report")
    advance_status(db, "2T1BU4EE9DC123456", "step1_fail",
                   rejection_step="step1", rejection_reason="Salvage vehicle")
    # 3FMCR9B62SRA21234 stays pending

    moved = reset_scrape_failed_to_pending(db)
    assert moved == 1

    # scrape_failed VIN back to pending with cleared rejection fields
    honda = db.execute("SELECT status, rejection_reason FROM hot_deal_vins WHERE vin='1HGCG5655WA041389'").fetchone()
    assert honda["status"] == "pending"
    assert honda["rejection_reason"] is None

    # step1_fail VIN unchanged — real screener verdict stays terminal
    toyota = db.execute("SELECT status, rejection_reason FROM hot_deal_vins WHERE vin='2T1BU4EE9DC123456'").fetchone()
    assert toyota["status"] == "step1_fail"
    assert toyota["rejection_reason"] == "Salvage vehicle"


def test_reclassify_scraper_failures_as_scrape_failed(db: sqlite3.Connection) -> None:
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389"},  # scraper error (CR-click failed)
        {"vin": "2T1BU4EE9DC123456"},  # scraper error (VIN not found in OVE)
        {"vin": "3FMCR9B62SRA21234"},  # real salvage rejection — stays
        {"vin": "4JGFF5KE6PA867403"},  # engine/drivetrain false positive — stays, needs --rescreen
    ])
    advance_status(db, "1HGCG5655WA041389", "step1_fail",
                   rejection_step="scraper_error",
                   rejection_reason="Could not open OVE condition report after 2 click attempts; intended_href=...")
    advance_status(db, "2T1BU4EE9DC123456", "step1_fail",
                   rejection_step="scraper_error",
                   rejection_reason="VIN 2T1BU4EE9DC123456 is not available in OVE search results")
    advance_status(db, "3FMCR9B62SRA21234", "step1_fail",
                   rejection_step="step1", rejection_reason="Salvage vehicle (listing flag)")
    advance_status(db, "4JGFF5KE6PA867403", "step1_fail",
                   rejection_step="step1", rejection_reason="Engine/drivetrain issue detected")

    moved = reclassify_scraper_failures_as_scrape_failed(db)
    assert moved == 2  # CR-click + VIN-not-found patterns

    statuses = {
        r["vin"]: r["status"]
        for r in db.execute("SELECT vin, status FROM hot_deal_vins").fetchall()
    }
    assert statuses["1HGCG5655WA041389"] == "scrape_failed"
    assert statuses["2T1BU4EE9DC123456"] == "scrape_failed"
    assert statuses["3FMCR9B62SRA21234"] == "step1_fail"  # real screener verdict preserved
    assert statuses["4JGFF5KE6PA867403"] == "step1_fail"  # false positive needs --rescreen path


def test_scrape_failed_not_counted_as_rejected_in_run_summary(db: sqlite3.Connection) -> None:
    # Regression: finish_run's rejected_vins count uses LIKE '%_fail'
    # which must not match 'scrape_failed' (ends in '_failed').
    insert_new_vins(db, [
        {"vin": "1HGCG5655WA041389"},
        {"vin": "2T1BU4EE9DC123456"},
    ])
    advance_status(db, "1HGCG5655WA041389", "scrape_failed",
                   rejection_step="scraper_error", rejection_reason="whatever")
    advance_status(db, "2T1BU4EE9DC123456", "step1_fail",
                   rejection_step="step1", rejection_reason="salvage")
    run_id = create_run(db, ["VCH Marketing List"])
    finish_run(db, run_id, "completed", new_vins=2, sold_vins=0)
    summary = get_run_summary(db, run_id)
    # Only the real step1_fail counts as rejected; scrape_failed is surfaced separately.
    run_row = db.execute("SELECT * FROM hot_deal_runs WHERE run_id=?", (run_id,)).fetchone()
    assert run_row["rejected_vins"] == 1
    assert summary["scrape_failed"] == 1
    assert summary["step1_fail"] == 1


def test_get_rejection_clusters_for_run_finds_buggy_clusters(db: sqlite3.Connection) -> None:
    """Regression test: 17 VINs falsely failed step1 with the same reason
    on 2026-04-26 because of a parser regex bug. The cluster query is
    what an alert wires off — it must surface that pattern.
    """
    run_id = create_run(db, ["VCH Marketing List"])

    # 12 VINs falsely fail with the same reason (above the default
    # threshold of 10) — this is what the alert should flag.
    cluster_vins = [f"BUG{i:014d}" for i in range(12)]
    insert_new_vins(db, [{"vin": v, "year": 2024, "make": "Ford", "model": "F-150"} for v in cluster_vins])
    for v in cluster_vins:
        claim_next_pending(db)
        advance_status(
            db, v, "step1_fail",
            rejection_step="step1",
            rejection_reason="Structural damage reported",
        )

    # 3 unrelated step3 rejections that should NOT cluster (different
    # reason strings) — these are real rejections, must be ignored.
    legit_vins = [f"OK{i:015d}" for i in range(3)]
    insert_new_vins(db, [{"vin": v, "year": 2024, "make": "Honda", "model": "Civic"} for v in legit_vins])
    for i, v in enumerate(legit_vins):
        claim_next_pending(db)
        advance_status(
            db, v, "step3_fail",
            rejection_step="step3",
            rejection_reason=f"VIN found on salvage site(s): site{i}.com",
        )

    finish_run(db, run_id, "completed")

    clusters = get_rejection_clusters_for_run(db, run_id, min_cluster_size=10)
    assert len(clusters) == 1
    assert clusters[0]["count"] == 12
    assert clusters[0]["reason"] == "Structural damage reported"
    assert len(clusters[0]["sample_vins"]) == 5  # capped at 5 per the implementation


def test_get_rejection_clusters_for_run_respects_threshold(db: sqlite3.Connection) -> None:
    """A cluster of 9 with the default threshold of 10 should NOT trip."""
    run_id = create_run(db, ["VCH Marketing List"])
    vins = [f"BELOW{i:012d}" for i in range(9)]
    insert_new_vins(db, [{"vin": v, "year": 2024, "make": "Ford", "model": "F-150"} for v in vins])
    for v in vins:
        claim_next_pending(db)
        advance_status(db, v, "step1_fail", rejection_step="step1", rejection_reason="As-Is vehicle")
    finish_run(db, run_id, "completed")

    assert get_rejection_clusters_for_run(db, run_id, min_cluster_size=10) == []
    # Lowering threshold below the cluster size DOES trip
    clusters = get_rejection_clusters_for_run(db, run_id, min_cluster_size=5)
    assert len(clusters) == 1 and clusters[0]["count"] == 9


def test_get_rejection_clusters_for_run_scopes_to_run_window(db: sqlite3.Connection) -> None:
    """Rejections outside the run's started_at..finished_at window are
    excluded — clusters from prior runs must not bleed in.
    """
    # Run 1: 12 same-reason rejections, then finish
    run1_id = create_run(db, ["VCH Marketing List"])
    old_vins = [f"OLD{i:014d}" for i in range(12)]
    insert_new_vins(db, [{"vin": v, "year": 2024, "make": "Ford", "model": "F-150"} for v in old_vins])
    for v in old_vins:
        claim_next_pending(db)
        advance_status(db, v, "step1_fail", rejection_step="step1", rejection_reason="Old run reason")
    finish_run(db, run1_id, "completed")

    # Run 2: only 2 rejections in its window, with DIFFERENT reason
    import time; time.sleep(0.01)
    run2_id = create_run(db, ["VCH Marketing List"])
    new_vins = [f"NEW{i:014d}" for i in range(2)]
    insert_new_vins(db, [{"vin": v, "year": 2024, "make": "Ford", "model": "F-150"} for v in new_vins])
    for v in new_vins:
        claim_next_pending(db)
        advance_status(db, v, "step1_fail", rejection_step="step1", rejection_reason="New run reason")
    finish_run(db, run2_id, "completed")

    # Run 2 should NOT see Run 1's cluster
    assert get_rejection_clusters_for_run(db, run2_id, min_cluster_size=2) == [
        {"reason": "New run reason", "count": 2,
         "sample_vins": [v for v in new_vins]}
    ]
    # Run 1 still sees its own cluster
    assert get_rejection_clusters_for_run(db, run1_id, min_cluster_size=10)[0]["count"] == 12

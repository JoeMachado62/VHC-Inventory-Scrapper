"""Tests for the Hot Deals VPS-push payload builder.

Per HOT_DEALS_SCRAPER_CONTRACT.md (top-level repo file). The pipeline
runner persists per-VIN ``payload-data.json`` artifacts as VINs reach
``status='hot_deal'``; this module assembles them into the batch the
VPS expects.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ove_scraper.hot_deal_payload import (
    DEFAULT_MIN_DELTA_BELOW_MMR,
    EXCLUDED_NEGATIVE_SIGNALS,
    EXCLUDED_SIGNALS_CHECKED,
    NEGATIVE_CR_FILTER_VERSION,
    build_deal_entry,
    build_hot_deals_batch,
    build_persisted_payload_data,
    build_pricing,
    deal_label_for_pct,
)


def _make_payload_data(
    vin: str = "1FT8W2BN0PEC12345",
    *,
    buy_now_price: float = 56049.0,
    mmr_high: float = 58500.0,
    auction_end: str | None = "2026-04-26T20:00:00Z",
    auction_start: str | None = "2026-04-26T14:00:00Z",
    listing_id: str | None = "OVE.BAA.440312706",
    year: int = 2025,
    make: str = "Ford",
    model: str = "F-250",
    trim: str = "XL",
    odometer: int = 22140,
) -> dict:
    """Build a payload-data dict shaped like what the pipeline persists."""
    return {
        "vin": vin,
        "source_platform": "manheim",
        "captured_at": "2026-04-26T11:00:00+00:00",
        "listing_json": {
            "vin": vin,
            "year": year,
            "make": make,
            "model": model,
            "trim": trim,
            "bodyStyle": "Pickup",
            "odometer": odometer,
            "crRating": 4.2,
            "buyNowPrice": buy_now_price,
            "priceRange": {"adjustedHigh": mmr_high, "adjustedLow": mmr_high - 5000},
            "auctionStartTime": auction_start,
            "auctionEndTime": auction_end,
            "endTime": auction_end,
            "listingId": listing_id,
            "pickupLocationCity": "Orlando",
            "pickupLocationState": "FL",
            "pickupLocationZip": "33101",
            "driveTrain": "4WD",
            "transmission": "Automatic",
        },
        "vin_row": {
            "vin": vin,
            "year": year,
            "make": make,
            "model": model,
            "trim": trim,
            "odometer": odometer,
            "price_asking": buy_now_price,
        },
        "deep_scrape": {
            "images": [],
            "seller_comments": "Runs and drives well.",
            "condition_report": {
                "overall_grade": "4.2",
                "structural_damage": False,
                "title_status": "Clean",
                "announcements": [],
                "tire_depths": {
                    "lf": {"position_label": "LF", "tread_depth": "7/32"},
                },
            },
            "listing_snapshot": {
                "title": f"{year} {make} {model} {trim}",
                "page_url": "https://www.ove.com/...",
            },
        },
    }


# ---------------------------------------------------------------------------
# deal_label_for_pct
# ---------------------------------------------------------------------------

class TestDealLabel:
    def test_excellent_for_10_percent_or_more(self):
        assert deal_label_for_pct(10.0) == "Excellent"
        assert deal_label_for_pct(15.5) == "Excellent"

    def test_great_for_5_to_10_percent(self):
        assert deal_label_for_pct(5.0) == "Great"
        assert deal_label_for_pct(9.99) == "Great"

    def test_good_for_1_to_5_percent(self):
        assert deal_label_for_pct(1.0) == "Good"
        assert deal_label_for_pct(4.19) == "Good"  # contract example

    def test_fair_below_1_percent(self):
        assert deal_label_for_pct(0.5) == "Fair"
        assert deal_label_for_pct(0.0) == "Fair"

    def test_unranked_when_none(self):
        assert deal_label_for_pct(None) == "Unranked"


# ---------------------------------------------------------------------------
# build_pricing
# ---------------------------------------------------------------------------

class TestBuildPricing:
    def test_contract_example_values(self):
        # The contract example: mmr=58500, ask=56049 -> delta=2451, pct=4.19
        pricing = build_pricing(asking_price=56049.0, mmr_value=58500.0)
        assert pricing is not None
        assert pricing["mmr_value"] == 58500.0
        assert pricing["asking_price"] == 56049.0
        assert pricing["deal_delta"] == 2451.0
        assert pricing["deal_delta_pct"] == 4.19
        assert pricing["deal_label"] == "Good"
        # deal_rank is filled at batch time, not here
        assert pricing["deal_rank"] is None

    def test_returns_none_on_missing_mmr(self):
        assert build_pricing(asking_price=100.0, mmr_value=None) is None

    def test_returns_none_on_missing_asking(self):
        assert build_pricing(asking_price=None, mmr_value=100.0) is None


# ---------------------------------------------------------------------------
# build_deal_entry — required-field gating + extraction
# ---------------------------------------------------------------------------

class TestBuildDealEntry:
    def test_happy_path_extracts_all_required_fields(self):
        data = _make_payload_data()
        data["deep_scrape"]["images"] = ["https://img.example.com/1.jpg"]
        entry = build_deal_entry(data)
        assert entry is not None
        assert entry["vin"] == "1FT8W2BN0PEC12345"
        assert entry["auction_end_at"] == "2026-04-26T20:00:00Z"
        assert entry["listing_id"] == "OVE.BAA.440312706"
        assert entry["listing_url"].endswith("#/details/1FT8W2BN0PEC12345/OVE")
        assert entry["vehicle"]["year"] == 2025
        assert entry["vehicle"]["make"] == "Ford"
        assert entry["vehicle"]["model"] == "F-250"
        assert entry["vehicle"]["price_asking"] == 56049.0
        assert entry["vehicle"]["location_state"] == "FL"
        assert entry["vehicle"]["features_normalized"]["pickup_location"] == "Orlando, FL"
        assert entry["vehicle"]["features_normalized"]["mmr"] == 58500.0
        assert entry["pricing"]["mmr_value"] == 58500.0
        assert entry["pricing"]["asking_price"] == 56049.0
        assert entry["pricing"]["deal_delta"] == 2451.0
        assert entry["pricing"]["deal_label"] == "Good"
        assert entry["cr_screen"]["status"] == "passed"
        assert entry["cr_screen"]["version"] == NEGATIVE_CR_FILTER_VERSION
        assert entry["cr_screen"]["reasons"] == []
        assert "structural_damage" in entry["cr_screen"]["excluded_signals_checked"]
        assert entry["detail"]["images"] == [{
            "url": "https://img.example.com/1.jpg",
            "role": "hero",
            "display_order": 0,
            "is_primary": True,
            "source_image_id": None,
            "metadata": {},
        }]
        # Positive highlights derived from the deep-scrape CR
        assert any("structural" in h.lower() for h in entry["cr_screen"]["positive_highlights"])
        assert any("clean title" in h.lower() for h in entry["cr_screen"]["positive_highlights"])

    def test_skips_when_auction_end_at_missing(self):
        data = _make_payload_data(auction_end=None)
        # Also clear the endTime fallback so we truly have no end
        data["listing_json"]["endTime"] = None
        assert build_deal_entry(data) is None

    def test_skips_when_pricing_unavailable(self):
        data = _make_payload_data()
        data["listing_json"]["buyNowPrice"] = None
        data["vin_row"]["price_asking"] = None
        assert build_deal_entry(data) is None

    def test_skips_when_below_minimum_delta(self):
        # Asking $58000 vs MMR $58500 -> delta=$500 < min=$1000
        data = _make_payload_data(buy_now_price=58000.0, mmr_high=58500.0)
        assert build_deal_entry(data) is None

    def test_skips_when_required_vehicle_basics_missing(self):
        data = _make_payload_data()
        data["listing_json"]["year"] = None
        data["vin_row"]["year"] = None
        assert build_deal_entry(data) is None

    def test_skips_when_invalid_vin(self):
        data = _make_payload_data(vin="TOO_SHORT")
        assert build_deal_entry(data) is None

    def test_falls_back_to_endTime_when_auctionEndTime_absent(self):
        data = _make_payload_data(auction_end="2026-04-30T20:00:00Z")
        data["listing_json"]["auctionEndTime"] = None
        # endTime is still set (the example uses it as a fallback)
        entry = build_deal_entry(data)
        assert entry is not None
        assert entry["auction_end_at"] == "2026-04-30T20:00:00Z"


# ---------------------------------------------------------------------------
# build_hot_deals_batch — full batch shape + ranking
# ---------------------------------------------------------------------------

class TestBuildHotDealsBatch:
    def test_batch_contract_top_level_shape(self):
        scraped_at = datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc)
        payload, skipped = build_hot_deals_batch(
            [_make_payload_data()],
            batch_id="vhc-marketing-2026-04-26-0900Z",
            scraped_at=scraped_at,
        )
        assert payload["source_list_name"] == "VHC Marketing List"
        assert payload["source_platform"] == "manheim"
        assert payload["batch_id"] == "vhc-marketing-2026-04-26-0900Z"
        assert payload["snapshot_mode"] == "full_replace"
        assert payload["scraped_at"] == "2026-04-26T09:00:00+00:00"
        assert payload["filter_rules"]["minimum_delta_below_mmr"] == DEFAULT_MIN_DELTA_BELOW_MMR
        assert payload["filter_rules"]["negative_cr_filter_version"] == NEGATIVE_CR_FILTER_VERSION
        assert payload["filter_rules"]["excluded_if"] == EXCLUDED_NEGATIVE_SIGNALS
        assert len(payload["deals"]) == 1
        assert skipped == []

    def test_deal_rank_assigned_by_pct_below_mmr(self):
        # Three VINs at MMR=$20,000 to keep all deltas comfortably above
        # the $1,000 minimum-delta threshold. Best deal ranked first.
        # mmr=20000 ask=18000 -> $2000 / 10% (Excellent)
        deal_excellent = _make_payload_data(
            vin="1FAKEVIN1XCEL0001",  # 17 chars
            buy_now_price=18000.0,
            mmr_high=20000.0,
        )
        # mmr=20000 ask=19000 -> $1000 / 5% (Great)
        deal_great = _make_payload_data(
            vin="2FAKEVIN2GREAT002",
            buy_now_price=19000.0,
            mmr_high=20000.0,
        )
        # mmr=20000 ask=16000 -> $4000 / 20% (Excellent), should rank #1
        deal_top = _make_payload_data(
            vin="3FAKEVIN3FIRST003",
            buy_now_price=16000.0,
            mmr_high=20000.0,
        )
        # Stamp the listing_json + vin_row with the same VINs.
        for d in (deal_excellent, deal_great, deal_top):
            d["listing_json"]["vin"] = d["vin"]
            d["vin_row"]["vin"] = d["vin"]
        payload, skipped = build_hot_deals_batch(
            [deal_excellent, deal_great, deal_top],
            batch_id="rank-test",
        )
        assert skipped == []
        assert len(payload["deals"]) == 3
        ranks = [(d["vin"], d["pricing"]["deal_rank"], d["pricing"]["deal_delta_pct"])
                 for d in payload["deals"]]
        # rank 1 = best deal (highest pct)
        assert ranks[0][1] == 1
        assert ranks[0][2] == 20.0  # $4000/$20k = 20% deal first
        assert ranks[1][1] == 2
        assert ranks[1][2] == 10.0  # $2000/$20k = 10%
        assert ranks[2][1] == 3
        assert ranks[2][2] == 5.0   # $1000/$20k = 5%

    def test_returns_skipped_vins_for_unbuildable_entries(self):
        good = _make_payload_data(vin="1FAKEVIN1GOOD0001")
        bad_no_end = _make_payload_data(vin="2FAKEVIN2BADEND02", auction_end=None)
        bad_no_end["listing_json"]["endTime"] = None
        bad_no_end["listing_json"]["saleEndTime"] = None
        bad_no_end["listing_json"]["closeTime"] = None
        payload, skipped = build_hot_deals_batch(
            [good, bad_no_end],
            batch_id="skip-test",
        )
        assert len(payload["deals"]) == 1
        assert payload["deals"][0]["vin"] == "1FAKEVIN1GOOD0001"
        assert skipped == ["2FAKEVIN2BADEND02"]

    def test_empty_input_produces_empty_deals_list(self):
        payload, skipped = build_hot_deals_batch([], batch_id="empty-test")
        assert payload["deals"] == []
        assert skipped == []
        # filter_rules block is still present so the VPS sees the version
        assert "filter_rules" in payload

    def test_default_scraped_at_uses_current_utc(self):
        before = datetime.now(timezone.utc)
        payload, _ = build_hot_deals_batch([_make_payload_data()], batch_id="ts-test")
        after = datetime.now(timezone.utc)
        scraped = datetime.fromisoformat(payload["scraped_at"])
        assert before <= scraped <= after


# ---------------------------------------------------------------------------
# MMR schema variants — regression for the 2026-04-26 silent VPS-push failure
# ---------------------------------------------------------------------------

def _make_payload_data_mmrPrice_schema(
    vin: str = "1FT8W2BN0PEC12345",
    *,
    mmr_price: float = 58500.0,
    buy_now_price: float = 56049.0,
) -> dict:
    """Payload-data shaped like the OVE listings actually return today.

    Verified empirically on 2026-04-26: 99/99 hot_deal listings exposed
    ``mmrPrice`` / ``averageMMRValuation`` and 0/99 had ``priceRange``.
    The original ``_extract_mmr`` only looked at ``priceRange`` with
    ``mmrValue`` / ``mmr`` fallbacks — none of which exist in this
    schema — so every VIN was silently dropped at batch-build time and
    the VPS push sent an empty batch. This fixture locks in the schema
    variant that was missed.
    """
    base = _make_payload_data(vin=vin, buy_now_price=buy_now_price)
    # Strip the priceRange block; the production schema doesn't include it.
    base["listing_json"].pop("priceRange", None)
    # Add the flat fields production listings actually expose.
    base["listing_json"]["mmrPrice"] = mmr_price
    base["listing_json"]["averageMMRValuation"] = mmr_price
    base["listing_json"]["aboveMmr"] = mmr_price + 4000.0
    base["listing_json"]["belowMmr"] = mmr_price - 4000.0
    return base


class TestMmrSchemaVariants:
    def test_mmrPrice_schema_produces_a_deal(self):
        # The 2026-04-26 production schema. Before the fix, batch was empty.
        payload, skipped = build_hot_deals_batch(
            [_make_payload_data_mmrPrice_schema(buy_now_price=56049.0, mmr_price=58500.0)],
            batch_id="mmrPrice-schema-test",
        )
        assert skipped == [], "no VINs should be skipped on the production schema"
        assert len(payload["deals"]) == 1
        deal = payload["deals"][0]
        assert deal["pricing"]["mmr_value"] == 58500.0
        assert deal["pricing"]["asking_price"] == 56049.0
        assert deal["pricing"]["deal_delta"] == 2451.0  # 58500 - 56049

    def test_priceRange_schema_still_works_for_backwards_compat(self):
        # The legacy schema we originally coded against. Some listings may
        # still use it; either schema should produce a deal.
        payload, skipped = build_hot_deals_batch(
            [_make_payload_data(buy_now_price=56049.0, mmr_high=58500.0)],
            batch_id="priceRange-schema-test",
        )
        assert skipped == []
        assert len(payload["deals"]) == 1

    def test_smoke_test_against_live_payload_files(self, tmp_path):
        """Catch any future MMR/payload schema drift at commit time.

        Walks the artifacts/hot-deal/<VIN>/payload-data.json directory if
        present and asserts that build_hot_deals_batch produces a non-zero
        number of deals (or returns no input). Skips when the directory
        is empty or absent so the test stays usable in fresh checkouts
        and CI runs without artifacts.
        """
        import json
        from pathlib import Path
        artifact_root = Path("artifacts/hot-deal")
        if not artifact_root.is_dir():
            pytest.skip("artifacts/hot-deal not present; skipping live-data smoke test")
        deals = []
        for vin_dir in artifact_root.iterdir():
            payload_file = vin_dir / "payload-data.json"
            if payload_file.is_file():
                deals.append(json.loads(payload_file.read_text(encoding="utf-8")))
        if not deals:
            pytest.skip("no payload-data.json files present; skipping live-data smoke test")
        payload, skipped = build_hot_deals_batch(deals, batch_id="smoke-test")
        # If 100% of live VINs are skipped, the schema has drifted again
        # and the build is silently producing empty batches. Fail loud.
        assert len(payload["deals"]) > 0, (
            f"build_hot_deals_batch produced 0 deals from {len(deals)} live "
            f"payload files (skipped={len(skipped)}). MMR or pricing schema "
            f"likely drifted again. First few skipped VINs: {skipped[:5]}"
        )


# ---------------------------------------------------------------------------
# build_persisted_payload_data — what the pipeline persists per-VIN
# ---------------------------------------------------------------------------

class TestBuildPersistedPayloadData:
    def test_packs_required_keys(self):
        # No deep-scrape result object — verify graceful handling
        data = build_persisted_payload_data(
            vin="1FT8W2BN0PEC12345",
            deep_scrape_result=None,
            listing_json={"foo": "bar"},
            vin_row={"year": 2025, "make": "Ford"},
        )
        assert data["vin"] == "1FT8W2BN0PEC12345"
        assert data["source_platform"] == "manheim"
        assert "captured_at" in data
        assert data["listing_json"] == {"foo": "bar"}
        assert data["vin_row"] == {"year": 2025, "make": "Ford"}
        assert data["deep_scrape"] == {}

    def test_serializes_deep_scrape_result_dataclass(self):
        # Pretend deep_scrape_result with attrs the function expects.
        class _FakeCR:
            def model_dump(self, mode="json"):
                return {"overall_grade": "4.2", "structural_damage": False}

        class _FakeSnap:
            def model_dump(self, mode="json"):
                return {"page_url": "https://example.com"}

        class _FakeResult:
            images = ["https://img/1.jpg"]
            seller_comments = "All good"
            condition_report = _FakeCR()
            listing_snapshot = _FakeSnap()

        data = build_persisted_payload_data(
            vin="1FT8W2BN0PEC12345",
            deep_scrape_result=_FakeResult(),
            listing_json={},
            vin_row={},
        )
        assert data["deep_scrape"]["images"] == ["https://img/1.jpg"]
        assert data["deep_scrape"]["seller_comments"] == "All good"
        assert data["deep_scrape"]["condition_report"]["overall_grade"] == "4.2"
        assert data["deep_scrape"]["listing_snapshot"]["page_url"] == "https://example.com"

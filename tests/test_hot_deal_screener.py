"""Tests for the Hot Deal screening logic."""
from __future__ import annotations

import pytest

from ove_scraper.hot_deal_screener import (
    screen_autocheck,
    screen_condition_report,
    screen_vin_web_search,
)
from ove_scraper.schemas import ConditionReport


def _make_cr(**overrides) -> ConditionReport:
    """Build a minimal ConditionReport with optional overrides."""
    return ConditionReport(**overrides)


def _make_listing(**overrides) -> dict:
    """Build a minimal OVE listing JSON dict."""
    base = {"asIs": False, "redLight": False, "salvageVehicle": False, "hasFrameDamage": False}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Step 1: Condition Report screening
# ---------------------------------------------------------------------------

class TestScreenConditionReport:
    def test_clean_vehicle_passes(self):
        result = screen_condition_report(_make_cr(), _make_listing())
        assert result.passed is True
        assert result.step == "step1"

    def test_as_is_fails(self):
        result = screen_condition_report(_make_cr(), _make_listing(asIs=True))
        assert result.passed is False
        assert "As-Is" in result.reason

    def test_red_light_fails(self):
        result = screen_condition_report(_make_cr(), _make_listing(redLight=True))
        assert result.passed is False
        assert "Red light" in result.reason

    def test_salvage_vehicle_fails(self):
        result = screen_condition_report(_make_cr(), _make_listing(salvageVehicle=True))
        assert result.passed is False
        assert "Salvage" in result.reason

    def test_frame_damage_fails(self):
        result = screen_condition_report(_make_cr(), _make_listing(hasFrameDamage=True))
        assert result.passed is False
        assert "Frame damage" in result.reason

    def test_branded_title_fails(self):
        cr = _make_cr(title_branding="Salvage - Rebuilt")
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Branded title" in result.reason

    def test_salvage_title_status_fails(self):
        cr = _make_cr(title_status="Salvage")
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Title status" in result.reason

    def test_structural_damage_fails(self):
        cr = _make_cr(structural_damage=True)
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Structural damage" in result.reason

    def test_windshield_crack_fails(self):
        cr = _make_cr(damage_items=[{"section": "Glass", "description": "Windshield cracked"}])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Windshield" in result.reason

    def test_tmu_in_announcements_fails(self):
        cr = _make_cr(announcements=["True Miles Unknown"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "TMU" in result.reason

    def test_engine_issue_in_announcements_fails(self):
        cr = _make_cr(announcements=["Engine noise reported"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Engine/drivetrain" in result.reason

    def test_powertrain_dtc_fails(self):
        cr = _make_cr(diagnostic_codes=["P0301"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "P0301" in result.reason

    def test_clean_title_passes(self):
        cr = _make_cr(title_status="Clean", title_branding="None")
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_yellow_light_still_passes(self):
        result = screen_condition_report(_make_cr(), _make_listing(yellowLight=True))
        assert result.passed is True  # yellow is caution, not disqualifying


# ---------------------------------------------------------------------------
# Step 2: AutoCheck screening
# ---------------------------------------------------------------------------

class TestScreenAutocheck:
    def test_clean_report_passes(self):
        data = {"title_brand_check": "OK", "odometer_check": "OK", "raw_text": "No issues found"}
        result = screen_autocheck(data)
        assert result.passed is True

    def test_title_brand_problem_fails(self):
        data = {"title_brand_check": "Problem Reported", "odometer_check": "OK", "raw_text": ""}
        result = screen_autocheck(data)
        assert result.passed is False
        assert "title brand" in result.reason.lower()

    def test_odometer_problem_fails(self):
        data = {"title_brand_check": "OK", "odometer_check": "Problem Reported", "raw_text": ""}
        result = screen_autocheck(data)
        assert result.passed is False
        assert "Odometer" in result.reason


# ---------------------------------------------------------------------------
# Step 3: Web search screening
# ---------------------------------------------------------------------------

class TestScreenVinWebSearch:
    def test_clean_search_passes(self):
        data = {"found_on_salvage_sites": [], "damage_images_found": False}
        result = screen_vin_web_search(data)
        assert result.passed is True

    def test_copart_hit_fails(self):
        data = {"found_on_salvage_sites": ["copart.com"], "damage_images_found": False}
        result = screen_vin_web_search(data)
        assert result.passed is False
        assert "copart.com" in result.reason

    def test_multiple_sites_fail(self):
        data = {"found_on_salvage_sites": ["iaai.com", "bidfax.info"], "damage_images_found": False}
        result = screen_vin_web_search(data)
        assert result.passed is False
        assert "iaai.com" in result.reason

    def test_damage_images_fail(self):
        data = {"found_on_salvage_sites": [], "damage_images_found": True}
        result = screen_vin_web_search(data)
        assert result.passed is False
        assert "damage" in result.reason.lower()

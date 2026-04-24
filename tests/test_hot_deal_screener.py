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

    def test_manheim_inspection_labels_do_not_false_fail(self):
        # Regression: 2026-04-23 run rejected 50/51 VINs because the
        # Manheim InsightCR template ships with field headings like
        # "ENGINE NOISE No Issues" and "DIAGNOSTIC TROUBLE CODES No
        # Active Codes" in its body. raw_text used to be scanned and
        # the regex matched the label. raw_text is no longer scanned,
        # and even if it were, "No Active Codes" must not match.
        cr = _make_cr(
            raw_text=(
                "ACTIVE VISIBLE LEAKS FROM ENGINE OR UNDERCARRIAGE AREA No Active Leaks "
                "ENGINE NOISE No Issues "
                "ENGINE OIL SLUDGE No Oil Sludge "
                "DIAGNOSTIC TROUBLE CODES No Active Codes "
                "CHECK ENGINE LIGHT No Issues"
            ),
        )
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True, f"label text should not fail; got {result.reason!r}"

    def test_raw_text_not_scanned_for_findings(self):
        # Even a truly scary raw_text string must not reject a vehicle
        # if none of the parsed finding fields flagged it — the
        # normalizer is the single source of truth for promoting
        # findings, raw_text is a fallback for the VPS template only.
        cr = _make_cr(raw_text="Engine noise reported Transmission slip")
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_lemon_law_announcement_still_fails(self):
        # Real announcement text observed on VIN KM8JEDD13SU329976
        # (2025 Hyundai Tucson) 2026-04-23 run. Must be caught via the
        # branded-title-in-announcements scan (buyback / lemon law
        # keywords), NOT via an accidental engine/drivetrain match.
        cr = _make_cr(announcements=[
            "BUYBACK/OPEN SALE, BRANDED TITLE MANUFACTURER'S BUYBACK/LEMON LAW "
            "CHECK ENGINE LIGHT DEALER MUST SIGN & DATE DISCLOSURE"
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Branded title" in result.reason or "buyback" in result.reason.lower() or "lemon" in result.reason.lower()

    def test_active_powertrain_code_in_announcement_fails(self):
        # Real announcement: "Active powertrain codes present" should fail
        cr = _make_cr(announcements=["Active powertrain codes present"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Engine/drivetrain" in result.reason

    def test_no_active_codes_in_announcement_passes(self):
        # If the normalizer were to promote a "No Active Codes" string
        # into announcements (unusual but defensive), it must not fail.
        cr = _make_cr(announcements=["No Active Codes"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    # ------------------------------------------------------------------
    # 2026-04-24 regression: Manheim template labels leaking through
    # mechanical_findings (dict-value stringification) and via
    # announcements were the #1 and #2 false-positive sources (52 of 137
    # rejections on that day's run).
    # ------------------------------------------------------------------

    def test_clean_mechanical_finding_engine_noise_passes(self):
        cr = _make_cr(mechanical_findings=[
            {"section": "mechanical", "section_label": "Mechanical",
             "system": "ENGINE NOISE", "condition": "No Issues"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True, f"clean ENGINE NOISE finding should pass; got {result.reason!r}"

    def test_clean_mechanical_finding_oil_sludge_passes(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "ENGINE OIL SLUDGE", "condition": "No Oil Sludge"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_clean_mechanical_finding_dtc_passes(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "DIAGNOSTIC TROUBLE CODES", "condition": "No Active Codes"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_clean_mechanical_finding_not_specified_passes(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "ACTIVE VISIBLE LEAKS FROM ENGINE OR UNDERCARRIAGE AREA",
             "condition": "Not Specified"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_clean_mechanical_finding_factory_equipment_passes(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "EMISSIONS/CATALYTIC/EXHAUST",
             "condition": "Factory Equipment Installed"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_real_mechanical_finding_engine_knock_fails(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "ENGINE NOISE", "condition": "Engine knock heard on startup"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Mechanical finding" in result.reason
        # Verify the detail was surfaced in the rejection_reason per Layer 3.
        assert "ENGINE NOISE" in result.reason
        assert "knock" in result.reason.lower()

    def test_real_mechanical_finding_needs_transmission_fails(self):
        cr = _make_cr(mechanical_findings=[
            {"system": "OTHER MECHANICAL COMMENTS",
             "condition": "Needs new transmission"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "Mechanical finding" in result.reason

    def test_bare_label_announcement_engine_noise_passes(self):
        # Manheim's listing-JSON disclosure block can inject
        # "ENGINE NOISE No Issues"-style text into cr.announcements.
        # The narrowed regex no longer matches bare label text.
        cr = _make_cr(announcements=["ENGINE NOISE", "TRANSMISSION NOISE"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_bare_label_announcement_mechanical_issue_passes(self):
        # "mechanical issue" / "mechanical problem" used to match as
        # bare template labels. Now requires "failure" or "damage".
        cr = _make_cr(announcements=["MECHANICAL ISSUE", "mechanical problem"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_contextual_engine_noise_reported_still_fails(self):
        # "Engine noise reported" uses a problem-confirming verb;
        # must still fail.
        cr = _make_cr(announcements=["Engine noise reported by seller"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False

    def test_transmission_slipping_announcement_fails(self):
        cr = _make_cr(announcements=["Transmission slipping between gears"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False

    def test_green_yellow_lights_still_pass(self):
        # Synthetic light announcements from listing JSON must never
        # trip the engine/drivetrain filter.
        cr = _make_cr(announcements=["Green Light", "Yellow Light"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is True

    def test_mechanical_concern_reason_includes_detail(self):
        # Layer 3 observability: rejection_reason should show which
        # finding (system + condition) triggered the failure.
        cr = _make_cr(mechanical_findings=[
            {"system": "POWERTRAIN", "condition": "Transmission slip on upshift"},
        ])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        assert "POWERTRAIN" in result.reason
        assert "slip" in result.reason.lower()

    def test_engine_issue_reason_includes_matched_phrase(self):
        cr = _make_cr(announcements=["Engine knock reported"])
        result = screen_condition_report(cr, _make_listing())
        assert result.passed is False
        # The rejection reason should contain the matched phrase for
        # post-mortem introspection.
        assert "Engine/drivetrain issue detected" in result.reason
        assert "knock" in result.reason.lower()


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

"""Pure screening logic for the Hot Deal pipeline.

Each function takes structured data and returns a pass/fail verdict.
No browser, network, or database calls — deterministic and testable.
"""
from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field

from ove_scraper.schemas import ConditionReport


class ScreenResult(BaseModel):
    passed: bool
    step: str
    reason: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Keyword patterns
# ---------------------------------------------------------------------------

_BRANDED_TITLE_PATTERNS = re.compile(
    r"salvage|rebuilt|rebuildable|branded|junk|scrapped|lemon|"
    r"manufacturer\s*buyback|flood|fire\s*brand|hail",
    re.IGNORECASE,
)

_TMU_PATTERNS = re.compile(
    r"\btmu\b|true\s*miles?\s*unknown|not\s*actual\s*mileage|"
    r"exceeds\s*mechanical\s*limits|odometer\s*discrepancy",
    re.IGNORECASE,
)

# Engine/drivetrain concern patterns.
#
# History (2026-04-23): an earlier pairing of this regex with cr.raw_text
# scanning caused the Manheim InsightCR template label "ENGINE NOISE"
# (field value "No Issues") to match on 50/51 rejected VINs — field
# headings, not inspector findings. The architectural fix is in
# _collect_text_fields below: scanning no longer touches cr.raw_text,
# only the parsed findings fields (announcements, remarks,
# seller_comments_items, problem_highlights) and the itemized
# mechanical_findings list. Those are the fields that carry actual
# inspector observations, not template labels.
#
# Patterns kept permissive because the normalizer already filters out
# non-finding boilerplate before populating these fields. Defense in
# depth: "Active Codes" alone is too broad (field label "No Active
# Codes"), so it requires an explicit powertrain/engine/transmission
# qualifier. "Check engine" requires an "on/illuminated/reported/active"
# follow-up.
_ENGINE_DRIVETRAIN_PATTERNS = re.compile(
    r"engine\s*(?:noise|knock|misfire|issue|problem|failure|replace|seized|blown)|"
    r"drivetrain\s*(?:noise|issue|problem|failure)|"
    r"transmission\s*(?:noise|slip|issue|problem|failure|replace)|"
    r"\bactive\s+(?:powertrain|engine|transmission)\s+(?:code|codes|dtc)\b|"
    r"check\s+engine\s+light\s+(?:on|illuminated|reported|active)\b|"
    r"(?:needs|requires)\s*(?:engine|motor|transmission)|"
    r"mechanical\s*(?:issue|problem|failure|damage)",
    re.IGNORECASE,
)

_WINDSHIELD_DAMAGE_PATTERNS = re.compile(
    r"windshield.*(?:crack|broken|shatter|chip|damage)|"
    r"(?:crack|broken|shatter).*windshield",
    re.IGNORECASE,
)

_SALVAGE_DOMAINS = {
    "copart.com", "iaai.com", "autobidmaster.com", "salvagereseller.com",
    "abetter.bid", "salvageautosauction.com", "sca.auction", "salvagebid.com",
    "bid.cars", "bidfax.info", "stat.vin", "carfast.express", "poctra.com",
    "usedbidcars.com", "row52.com", "lkqcanada.ca", "copart.co.uk",
    "auctions.synetiq.co.uk", "pickles.com.au", "manheim.com.au",
}

_AUTOCHECK_BRANDED_PATTERNS = re.compile(
    r"salvage\s*brand|rebuilt\s*(?:or\s*)?rebuildable\s*brand|"
    r"lemon|manufacturer\s*buyback|fire\s*brand|"
    r"hail\s*(?:or\s*)?flood\s*brand|junk\s*(?:or\s*)?scrapped\s*brand|"
    r"odometer\s*brand|problem\s*reported",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Step 1: Condition Report screening
# ---------------------------------------------------------------------------

def screen_condition_report(
    cr: ConditionReport,
    listing_json: dict[str, Any],
) -> ScreenResult:
    """Screen a VIN based on its condition report and OVE listing JSON."""

    # As-Is
    if listing_json.get("asIs") is True:
        return ScreenResult(passed=False, step="step1", reason="As-Is vehicle")

    # Red light
    if listing_json.get("redLight") is True:
        return ScreenResult(passed=False, step="step1", reason="Red light")

    # Salvage from listing JSON
    if listing_json.get("salvageVehicle") is True:
        return ScreenResult(passed=False, step="step1", reason="Salvage vehicle (listing flag)")

    # Frame damage from listing JSON
    if listing_json.get("hasFrameDamage") is True:
        return ScreenResult(passed=False, step="step1", reason="Frame damage (listing flag)")

    # Title branding from CR
    if cr.title_branding and _BRANDED_TITLE_PATTERNS.search(cr.title_branding):
        return ScreenResult(
            passed=False, step="step1",
            reason=f"Branded title: {cr.title_branding}",
        )

    # Title status from CR
    if cr.title_status and _BRANDED_TITLE_PATTERNS.search(cr.title_status):
        return ScreenResult(
            passed=False, step="step1",
            reason=f"Title status: {cr.title_status}",
        )

    # Branded-title language appearing in announcements — covers the case
    # where Manheim's announcement lists a disclosure ("MANUFACTURER'S
    # BUYBACK/LEMON LAW", "BRANDED TITLE SALVAGE", etc.) but the CR's
    # title_branding field isn't populated. Observed 2026-04-23 on VIN
    # KM8JEDD13SU329976 (Hyundai Tucson): the lemon-law announcement
    # was a real red flag that was only caught by a broad
    # engine/drivetrain match because title_branding was empty.
    for announcement in cr.announcements:
        if _BRANDED_TITLE_PATTERNS.search(announcement):
            return ScreenResult(
                passed=False, step="step1",
                reason=f"Branded title announcement: {announcement[:120]}",
            )

    # Structural damage
    if cr.structural_damage is True:
        return ScreenResult(passed=False, step="step1", reason="Structural damage reported")

    # Windshield damage
    for item in cr.damage_items:
        desc = " ".join(str(v) for v in item.values())
        if _WINDSHIELD_DAMAGE_PATTERNS.search(desc):
            return ScreenResult(
                passed=False, step="step1",
                reason="Windshield damage",
                details={"damage_item": item},
            )

    # Scan all text fields for TMU
    all_text = _collect_text_fields(cr)
    if _TMU_PATTERNS.search(all_text):
        return ScreenResult(passed=False, step="step1", reason="True Miles Unknown (TMU)")

    # Engine / drivetrain issues
    if _ENGINE_DRIVETRAIN_PATTERNS.search(all_text):
        return ScreenResult(passed=False, step="step1", reason="Engine/drivetrain issue detected")

    # Powertrain diagnostic codes (P0xxx)
    for code in cr.diagnostic_codes:
        if re.match(r"^P0", code, re.IGNORECASE):
            return ScreenResult(
                passed=False, step="step1",
                reason=f"Powertrain diagnostic code: {code}",
            )

    # Mechanical findings with concerning keywords
    for finding in cr.mechanical_findings:
        desc = " ".join(str(v) for v in finding.values())
        if _ENGINE_DRIVETRAIN_PATTERNS.search(desc):
            return ScreenResult(
                passed=False, step="step1",
                reason="Mechanical finding: engine/drivetrain concern",
                details={"finding": finding},
            )

    return ScreenResult(passed=True, step="step1")


# ---------------------------------------------------------------------------
# Step 2: AutoCheck modal screening
# ---------------------------------------------------------------------------

def screen_autocheck(autocheck_data: dict[str, Any]) -> ScreenResult:
    """Screen based on the full AutoCheck report scraped from the modal."""

    raw_text = autocheck_data.get("raw_text", "")

    # Title brand check
    title_check = autocheck_data.get("title_brand_check", "")
    if "problem reported" in title_check.lower():
        return ScreenResult(
            passed=False, step="step2",
            reason="AutoCheck: Major title brand problem reported",
            details={"title_brand_check": title_check},
        )

    # Scan raw text for branded title keywords
    if _AUTOCHECK_BRANDED_PATTERNS.search(raw_text):
        # Only fail if in the title brand section context
        title_section = _extract_section(raw_text, "Major State Title Brand Check")
        if title_section and "problem reported" in title_section.lower():
            return ScreenResult(
                passed=False, step="step2",
                reason="AutoCheck: Branded title detected in report",
            )

    # Odometer check
    odometer_check = autocheck_data.get("odometer_check", "")
    if "problem reported" in odometer_check.lower():
        return ScreenResult(
            passed=False, step="step2",
            reason="AutoCheck: Odometer problem reported",
            details={"odometer_check": odometer_check},
        )

    return ScreenResult(passed=True, step="step2")


# ---------------------------------------------------------------------------
# Step 3: VIN web search screening
# ---------------------------------------------------------------------------

def screen_vin_web_search(search_result: dict[str, Any]) -> ScreenResult:
    """Screen based on OpenAI web_search results for the VIN."""

    found_sites = search_result.get("found_on_salvage_sites", [])
    if found_sites:
        return ScreenResult(
            passed=False, step="step3",
            reason=f"VIN found on salvage site(s): {', '.join(found_sites)}",
            details={"sites": found_sites},
        )

    if search_result.get("damage_images_found"):
        return ScreenResult(
            passed=False, step="step3",
            reason="Severe damage images found in web search",
        )

    return ScreenResult(passed=True, step="step3")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_text_fields(cr: ConditionReport) -> str:
    """Concatenate the *parsed finding* text fields from a CR for keyword
    scanning.

    Intentionally excludes cr.raw_text because raw_text is the whole CR
    body dumped as one string — it contains every inspection field HEADING
    (e.g. "ENGINE NOISE No Issues", "DIAGNOSTIC TROUBLE CODES No Active
    Codes") which trivially match any engine/drivetrain keyword regex.
    The normalizer is responsible for promoting real inspector findings
    into the structured fields below; if a finding exists it will be in
    announcements, remarks, seller comments, or problem_highlights.
    """
    parts = []
    parts.extend(cr.announcements)
    parts.extend(cr.remarks)
    parts.extend(cr.seller_comments_items)
    parts.extend(cr.problem_highlights)
    return " ".join(parts)


def _extract_section(text: str, heading: str) -> str | None:
    """Extract text from a section heading to the next heading or end."""
    pattern = re.compile(
        rf"{re.escape(heading)}(.*?)(?=\n[A-Z][a-z].*?(?:Check|Brand|Protection)|$)",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(text)
    return match.group(1).strip() if match else None

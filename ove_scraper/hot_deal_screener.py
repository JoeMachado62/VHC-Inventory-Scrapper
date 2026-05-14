"""Pure screening logic for the Hot Deal pipeline.

Each function takes structured data and returns a pass/fail verdict.
No browser, network, or database calls — deterministic and testable.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from pydantic import BaseModel, Field

from ove_scraper.hot_deal_rule_catalog import (
    HOT_DEAL_NARRATIVE_RULES,
    NarrativeRule,
    format_defect_flag_reason,
)
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
# History:
#
# 2026-04-23: raw_text scanning of cr.raw_text was matching the Manheim
# InsightCR template label "ENGINE NOISE" (field value "No Issues") on
# 50/51 rejected VINs. Removed raw_text from _collect_text_fields.
#
# 2026-04-24: same pattern still caused 52/137 false-positive rejections
# through two channels:
#   (a) cr.mechanical_findings contains structured dicts with field
#       labels in the `system` key, e.g.
#       {"system": "ENGINE NOISE", "condition": "No Issues"}. The
#       screener stringified ALL dict values and matched on the
#       system-label text regardless of the clean-state condition.
#   (b) cr.announcements is populated from Manheim's listing-JSON
#       disclosure block which contains field-label-like strings
#       without problem context.
#
# Two fixes applied together:
#   1. _is_clean_state_finding() filters mechanical_findings that
#      report a clean inspection result before regex match.
#   2. Regex narrowed to require problem-indicator VERBS (knock,
#      misfire, slip, failure, replace, seized, blown, stall, repair,
#      damage) or specific disambiguating modifiers ("active
#      powertrain code", "check engine light on", "needs transmission").
#      Bare label variants (engine noise, engine issue, engine problem,
#      transmission noise, mechanical issue, mechanical problem, etc.)
#      are removed because Manheim prints them as field headings on
#      CLEAN reports.
#
# Real findings always use a verb. "Engine knock reported",
# "Transmission slips on upshift", "Needs new transmission" all match.
# "ENGINE NOISE: No Issues" no longer matches.
_ENGINE_DRIVETRAIN_PATTERNS = re.compile(
    r"engine\s+(?:knock|misfire|failure|replace|seized|blown|stall)\b|"
    r"engine\s+noise\s+(?:reported|heard|observed|present|on\s+startup|from|at)\b|"
    r"transmission\s+(?:slip|slipping|failure|replace|repair)\b|"
    r"transmission\s+noise\s+(?:reported|heard|observed|present)\b|"
    r"drivetrain\s+(?:failure|replace|repair)\b|"
    r"\bactive\s+(?:powertrain|engine|transmission)\s+(?:code|codes|dtc)\b|"
    r"check\s+engine\s+light\s+(?:on|illuminated|reported|active)\b|"
    r"(?:needs|requires)\s+(?:(?:a|an|new|replacement|rebuilt|another)\s+)?(?:engine|motor|transmission|drivetrain)\b|"
    r"mechanical\s+(?:failure|damage)\b",
    re.IGNORECASE,
)

# Clean-state condition values emitted by the Manheim inspection parser.
# The parser annotates EVERY inspection checkpoint as a mechanical_finding
# entry — including those that passed. These strings signal "inspected
# and found no problem" and must not trigger a rejection.
_CLEAN_STATE_CONDITIONS = frozenset({
    "no issues",
    "no active codes",
    "no oil sludge",
    "no active leaks",
    "no smoke",
    "factory equipment installed",
    "not specified",
    "ok",
    "none",
    "clean",
    "no problems",
})


def _is_clean_state_finding(finding: dict) -> bool:
    """Return True if this mechanical_finding reports a clean inspection.

    A finding like {system: "ENGINE NOISE", condition: "No Issues"} is
    the Manheim template's way of saying "we inspected for engine
    noise and found none." Treating it as a concern false-fails every
    clean Manheim CR (52 VINs on 2026-04-24). We recognise clean state
    either via the explicit allowlist above or by leading-negation
    prefixes "no " / "none" that universally mean absence of a problem.

    Empty condition is treated as clean because the screener should
    only act on positively-confirmed findings, not missing data.
    """
    condition = str(finding.get("condition") or "").strip().lower()
    if not condition:
        return True
    if condition in _CLEAN_STATE_CONDITIONS:
        return True
    if condition.startswith("no ") or condition.startswith("none"):
        return True
    return False

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

_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")
_WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class _NarrativeEvidence:
    source_label: str
    raw_text: str
    normalized_text: str


@dataclass(frozen=True)
class _NarrativeSignalMatch:
    signal_key: str
    source_label: str
    raw_text: str
    matched_text: str
    score: int


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

    history = cr.vehicle_history or {}
    if history.get("engine_starts") is False:
        return ScreenResult(
            passed=False,
            step="step1",
            reason="Engine does not start",
            details={"field": "vehicle_history.engine_starts"},
        )
    if history.get("drivable") is False:
        return ScreenResult(
            passed=False,
            step="step1",
            reason="Vehicle does not drive",
            details={"field": "vehicle_history.drivable"},
        )

    defect_flags = _get_defect_flags(cr)
    if defect_flags:
        first = defect_flags[0]
        return ScreenResult(
            passed=False,
            step="step1",
            reason=format_defect_flag_reason(first),
            details={"defect_flag": first, "defect_flags": defect_flags},
        )

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
    title_risk_signal = _find_narrative_rule_match(cr, "title_risk", listing_json)
    if title_risk_signal:
        strongest = max(title_risk_signal["signals"], key=lambda item: item["score"])
        return ScreenResult(
            passed=False, step="step1",
            reason=f"Title risk in {strongest['source_label']}: {strongest['raw_text'][:120]}",
            details=title_risk_signal,
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
    all_text = _collect_text_fields(cr, listing_json)
    if _TMU_PATTERNS.search(all_text):
        return ScreenResult(passed=False, step="step1", reason="True Miles Unknown (TMU)")

    # Engine / drivetrain issues — surface the matching phrase in the
    # rejection reason so DB introspection is self-describing (2026-04-24).
    et_match = _ENGINE_DRIVETRAIN_PATTERNS.search(all_text)
    if et_match:
        return ScreenResult(
            passed=False, step="step1",
            reason=f"Engine/drivetrain issue detected: {et_match.group(0)!r}",
        )

    # Powertrain diagnostic codes (P0xxx)
    for code in cr.diagnostic_codes:
        if re.match(r"^P0", code, re.IGNORECASE):
            return ScreenResult(
                passed=False, step="step1",
                reason=f"Powertrain diagnostic code: {code}",
            )

    # Mechanical findings with concerning keywords. Skip clean-state
    # inspection points first — Manheim's template enumerates every
    # checkpoint including those that passed ("ENGINE NOISE / No Issues",
    # "DIAGNOSTIC TROUBLE CODES / No Active Codes", etc.) and those must
    # never be treated as concerns.
    for finding in cr.mechanical_findings:
        if _is_clean_state_finding(finding):
            continue
        desc = " ".join(str(v) for v in finding.values())
        if _ENGINE_DRIVETRAIN_PATTERNS.search(desc):
            system = finding.get("system") or "mechanical"
            condition = finding.get("condition") or ""
            return ScreenResult(
                passed=False, step="step1",
                reason=f"Mechanical finding: engine/drivetrain concern ({system}: {condition})",
                details={"finding": finding},
            )

    return ScreenResult(passed=True, step="step1")


# ---------------------------------------------------------------------------
# Step 2: AutoCheck modal screening
# ---------------------------------------------------------------------------

def screen_autocheck(autocheck_data: dict[str, Any]) -> ScreenResult:
    """Screen based on the full AutoCheck report scraped from the modal."""

    status = str(autocheck_data.get("scrape_status") or "not_attempted")
    if status != "success":
        return ScreenResult(
            passed=False,
            step="step2",
            reason=f"AutoCheck: full report not captured ({status})",
            details={
                "scrape_status": status,
                "failure_category": autocheck_data.get("failure_category"),
                "failure_message": autocheck_data.get("failure_message"),
            },
        )

    raw_text = " ".join(
        str(autocheck_data.get(key) or "")
        for key in ("raw_text", "full_report_text")
    )

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


def is_autocheck_capture_failure(result: ScreenResult) -> bool:
    """True when Step 2 failed because scraping was incomplete."""
    if result.passed or result.step != "step2":
        return False
    details = result.details or {}
    status = str(details.get("scrape_status") or "")
    return bool(status) and status != "success"


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

def _collect_text_fields(cr: ConditionReport, listing_json: dict[str, Any] | None = None) -> str:
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
    for item in _iter_listing_narrative_values(listing_json):
        parts.append(item)
    return " ".join(parts)


def _get_defect_flags(cr: ConditionReport) -> list[dict[str, Any]]:
    metadata = cr.metadata or {}
    flags = metadata.get("defect_flags")
    if not isinstance(flags, list):
        return []
    return [flag for flag in flags if isinstance(flag, dict)]


def _normalize_narrative_text(text: str) -> str:
    lowered = str(text or "").lower().replace("&", " and ")
    cleaned = _NON_ALNUM_PATTERN.sub(" ", lowered)
    return _WHITESPACE_PATTERN.sub(" ", cleaned).strip()


def _iter_narrative_evidence(
    cr: ConditionReport,
    listing_json: dict[str, Any] | None = None,
) -> list[_NarrativeEvidence]:
    evidence: list[_NarrativeEvidence] = []
    for source_label, values in (
        ("announcement", cr.announcements),
        ("remark", cr.remarks),
        ("seller comment", cr.seller_comments_items),
        ("problem highlight", cr.problem_highlights),
    ):
        for value in values:
            raw_text = str(value or "").strip()
            if not raw_text:
                continue
            evidence.append(
                _NarrativeEvidence(
                    source_label=source_label,
                    raw_text=raw_text,
                    normalized_text=_normalize_narrative_text(raw_text),
                )
            )
    for source_label, raw_text in _iter_listing_narrative_evidence(listing_json):
        evidence.append(
            _NarrativeEvidence(
                source_label=source_label,
                raw_text=raw_text,
                normalized_text=_normalize_narrative_text(raw_text),
            )
        )
    return evidence


def _find_narrative_rule_match(
    cr: ConditionReport,
    rule_key: str,
    listing_json: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    evidence = _iter_narrative_evidence(cr, listing_json)
    if not evidence:
        return None

    for rule in HOT_DEAL_NARRATIVE_RULES:
        if rule.key != rule_key:
            continue
        return _evaluate_narrative_rule(rule, evidence)
    return None


def _iter_listing_narrative_values(listing_json: dict[str, Any] | None) -> list[str]:
    return [text for _, text in _iter_listing_narrative_evidence(listing_json)]


def _iter_listing_narrative_evidence(listing_json: dict[str, Any] | None) -> list[tuple[str, str]]:
    if not listing_json:
        return []

    evidence: list[tuple[str, str]] = []
    enrichment = listing_json.get("announcementsEnrichment") or {}

    def append_values(source_label: str, values: Any) -> None:
        if isinstance(values, str):
            text = values.strip()
            if text:
                evidence.append((source_label, text))
            return
        if isinstance(values, list):
            for item in values:
                text = str(item or "").strip()
                if text:
                    evidence.append((source_label, text))

    append_values("listing announcement", enrichment.get("announcements"))
    append_values("listing remark", enrichment.get("remarks"))
    append_values("listing comment", listing_json.get("comments"))
    append_values("listing additional announcement", listing_json.get("additionalAnnouncements"))
    append_values("listing remark", listing_json.get("remarks"))
    return evidence


def _evaluate_narrative_rule(
    rule: NarrativeRule,
    evidence: list[_NarrativeEvidence],
) -> dict[str, Any] | None:
    matches: list[_NarrativeSignalMatch] = []
    total_score = 0

    for signal in rule.signals:
        best_match: _NarrativeSignalMatch | None = None
        for item in evidence:
            matched = signal.pattern.search(item.normalized_text)
            if not matched:
                continue
            score = signal.weight + signal.source_bonus.get(item.source_label, 0)
            signal_match = _NarrativeSignalMatch(
                signal_key=signal.key,
                source_label=item.source_label,
                raw_text=item.raw_text,
                matched_text=matched.group(0),
                score=score,
            )
            if best_match is None or signal_match.score > best_match.score:
                best_match = signal_match
        if best_match is not None:
            matches.append(best_match)
            total_score += best_match.score

    if total_score < rule.threshold:
        return None

    return {
        "rule_key": rule.key,
        "category": rule.category,
        "score": total_score,
        "threshold": rule.threshold,
        "signals": [
            {
                "signal_key": match.signal_key,
                "source_label": match.source_label,
                "raw_text": match.raw_text,
                "matched_text": match.matched_text,
                "score": match.score,
            }
            for match in matches
        ],
    }


def _extract_section(text: str, heading: str) -> str | None:
    """Extract text from a section heading to the next heading or end."""
    pattern = re.compile(
        rf"{re.escape(heading)}(.*?)(?=\n[A-Z][a-z].*?(?:Check|Brand|Protection)|$)",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(text)
    return match.group(1).strip() if match else None

"""Declarative Hot Deal defect rule catalog.

The screener keeps control-flow logic in ``hot_deal_screener.py`` but the
reasons, phrases, and source weighting live here so we can expand failure
causes without scattering regexes through the pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


def _rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


@dataclass(frozen=True)
class NarrativeSignal:
    key: str
    pattern: re.Pattern[str]
    weight: int
    source_bonus: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class NarrativeRule:
    key: str
    category: str
    failure_label: str
    threshold: int
    signals: tuple[NarrativeSignal, ...]


@dataclass(frozen=True)
class DefectRule:
    key: str
    category: str
    severity: str
    failure_label: str
    patterns: tuple[re.Pattern[str], ...]
    exclude_patterns: tuple[re.Pattern[str], ...] = ()


_TITLE_SOURCE_BONUS = {
    "announcement": 10,
    "remark": 20,
    "seller comment": 20,
    "problem highlight": 5,
}


TITLE_RISK_RULE = NarrativeRule(
    key="title_risk",
    category="title",
    failure_label="Title risk",
    threshold=80,
    signals=(
        NarrativeSignal(
            key="salvage_or_branded_title",
            pattern=_rx(
                r"\bsalvage\b|\brebuilt\b|\brebuildable\b|"
                r"\bbranded\s+title\b|\bjunk\b|\bscrapped\b"
            ),
            weight=100,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="buyback_or_lemon",
            pattern=_rx(
                r"\blemon\b|\bbuyback\b|"
                r"\bmanufacturer(?:\s+s)?\s+buyback\b"
            ),
            weight=100,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="flood_fire_hail_brand",
            pattern=_rx(
                r"\bflood\b|\bfire\s+brand\b|\bhail\b"
            ),
            weight=100,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="total_loss_history",
            pattern=_rx(r"\btotal\s+loss\b"),
            weight=60,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="registered_to_insurance_company",
            pattern=_rx(r"\bregistered\s+to\s+(?:an?\s+)?insurance\s+company\b"),
            weight=55,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="title_with_insurance_company",
            pattern=_rx(
                r"\btitle\b.{0,40}\binsurance\s+company\b|"
                r"\binsurance\s+company\b.{0,40}\btitle\b"
            ),
            weight=100,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="insurance_company_reference",
            pattern=_rx(r"\binsurance\s+company\b|\binsurance\s+loss\b|\binsurer\b"),
            weight=25,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
        NarrativeSignal(
            key="owner_retained_or_destruction",
            pattern=_rx(
                r"\bowner\s+retained\b|"
                r"\bcertificate\s+of\s+destruction\b|"
                r"\bnon\s+repairable\b|\bnonrepairable\b|"
                r"\bparts\s+only\b|\bscrap\s+only\b"
            ),
            weight=100,
            source_bonus=_TITLE_SOURCE_BONUS,
        ),
    ),
)


HOT_DEAL_NARRATIVE_RULES: tuple[NarrativeRule, ...] = (
    TITLE_RISK_RULE,
)


_CLEAN_STATE_PATTERNS = tuple(
    _rx(pattern)
    for pattern in (
        r"\bno\s+(?:issues?|problems?|damage|active\s+codes?|active\s+leaks?|smoke)\b",
        r"\bnone\b",
        r"\bok\b",
        r"\bfully\s+functional\b",
        r"\bfactory\s+equipment\s+installed\b",
        r"\bnot\s+specified\b",
    )
)

_COSMETIC_ONLY_PATTERN = _rx(
    r"\b(?:scratch(?:es)?|dent(?:s)?|ding(?:s)?|chip(?:ped|s)?|scuff(?:s)?|"
    r"prior\s+paint|previous\s+repair|paint\s+damage|pdr|misaligned|loose|"
    r"missing|worn|after\s+market|non\s*-\s*oem|de-?identify|value\s+added|"
    r"curb\s+rash|wheel\s+cover|burn\s+hole|prev\s+repair)\b"
)

_STRUCTURAL_FRAME = DefectRule(
    key="structural_frame",
    category="structure",
    severity="critical",
    failure_label="Structural/frame concern",
    patterns=(
        _rx(r"\bstructural\s+(?:damage|issue|alteration|repair|concern)\b"),
        _rx(r"\bframe\s+(?:damage|repair|alteration|issue|concern|bent|rust)\b"),
        _rx(r"\bunibody\s+(?:damage|repair|issue)\b"),
        _rx(r"\b(?:apron|rail|strut\s+tower|core\s+support)\s+(?:damage|repair|bent)\b"),
    ),
)

_TITLE_HISTORY = DefectRule(
    key="title_history",
    category="title",
    severity="critical",
    failure_label="Title/history concern",
    patterns=(
        _rx(r"\bsalvage\b|\brebuilt\b|\brebuildable\b|\bbranded\s+title\b"),
        _rx(r"\btotal\s+loss\b|\binsurance\s+loss\b|\bregistered\s+to\s+(?:an?\s+)?insurance\s+company\b"),
        _rx(r"\blemon\b|\bbuyback\b|\bmanufacturer(?:\s+s)?\s+buyback\b"),
        _rx(r"\bflood\b|\bwater\s+damage\b|\bfire\s+(?:brand|damage)\b|\bhail\s+brand\b"),
        _rx(r"\bowner\s+retained\b|\bcertificate\s+of\s+destruction\b|\bparts\s+only\b|\bscrap\s+only\b"),
    ),
)

_ODOMETER = DefectRule(
    key="odometer",
    category="odometer",
    severity="critical",
    failure_label="Odometer concern",
    patterns=(
        _rx(r"\btmu\b|\btrue\s+miles?\s+unknown\b"),
        _rx(r"\bnot\s+actual\s+mileage\b|\bodometer\s+(?:rollback|discrepanc|brand|issue|problem)\b"),
        _rx(r"\bexceeds\s+mechanical\s+limits\b"),
    ),
)

_NON_RUNNING = DefectRule(
    key="non_running",
    category="drivability",
    severity="critical",
    failure_label="Non-running/non-driving concern",
    patterns=(
        _rx(r"\bno\s+start\b|\bcrank(?:s|ing)?\s+no\s+start\b|\bstarts?\s+then\s+stall"),
        _rx(r"\b(?:does\s+not|doesn't|wont|won't)\s+(?:start|run|drive)\b"),
        _rx(r"\bnot\s+(?:running|drivable|driveable)\b|\btow\s+only\b"),
        _rx(r"\binop(?:erative)?\b.{0,80}\b(?:battery|engine|transmission|drivetrain|drive)\b"),
        _rx(r"\b(?:battery|engine|transmission|drivetrain|drive)\b.{0,80}\binop(?:erative)?\b"),
    ),
)

_POWERTRAIN = DefectRule(
    key="powertrain",
    category="mechanical",
    severity="critical",
    failure_label="Powertrain concern",
    patterns=(
        _rx(r"\bengine\s+(?:knock|noise|misfire|failure|replace|seized|blown|stall|overheat|smoke|leak)\b"),
        _rx(r"\b(?:needs|requires)\s+(?:(?:a|an|new|replacement|rebuilt|another)\s+)?(?:engine|motor|transmission|drivetrain|transfer\s+case)\b"),
        _rx(r"\btrans(?:mission)?\s+(?:noise|slip|slipping|shudder|failure|replace|repair|hard\s+shift|delayed\s+engagement)\b"),
        _rx(r"\bdrivetrain\s+(?:noise|vibration|failure|replace|repair|issue)\b"),
        _rx(r"\btransfer\s+case\s+(?:noise|failure|issue|repair)\b"),
        _rx(r"\bmechanical\s+(?:failure|damage|issue|problem)\b"),
        _rx(r"\bactive\s+(?:powertrain|engine|transmission)\s+(?:code|codes|dtc)\b"),
    ),
)

_WARNING_LIGHTS = DefectRule(
    key="warning_lights",
    category="warning_lights",
    severity="major",
    failure_label="Warning light concern",
    patterns=(
        _rx(r"\bcheck\s+engine\s+light\s+(?:on|illuminated|reported|active)\b"),
        _rx(r"\b(?:cel|mil)\s+(?:on|illuminated|active)\b"),
        _rx(r"\bwarning\s*[- ]\s*(?:engine|electrical|powertrain|airbag|abs|brake|hybrid|ev)\b"),
        _rx(r"\b(?:abs|srs|airbag|brake|stability|traction)\s+(?:light|warning|fault|issue|inop)\b"),
    ),
)

_AIRBAG_SAFETY = DefectRule(
    key="airbag_safety",
    category="safety",
    severity="critical",
    failure_label="Airbag/safety concern",
    patterns=(
        _rx(r"\bair\s*bag\b.{0,40}\b(?:deployed|missing|fault|warning|inop|issue|light)\b"),
        _rx(r"\bairbag\b.{0,40}\b(?:deployed|missing|fault|warning|inop|issue|light)\b"),
        _rx(r"\bsrs\b.{0,40}\b(?:fault|warning|light|inop|issue)\b"),
        _rx(r"\brestraint\s+(?:fault|warning|inop|issue)\b"),
    ),
)

_STRUCTURED_SEVERITY = DefectRule(
    key="structured_severity",
    category="structured_damage",
    severity="major",
    failure_label="Structured condition defect",
    patterns=(
        _rx(r"\b(?:repair\s+required|replacement\s+required|unacceptable|suspect\s+repair|inop(?:erative)?)\b"),
        _rx(r"\bmechanical\b|\bwarning\s*[- ]\s*(?:engine|electrical|powertrain)\b"),
    ),
)

DEFECT_RULES: tuple[DefectRule, ...] = (
    _STRUCTURAL_FRAME,
    _TITLE_HISTORY,
    _ODOMETER,
    _NON_RUNNING,
    _POWERTRAIN,
    _WARNING_LIGHTS,
    _AIRBAG_SAFETY,
    _STRUCTURED_SEVERITY,
)


def normalize_defect_text(text: str) -> str:
    lowered = str(text or "").lower().replace("&", " and ")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", lowered)).strip()


def looks_clean_or_cosmetic_only(text: str, *, source_label: str = "") -> bool:
    normalized = normalize_defect_text(text)
    if not normalized:
        return True
    if normalized in {
        "mechanical and diagnostic trouble codes",
        "mechanical diagnostic trouble codes",
        "condition details",
        "announcements and comments",
    }:
        return True
    if source_label in {"announcement", "listing announcement"} and normalized in {
        "engine noise",
        "transmission noise",
        "mechanical issue",
        "mechanical problem",
        "other mechanical comments",
    }:
        return True
    if "mechanical guarantee" in normalized:
        return True
    serious_tokens = (
        "structural",
        "frame",
        "engine",
        "transmission",
        "drivetrain",
        "airbag",
        "srs",
        "abs",
        "no start",
        "does not start",
        "does not drive",
        "inop",
        "unacceptable",
        "warning",
        "salvage",
        "total loss",
        "flood",
    )
    if (
        ("no action required" in normalized or "acceptable" in normalized or "open recall" in normalized)
        and not any(token in normalized for token in serious_tokens)
    ):
        return True
    if any(pattern.search(normalized) for pattern in _CLEAN_STATE_PATTERNS):
        return True
    # Cosmetic-only rows should not fail. The structured row category/text
    # will include serious words when the issue is mechanical or safety.
    if _COSMETIC_ONLY_PATTERN.search(normalized) and not any(
        token in normalized
        for token in (
            "structural",
            "frame",
            "engine",
            "transmission",
            "drivetrain",
            "warning",
            "airbag",
            "srs",
            "abs",
            "no start",
            "inop",
            "unacceptable",
        )
    ):
        return True
    return False


def evaluate_defect_rules(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return source-labeled defect flags for normalized evidence records."""
    flags: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for item in evidence:
        raw_text = str(item.get("text") or "").strip()
        source_label = str(item.get("source_label") or "unknown")
        if looks_clean_or_cosmetic_only(raw_text, source_label=source_label):
            continue
        normalized = normalize_defect_text(raw_text)
        for rule in DEFECT_RULES:
            if any(pattern.search(normalized) for pattern in rule.exclude_patterns):
                continue
            matched_text = None
            for pattern in rule.patterns:
                match = pattern.search(normalized)
                if match:
                    matched_text = match.group(0)
                    break
            if not matched_text:
                continue
            key = (rule.key, source_label, normalized[:220])
            if key in seen:
                continue
            seen.add(key)
            flags.append(
                {
                    "rule_key": rule.key,
                    "category": rule.category,
                    "severity": rule.severity,
                    "source_label": source_label,
                    "matched_text": matched_text,
                    "raw_text": raw_text,
                    "failure_label": rule.failure_label,
                    "metadata": dict(item.get("metadata") or {}),
                }
            )
            # The most specific first match is enough per evidence row.
            break
    return flags


def format_defect_flag_reason(flag: dict[str, Any]) -> str:
    label = flag.get("failure_label") or "Defect concern"
    source = flag.get("source_label") or "unknown"
    raw_text = str(flag.get("raw_text") or "").strip()
    return f"{label} in {source}: {raw_text[:160]}"

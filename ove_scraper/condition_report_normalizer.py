from __future__ import annotations

import re
from typing import Any

from ove_scraper.cr_parsers import identify_report_family, parse_condition_report_text
from ove_scraper.hot_deal_rule_catalog import evaluate_defect_rules
from ove_scraper.schemas import ConditionReport


def normalize_condition_report(
    report: ConditionReport | None,
    *,
    raw_text: str | None,
    report_link: dict[str, object] | None,
    listing_json: dict[str, Any] | None = None,
) -> ConditionReport | None:
    if report is None and not raw_text and not report_link and not listing_json:
        return None

    report = report or ConditionReport()
    if listing_json is not None:
        _apply_listing_json(report, listing_json)
    text = raw_text or report.raw_text or ""
    structured = parse_condition_report_text(str((report_link or {}).get("href") or ""), text)
    family = identify_report_family(str((report_link or {}).get("href") or ""))
    has_structured_report = bool(structured) and family is not None

    if text and (not report.raw_text or len(text) > len(report.raw_text)):
        report.raw_text = text

    if structured.get("overall_grade") and (has_structured_report or not report.overall_grade):
        report.overall_grade = str(structured["overall_grade"])
    if structured.get("interior_condition") and (has_structured_report or not report.interior_condition):
        report.interior_condition = str(structured["interior_condition"])
    if has_structured_report and "announcements" in structured:
        report.announcements = [str(item) for item in structured.get("announcements") or [] if str(item).strip()]
    elif structured.get("announcements") and not report.announcements:
        report.announcements = [str(item) for item in structured["announcements"] if str(item).strip()]
    if "drivable" in structured:
        report.vehicle_history = {**report.vehicle_history, "drivable": structured["drivable"]}
    if "engine_starts" in structured:
        report.vehicle_history = {**report.vehicle_history, "engine_starts": structured["engine_starts"]}
    if has_structured_report and "remarks" in structured:
        report.remarks = [str(item) for item in structured.get("remarks") or [] if str(item).strip()]
    elif structured.get("remarks") and not report.remarks:
        report.remarks = [str(item) for item in structured["remarks"] if str(item).strip()]
    if has_structured_report and "seller_comments" in structured:
        report.seller_comments_items = [str(item) for item in structured.get("seller_comments") or [] if str(item).strip()]
    elif structured.get("seller_comments") and not report.seller_comments_items:
        report.seller_comments_items = [str(item) for item in structured["seller_comments"] if str(item).strip()]
    if structured.get("title_status") and (has_structured_report or not report.title_status):
        report.title_status = str(structured["title_status"])
    if structured.get("title_state") and (has_structured_report or not report.title_state):
        report.title_state = str(structured["title_state"])
    if structured.get("title_branding") and (has_structured_report or not report.title_branding):
        report.title_branding = str(structured["title_branding"])
    if "structural_damage" in structured and report.structural_damage is None:
        report.structural_damage = bool(structured["structural_damage"])
    if structured.get("paint_condition") and (has_structured_report or not report.paint_condition):
        report.paint_condition = str(structured["paint_condition"])
    if structured.get("tire_depths"):
        report.tire_depths = (
            dict(structured["tire_depths"])
            if has_structured_report
            else {**structured["tire_depths"], **report.tire_depths}
        )
    if structured.get("damage_items") and (has_structured_report or not report.damage_items):
        report.damage_items = list(structured["damage_items"])
    if has_structured_report and "mechanical_findings" in structured:
        report.mechanical_findings = list(structured.get("mechanical_findings") or [])
    elif structured.get("mechanical_findings"):
        report.mechanical_findings = list(structured["mechanical_findings"])
    if has_structured_report and "diagnostic_codes" in structured:
        report.diagnostic_codes = list(structured.get("diagnostic_codes") or [])
    elif structured.get("diagnostic_codes"):
        report.diagnostic_codes = list(structured["diagnostic_codes"])
    if structured.get("damage_summary"):
        report.damage_summary = (
            dict(structured["damage_summary"])
            if has_structured_report
            else {**structured["damage_summary"], **report.damage_summary}
        )
    if has_structured_report and "problem_highlights" in structured:
        report.problem_highlights = [str(item) for item in structured.get("problem_highlights") or [] if str(item).strip()]
    elif structured.get("problem_highlights"):
        report.problem_highlights = [str(item) for item in structured["problem_highlights"] if str(item).strip()]
    if structured.get("severity_summary") and (has_structured_report or not report.severity_summary):
        report.severity_summary = str(structured["severity_summary"])
    if structured.get("ai_summary") and (has_structured_report or not report.ai_summary):
        report.ai_summary = str(structured["ai_summary"])
    # equipment_features is populated by the Liquid Motors parser from the
    # VEHICLE INFORMATION free-text feature list. Manheim parsers don't
    # populate this field — those vehicles use installed_equipment /
    # high_value_options from the OVE listing JSON instead. Copy through
    # to the schema field if present.
    if structured.get("equipment_features") and not report.equipment_features:
        report.equipment_features = [
            str(item).strip()
            for item in structured["equipment_features"]
            if isinstance(item, str) and str(item).strip()
        ]

    announcements = extract_announcements(text)
    remarks = None if has_structured_report else extract_single_value(text, "remarks")
    seller_comments = None if has_structured_report else extract_single_value(text, "seller comments")
    title_status = None if has_structured_report and structured.get("title_status") else extract_single_value(text, "title status")
    title_state = None if has_structured_report and structured.get("title_state") else extract_single_value(text, "title state")
    title_branding = None if has_structured_report and structured.get("title_branding") else extract_single_value(text, "title branding")
    owners = extract_count(text, "owners")
    accidents = extract_count(text, "accidents")

    if announcements and not report.announcements:
        report.announcements = announcements
    if remarks and remarks.lower() not in {"no remarks", "none"}:
        report.remarks = [remarks]
    if seller_comments and seller_comments.lower() not in {"no comments", "none"} and not report.seller_comments_items:
        report.seller_comments_items = [seller_comments]
    if title_status and not report.title_status:
        report.title_status = title_status
    if title_state and not report.title_state:
        report.title_state = title_state
    if title_branding and not report.title_branding:
        report.title_branding = title_branding
    if owners is not None or accidents is not None:
        report.vehicle_history = {
            **report.vehicle_history,
            **({"owners": owners} if owners is not None else {}),
            **({"accidents": accidents} if accidents is not None else {}),
        }

    if not report.problem_highlights:
        report.problem_highlights = build_problem_highlights(report)
    if report.problem_highlights and not report.severity_summary:
        report.severity_summary = "attention"

    metadata = dict(report.metadata)
    if report_link and "report_link" not in metadata:
        metadata["report_link"] = report_link
    if structured:
        metadata["structured_parse"] = {
            key: value
            for key, value in structured.items()
            if key
            not in {
                "damage_items",
                "damage_summary",
                "problem_highlights",
                "tire_depths",
            }
        }
    family = identify_report_family(str((report_link or {}).get("href") or ""))
    if family is not None:
        metadata["report_family"] = family.family
        metadata["report_family_notes"] = family.notes
        metadata["supports_structured_damage"] = family.supports_structured_damage
        metadata["supports_tire_depths"] = family.supports_tire_depths
        metadata["supports_vehicle_history"] = family.supports_vehicle_history
    report.metadata = metadata

    # Merge listing-JSON announcements (real + synthetic) into the final
    # report.announcements. _apply_listing_json stashed them in private
    # metadata fields earlier; the regex CR parser may have overwritten
    # report.announcements with its own results in between. We want the
    # final list to contain BOTH the parser-extracted disclosures AND the
    # listing-JSON-derived light/arbitration signals, deduped.
    listing_real = report.metadata.pop("_listing_announcements", None) or []
    listing_synthetic = report.metadata.pop("_listing_synthetic_announcements", None) or []
    listing_remarks = report.metadata.pop("_listing_remarks", None) or []
    listing_seller_comments = report.metadata.pop("_listing_seller_comments", None) or []
    listing_damage_items = report.metadata.pop("_listing_condition_damage_items", None) or []
    listing_mechanical_findings = report.metadata.pop("_listing_condition_mechanical_findings", None) or []

    if listing_remarks:
        report.remarks = _merge_unique_text(report.remarks, listing_remarks)
    if listing_seller_comments:
        report.seller_comments_items = _merge_unique_text(report.seller_comments_items, listing_seller_comments)
    if listing_damage_items:
        report.damage_items = _merge_unique_dicts(
            report.damage_items,
            listing_damage_items,
            key_fields=("source", "panel", "condition", "reported_severity", "notes"),
        )
        report.damage_summary = _summarize_report_damage_items(report.damage_items)
    if listing_mechanical_findings:
        report.mechanical_findings = _merge_unique_dicts(
            report.mechanical_findings,
            listing_mechanical_findings,
            key_fields=("source", "system", "condition"),
        )

    merged_announcements: list[str] = list(report.announcements or [])
    seen_lower = {a.lower() for a in merged_announcements}
    for item in list(listing_real) + list(listing_synthetic):
        if not item:
            continue
        if item.lower() in seen_lower:
            continue
        merged_announcements.append(item)
        seen_lower.add(item.lower())
    report.announcements = merged_announcements

    # ALWAYS mirror the final announcements list into
    # metadata.announcementsEnrichment.announcements so the VPS contract's
    # second alternative path is satisfied. Verified 2026-04-09 via 422
    # response on VIN 1N4BL4EV2NN423240: the VPS validator requires either
    # condition_report.announcements OR
    # condition_report.metadata.announcementsEnrichment.announcements
    # to be present. We populate both to be safe.
    metadata_final = dict(report.metadata)
    metadata_enrichment = dict(metadata_final.get("announcementsEnrichment") or {})
    metadata_enrichment["announcements"] = list(report.announcements)
    metadata_final["announcementsEnrichment"] = metadata_enrichment
    metadata_final["defect_flags"] = build_defect_flags(report, listing_json)
    report.metadata = metadata_final

    # Add lf/rf/lr/rr aliases to tire_depths so the VPS contract is
    # satisfied regardless of which parser family populated the dict.
    # _parse_manheim_inspectionreport produces driver_front/driver_rear/
    # passenger_front/passenger_rear; _parse_manheim_insightcr produces
    # left_front/left_rear/right_front/right_rear. The VPS expects
    # lf/rf/lr/rr. Aliases share the same dict reference so all three
    # naming schemes coexist; the VPS template can read whichever it
    # prefers.
    _add_tire_depth_aliases(report)

    return report


def build_defect_flags(report: ConditionReport, listing_json: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Build structured defect flags for shared CR/report consumers.

    These flags are metadata only for individual ordered condition reports.
    The Hot Deal screener is the policy layer that decides whether a flag
    excludes a VIN from the marketing batch.
    """
    evidence: list[dict[str, Any]] = []

    def add_text(source_label: str, value: Any, metadata: dict[str, Any] | None = None) -> None:
        if isinstance(value, str):
            text = value.strip()
            if text:
                evidence.append({"source_label": source_label, "text": text, "metadata": metadata or {}})
        elif isinstance(value, list):
            for item in value:
                add_text(source_label, item, metadata)

    add_text("announcement", report.announcements)
    add_text("remark", report.remarks)
    add_text("seller comment", report.seller_comments_items)
    add_text("problem highlight", report.problem_highlights)
    if report.title_status:
        add_text("title status", report.title_status)
    if report.title_branding:
        add_text("title branding", report.title_branding)

    history = report.vehicle_history or {}
    if history.get("engine_starts") is False:
        add_text("vehicle history", "Engine does not start", {"field": "engine_starts"})
    if history.get("drivable") is False:
        add_text("vehicle history", "Vehicle does not drive", {"field": "drivable"})

    for item in report.damage_items:
        text = _damage_item_text(item)
        if text:
            evidence.append({
                "source_label": str(item.get("source") or "condition report damage"),
                "text": text,
                "metadata": {k: v for k, v in item.items() if k != "raw"},
            })
    for finding in report.mechanical_findings:
        text = _mechanical_finding_text(finding)
        if text:
            evidence.append({
                "source_label": str(finding.get("source") or "condition report mechanical"),
                "text": text,
                "metadata": dict(finding),
            })
    for code in report.diagnostic_codes:
        if re.match(r"^P0[A-F0-9]{3}$", str(code or ""), flags=re.IGNORECASE):
            add_text("diagnostic code", f"Active powertrain code {code}", {"code": code})

    if listing_json:
        enrichment = listing_json.get("announcementsEnrichment") or {}
        add_text("listing announcement", enrichment.get("announcements"))
        add_text("listing remark", enrichment.get("remarks"))
        add_text("listing comment", listing_json.get("comments"))
        add_text("listing additional announcement", listing_json.get("additionalAnnouncements"))
        add_text("listing remark", listing_json.get("remarks"))

    return evaluate_defect_rules(evidence)


_TIRE_DEPTH_ALIAS_MAP = {
    "lf": ("left_front", "driver_front"),
    "rf": ("right_front", "passenger_front"),
    "lr": ("left_rear", "driver_rear"),
    "rr": ("right_rear", "passenger_rear"),
}


def _add_tire_depth_aliases(report: ConditionReport) -> None:
    if not report.tire_depths:
        return
    for short, sources in _TIRE_DEPTH_ALIAS_MAP.items():
        if short in report.tire_depths:
            continue
        for src in sources:
            if src in report.tire_depths:
                report.tire_depths[short] = report.tire_depths[src]
                break


def _merge_unique_text(existing: list[str], additions: list[str]) -> list[str]:
    merged = [str(item) for item in existing or [] if str(item).strip()]
    seen = {item.strip().lower() for item in merged}
    for item in additions or []:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        merged.append(text)
        seen.add(key)
    return merged


def _merge_unique_dicts(
    existing: list[dict[str, Any]],
    additions: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    merged = [dict(item) for item in existing or [] if isinstance(item, dict)]
    seen = {
        tuple(str(item.get(field) or "") for field in key_fields)
        for item in merged
    }
    for item in additions or []:
        if not isinstance(item, dict):
            continue
        candidate = dict(item)
        key = tuple(str(candidate.get(field) or "") for field in key_fields)
        if key in seen:
            continue
        merged.append(candidate)
        seen.add(key)
    return merged


def _damage_item_text(item: dict[str, Any]) -> str:
    parts = [
        item.get("section_label") or item.get("section"),
        item.get("panel"),
        item.get("condition"),
        item.get("reported_severity") or item.get("severity"),
        item.get("action"),
        item.get("notes"),
    ]
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def _mechanical_finding_text(finding: dict[str, Any]) -> str:
    parts = [finding.get("system"), finding.get("condition"), finding.get("notes")]
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def _summarize_report_damage_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_color: dict[str, int] = {}
    by_section: dict[str, int] = {}
    structural_issue = False
    for item in items or []:
        section = str(item.get("section") or "other")
        color = str(item.get("severity_color") or "gray")
        by_section[section] = by_section.get(section, 0) + 1
        by_color[color] = by_color.get(color, 0) + 1
        text = _damage_item_text(item).lower()
        if section == "structure" or "structural" in text or "frame" in text or color == "red":
            structural_issue = True
    return {
        "total_items": len(items or []),
        "by_color": by_color,
        "by_section": by_section,
        "structural_issue": structural_issue,
    }


def extract_announcements(text: str) -> list[str]:
    match = re.search(r"Announcements\s+(.*?)(?:Remarks|Seller Comments|Title|$)", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    value = " ".join(match.group(1).split())
    if not value or value.lower().startswith("no announcements"):
        return []
    parts = [part.strip(" .") for part in re.split(r"[;|]", value) if part.strip(" .")]
    return parts or [value]


def extract_single_value(text: str, label: str) -> str | None:
    pattern = re.compile(
        rf"{re.escape(label)}\s*(.*?)(?:Announcements|Remarks|Seller Comments|Title Status|Title State|Title Branding|Contact:|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return None
    value = " ".join(match.group(1).split()).strip(" .:-")
    return value or None


def extract_count(text: str, label: str) -> int | None:
    match = re.search(rf"{re.escape(label)}\D*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def build_problem_highlights(report: ConditionReport) -> list[str]:
    highlights: list[str] = []
    highlights.extend(report.announcements)
    highlights.extend(report.remarks)
    highlights.extend(report.seller_comments_items)
    return [value for value in highlights if value]


def _coerce_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                text = item.strip()
            elif isinstance(item, dict):
                text = str(item.get("text") or item.get("value") or "").strip()
            else:
                text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []


def _extract_listing_condition_damage_items(condition_enrichment: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    damage_items: list[dict[str, Any]] = []
    mechanical_findings: list[dict[str, Any]] = []
    raw_damages = condition_enrichment.get("damages")
    if not isinstance(raw_damages, list):
        return damage_items, mechanical_findings

    for raw in raw_damages:
        if not isinstance(raw, dict):
            continue
        category = str(raw.get("category") or "").strip()
        item = str(raw.get("item") or "").strip()
        damage = str(raw.get("damage") or "").strip()
        severity = str(raw.get("severity") or "").strip()
        action = str(raw.get("action") or "").strip()
        notes = str(raw.get("notes") or "").strip()
        if not any((category, item, damage, severity, action, notes)):
            continue
        section = _normalize_damage_section(category, item)
        severity_color = _listing_damage_severity_color(category, item, damage, severity, notes)
        severity_label, severity_rank = _listing_severity_rank(severity_color)
        normalized = {
            "source": "listing_json.conditionEnrichment.damages",
            "section": section,
            "section_label": category or section.title(),
            "panel": item,
            "condition": damage,
            "reported_severity": severity,
            "severity_color": severity_color,
            "severity_label": severity_label,
            "severity_rank": severity_rank,
            "action": action,
            "notes": notes,
            "raw": raw,
        }
        damage_items.append(normalized)
        if _looks_like_mechanical_damage(category, item, damage, severity, notes):
            condition_parts = [damage, severity, action, notes]
            mechanical_findings.append(
                {
                    "source": "listing_json.conditionEnrichment.damages",
                    "section": "mechanical",
                    "section_label": "Mechanical",
                    "system": item or category or "Mechanical",
                    "condition": " ".join(part for part in condition_parts if part),
                    "notes": notes,
                    "raw": raw,
                }
            )
    return damage_items, mechanical_findings


def _normalize_damage_section(category: str, item: str) -> str:
    signal = f"{category} {item}".lower()
    if "structure" in signal or "frame" in signal:
        return "structure"
    if "mechanical" in signal or "engine" in signal or "transmission" in signal or "battery" in signal:
        return "mechanical"
    if "interior" in signal:
        return "interior"
    if "tire" in signal or "wheel" in signal:
        return "tires"
    return "other" if category.lower() == "miscellaneous" else "exterior"


def _listing_damage_severity_color(category: str, item: str, damage: str, severity: str, notes: str) -> str:
    signal = f"{category} {item} {damage} {severity} {notes}".lower()
    if any(token in signal for token in ("structural", "frame", "unacceptable", "no start", "does not start")):
        return "red"
    if any(token in signal for token in ("inop", "repair required", "replacement required", "warning - engine", "warning - electrical", "mechanical")):
        return "orange"
    if any(token in signal for token in ("suspect repair", "broken", "cracked", "missing")):
        return "yellow"
    return "gray"


def _listing_severity_rank(color: str) -> tuple[str, int]:
    if color == "red":
        return "severe", 4
    if color == "orange":
        return "major", 3
    if color == "yellow":
        return "moderate", 2
    return "minor", 1


def _looks_like_mechanical_damage(category: str, item: str, damage: str, severity: str, notes: str) -> bool:
    signal = f"{category} {item} {damage} {severity} {notes}".lower()
    return any(
        token in signal
        for token in (
            "mechanical",
            "engine",
            "trans",
            "drivetrain",
            "battery",
            "warning - engine",
            "warning - electrical",
            "no start",
            "does not start",
            "inop",
        )
    )


def _apply_listing_json(report: ConditionReport, listing: dict[str, Any]) -> None:
    """Populate ConditionReport fields from the embedded OVE listing JSON.

    The listing JSON is structurally stable (it's the OVE backend's
    serialized listing object) and is the most reliable source for
    announcements, conditionGrade, autocheck (owners/accidents),
    conditionReportUrl, paint colors, and the installedEquipment list.
    Pulling these fields from JSON instead of regex-parsing rendered HTML
    means they survive any front-end layout change.

    This helper is called BEFORE the regex-based extractors so that the
    JSON-derived values become the authoritative defaults; the regex
    extractors can still fill gaps that the JSON happens to omit.
    """
    # Announcements — pulled from the OVE listing JSON's
    # announcementsEnrichment.announcements field. For vehicles with no
    # formal announcements, the listing JSON's enrichment block is empty
    # but the auction-light status (greenLight/yellowLight/redLight) and
    # arbitrationRating ARE populated and ARE valid disclosure data: the
    # auctioneer's positive declaration that the vehicle is clean.
    #
    # We collect listing-JSON announcements here but DO NOT write them
    # directly to report.announcements yet — _apply_listing_json runs
    # BEFORE the regex CR parser, and that parser can overwrite
    # report.announcements with its own results. Instead we stash both
    # the listing-JSON announcements AND the synthetic light/arbitration
    # signals in a private metadata field that the post-parser merge
    # step at the end of normalize_condition_report uses to build the
    # final list.
    enrichment = listing.get("announcementsEnrichment") or {}
    raw_announcements = _coerce_text_list(enrichment.get("announcements"))
    listing_announcements = raw_announcements + _coerce_text_list(listing.get("announcements"))
    listing_remarks = (
        _coerce_text_list(enrichment.get("remarks"))
        + _coerce_text_list(listing.get("remarks"))
        + _coerce_text_list(listing.get("additionalAnnouncements"))
    )
    listing_seller_comments = _coerce_text_list(listing.get("comments"))
    synthetic: list[str] = []
    if listing.get("greenLight") is True:
        synthetic.append("Green Light")
    if listing.get("yellowLight") is True:
        synthetic.append("Yellow Light")
    if listing.get("redLight") is True:
        synthetic.append("Red Light")
    if listing.get("blueLight") is True:
        synthetic.append("Blue Light")
    # Arbitration Rating is intentionally NOT surfaced as a consumer-facing
    # announcement. Per user 2026-04-09 it's an internal/wholesale grading
    # metric that means nothing to retail buyers. The auction-light status
    # above is the consumer-meaningful version of the same signal.
    if listing.get("asIs") is True:
        synthetic.append("As-Is")
    if listing.get("salvageVehicle") is True:
        synthetic.append("Salvage Vehicle")
    if listing.get("hasFrameDamage") is True:
        synthetic.append("Frame Damage")
    if listing.get("previouslyCanadianListing") is True:
        synthetic.append("Previously Canadian")
    autocheck_obj = listing.get("autocheck") or {}
    if isinstance(autocheck_obj, dict):
        if autocheck_obj.get("titleAndProblemCheckOK") is False:
            synthetic.append("AutoCheck: Title or Problem Reported")
        if autocheck_obj.get("odometerCheckOK") is False:
            synthetic.append("AutoCheck: Odometer Issue Reported")
    metadata = dict(report.metadata)
    metadata["_listing_announcements"] = listing_announcements
    metadata["_listing_synthetic_announcements"] = synthetic
    metadata["_listing_remarks"] = listing_remarks
    metadata["_listing_seller_comments"] = listing_seller_comments
    report.metadata = metadata

    # Overall grade
    grade = listing.get("conditionGrade")
    if grade is not None and not report.overall_grade:
        report.overall_grade = str(grade)

    # Autocheck → vehicle history
    autocheck = listing.get("autocheck") or {}
    condition_enrichment = listing.get("conditionEnrichment") or {}
    history = dict(report.vehicle_history)
    owner_count = autocheck.get("ownerCount")
    if isinstance(owner_count, int) and "owners" not in history:
        history["owners"] = owner_count
    accidents = autocheck.get("numberOfAccidents")
    if isinstance(accidents, int) and "accidents" not in history:
        history["accidents"] = accidents
    if isinstance(condition_enrichment, dict):
        engine_starts = condition_enrichment.get("engineStarts")
        if isinstance(engine_starts, bool) and "engine_starts" not in history:
            history["engine_starts"] = engine_starts
        drivable = condition_enrichment.get("drivable")
        if isinstance(drivable, bool) and "drivable" not in history:
            history["drivable"] = drivable
    if history != report.vehicle_history:
        report.vehicle_history = history

    if isinstance(condition_enrichment, dict):
        damage_items, mechanical_findings = _extract_listing_condition_damage_items(condition_enrichment)
        if damage_items or mechanical_findings:
            metadata = dict(report.metadata)
            metadata["_listing_condition_damage_items"] = damage_items
            metadata["_listing_condition_mechanical_findings"] = mechanical_findings
            report.metadata = metadata

    # conditionReportUrl → metadata.report_link.href (canonical CR deep link
    # the VPS template uses for the "See Original Condition Report" button)
    cr_url = listing.get("conditionReportUrl")
    if isinstance(cr_url, str) and cr_url:
        canonical = cr_url
        if canonical.startswith("//"):
            canonical = "https:" + canonical
        elif canonical.startswith("/"):
            canonical = "https://www.ove.com" + canonical
        metadata = dict(report.metadata)
        existing_link = dict(metadata.get("report_link") or {})
        if not existing_link.get("href"):
            existing_link["href"] = canonical
            metadata["report_link"] = existing_link
            report.metadata = metadata

    # Paint color / code from designatedDescriptionEnrichment
    dde = listing.get("designatedDescriptionEnrichment") or {}
    designated = dde.get("designatedDescription") or {}
    colors = designated.get("colors") or {}
    exterior_colors = colors.get("exterior") or []
    primary_exterior: dict[str, Any] | None = None
    if isinstance(exterior_colors, list):
        for entry in exterior_colors:
            if isinstance(entry, dict) and entry.get("isPrimary"):
                primary_exterior = entry
                break
        if primary_exterior is None and exterior_colors:
            first = exterior_colors[0]
            if isinstance(first, dict):
                primary_exterior = first
    if primary_exterior is not None:
        normalized = primary_exterior.get("normalizedName") or primary_exterior.get("oemName")
        if isinstance(normalized, str) and not report.exterior_color:
            report.exterior_color = normalized
        oem_name = primary_exterior.get("oemName")
        if isinstance(oem_name, str) and not report.exterior_color_oem_name:
            report.exterior_color_oem_name = oem_name
        paint_code = primary_exterior.get("oemOptionCode")
        if isinstance(paint_code, str) and not report.exterior_paint_code:
            report.exterior_paint_code = paint_code
        rgb = primary_exterior.get("rgbHex")
        if isinstance(rgb, str) and not report.exterior_color_rgb:
            report.exterior_color_rgb = rgb
    # Top-level fallbacks for the short labels
    if not report.exterior_color:
        ext = listing.get("exteriorColor")
        if isinstance(ext, str) and ext:
            report.exterior_color = ext
    interior = listing.get("interiorColor")
    if isinstance(interior, str) and interior and not report.interior_color:
        report.interior_color = interior
    has_prior_paint = listing.get("hasPriorPaint")
    if isinstance(has_prior_paint, bool) and report.has_prior_paint is None:
        report.has_prior_paint = has_prior_paint
        # The CR template uses paint_condition as the visible string; mirror
        # the boolean if no other parser has populated it.
        if not report.paint_condition:
            report.paint_condition = "Prior Paint" if has_prior_paint else "No Prior Paint"

    # Installed equipment + high-value options. The detailed list lives at
    # designatedDescriptionEnrichment.installedEquipment (NOT at top-level —
    # the top-level "equipment" key is just a flat list of feature strings
    # like "Power Windows" with no pricing info).
    raw_equipment = (dde.get("installedEquipment") if isinstance(dde, dict) else None)
    if not isinstance(raw_equipment, list):
        raw_equipment = listing.get("installedEquipment")
    if isinstance(raw_equipment, list):
        normalized_equipment: list[dict[str, Any]] = []
        for item in raw_equipment:
            if not isinstance(item, dict):
                continue
            pricing = item.get("pricing") or {}
            msrp_obj = pricing.get("msrp") or {}
            invoice_obj = pricing.get("invoice") or {}
            msrp_amount = msrp_obj.get("amount") if isinstance(msrp_obj, dict) else None
            invoice_amount = invoice_obj.get("amount") if isinstance(invoice_obj, dict) else None
            entry: dict[str, Any] = {
                "id": item.get("id"),
                "primary_description": item.get("primaryDescription"),
                "extended_description": item.get("extendedDescription"),
                "classification": item.get("classification"),
                "installed_reason": item.get("installedReason"),
                "oem_option_code": item.get("oemOptionCode"),
                "msrp": msrp_amount,
                "invoice": invoice_amount,
                "generics": item.get("generics") or [],
            }
            normalized_equipment.append(entry)
        if normalized_equipment and not report.installed_equipment:
            report.installed_equipment = normalized_equipment
        # High-value filter: optional/build-data items with a real MSRP.
        # Standard equipment ($0 build data) is excluded — it's noise.
        high_value: list[dict[str, Any]] = []
        for entry in normalized_equipment:
            reason = entry.get("installed_reason")
            msrp = entry.get("msrp")
            if reason in ("Build Data", "Optional") and isinstance(msrp, (int, float)) and msrp > 0:
                high_value.append(entry)
        high_value.sort(key=lambda e: e.get("msrp") or 0, reverse=True)
        if high_value and not report.high_value_options:
            report.high_value_options = high_value

    # Stash the full listing JSON in metadata so the VPS template can render
    # any field it wants without us needing to predict every future use.
    if listing:
        metadata = dict(report.metadata)
        if "listing_json" not in metadata:
            metadata["listing_json"] = listing
            report.metadata = metadata

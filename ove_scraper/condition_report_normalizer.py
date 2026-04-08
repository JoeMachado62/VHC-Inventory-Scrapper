from __future__ import annotations

import re

from ove_scraper.cr_parsers import identify_report_family, parse_condition_report_text
from ove_scraper.schemas import ConditionReport


def normalize_condition_report(
    report: ConditionReport | None,
    *,
    raw_text: str | None,
    report_link: dict[str, object] | None,
) -> ConditionReport | None:
    if report is None and not raw_text and not report_link:
        return None

    report = report or ConditionReport()
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

    announcements = extract_announcements(text)
    remarks = None if has_structured_report else extract_single_value(text, "remarks")
    seller_comments = None if has_structured_report else extract_single_value(text, "seller comments")
    title_status = None if has_structured_report else extract_single_value(text, "title status")
    title_state = None if has_structured_report else extract_single_value(text, "title state")
    title_branding = None if has_structured_report else extract_single_value(text, "title branding")
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
    return report


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

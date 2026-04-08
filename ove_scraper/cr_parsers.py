from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


SEVERITY_COLOR_MAP = {
    "gray": ("none", 0),
    "grey": ("none", 0),
    "green": ("ok", 0),
    "yellow": ("moderate", 1),
    "amber": ("moderate", 1),
    "orange": ("major", 2),
    "red": ("severe", 3),
    "blue": ("info", 0),
}


@dataclass(frozen=True, slots=True)
class ReportParserDescriptor:
    family: str
    host: str
    path_hint: str
    supports_structured_damage: bool
    supports_tire_depths: bool
    supports_vehicle_history: bool
    notes: str


PARSER_DESCRIPTORS: tuple[ReportParserDescriptor, ...] = (
    ReportParserDescriptor(
        family="manheim_inspectionreport",
        host="inspectionreport.manheim.com",
        path_hint="/",
        supports_structured_damage=True,
        supports_tire_depths=True,
        supports_vehicle_history=True,
        notes="Primary Manheim inspection report with condition tables, body-map damage, and tire depth layout.",
    ),
    ReportParserDescriptor(
        family="manheim_insightcr",
        host="insightcr.manheim.com",
        path_hint="/cr-display",
        supports_structured_damage=True,
        supports_tire_depths=True,
        supports_vehicle_history=True,
        notes="Insight CR layout. Similar inspection data, but different section/component structure.",
    ),
    ReportParserDescriptor(
        family="liquidmotors_ir",
        host="content.liquidmotors.com",
        path_hint="/IR/",
        supports_structured_damage=True,
        supports_tire_depths=True,
        supports_vehicle_history=False,
        notes="Liquid Motors hosted inspection report HTML with legacy table-based layout.",
    ),
    ReportParserDescriptor(
        family="manheim_ecr",
        host="mmsc400.manheim.com",
        path_hint="/MABEL/ECR2I.htm",
        supports_structured_damage=True,
        supports_tire_depths=True,
        supports_vehicle_history=True,
        notes="Legacy Manheim ECR endpoint linked from CR score chips on OVE result cards.",
    ),
)


def identify_report_family(report_url: str | None) -> ReportParserDescriptor | None:
    if not report_url:
        return None
    parsed = urlparse(report_url)
    host = parsed.netloc.lower()
    path = parsed.path or "/"

    for descriptor in PARSER_DESCRIPTORS:
        if host == descriptor.host and path.startswith(descriptor.path_hint):
            return descriptor
    return None


def normalize_severity_color(color_name: str | None) -> tuple[str, int]:
    if not color_name:
        return ("unknown", -1)
    return SEVERITY_COLOR_MAP.get(color_name.strip().lower(), ("unknown", -1))


def parse_condition_report_text(report_url: str | None, report_text: str | None) -> dict[str, Any]:
    descriptor = identify_report_family(report_url)
    if descriptor is None or not report_text:
        return {}
    parsed: dict[str, Any] = {}
    if descriptor.family == "manheim_inspectionreport":
        parsed = _parse_manheim_inspectionreport(report_text)
    elif descriptor.family == "manheim_insightcr":
        parsed = _parse_manheim_insightcr(report_text)
    elif descriptor.family == "manheim_ecr":
        parsed = _parse_manheim_ecr(report_text)
    elif descriptor.family == "liquidmotors_ir":
        parsed = _parse_liquidmotors_ir(report_text)
    return _merge_parsed_reports(parsed, _parse_generic_condition_report(report_text))


def _merge_parsed_reports(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    for key, value in primary.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
            continue
        merged[key] = value
    return {key: value for key, value in merged.items() if value not in (None, [], {})}


def _parse_liquidmotors_ir(report_text: str) -> dict[str, Any]:
    if "DAMAGE SUMMARY AND ADDITIONAL IMAGES" in report_text or "Engine Starts-" in report_text:
        return _parse_manheim_ecr(report_text)
    return _parse_manheim_inspectionreport(report_text)


def _parse_generic_condition_report(report_text: str) -> dict[str, Any]:
    text = report_text.replace("\r", "")
    lines = _clean_lines(text)
    parsed: dict[str, Any] = {}

    grade_match = re.search(
        r"(?:^|\n)\s*(?:Grade\s+)?([0-5](?:\.\d)?)\s*(?:\n|\s+)(Extra Clean|Above Average|Below Average|Clean|Average|Rough|Extra|Above|Below)?(?:\s+(Clean|Average|Rough))?",
        text,
        flags=re.IGNORECASE,
    )
    if grade_match:
        parsed["overall_grade"] = grade_match.group(1)
        grade_label = " ".join(part for part in grade_match.groups()[1:] if part)
        if grade_label:
            parsed["grade_label"] = grade_label

    announcement_fields = _parse_labeled_section(
        lines,
        "ANNOUNCEMENTS & COMMENTS",
        "Title",
        ("Announcements", "Remarks", "Additional Announcements", "Seller Comments"),
    )
    announcements = [
        value
        for label in ("Announcements", "Additional Announcements")
        for value in announcement_fields.get(label, [])
        if value and value.lower() not in {"no announcements", "none"}
    ]
    if not announcements:
        announcements = [
            value
            for value in _extract_section_values(text, "Announcements", "Remarks")
            if value.lower() not in {"--", "no announcements", "none"}
        ]
    if announcements:
        parsed["announcements"] = announcements

    remarks = [
        value
        for value in announcement_fields.get("Remarks", [])
        if value and value.lower() not in {"no remarks", "none"}
    ]
    if not remarks:
        remarks = [
            value
            for value in _extract_section_values(text, "Remarks", "Seller Comments")
            if value and value.lower() not in {"no remarks", "none"}
        ]
    if remarks:
        parsed["remarks"] = remarks

    seller_comments = [
        value
        for value in announcement_fields.get("Seller Comments", [])
        if value and value.lower() not in {"no comments", "none"}
    ]
    if not seller_comments:
        seller_comments = [
            value
            for value in _extract_section_values(text, "Seller Comments", "Title Status")
            if value and value.lower() not in {"no comments", "none"}
        ]
    if seller_comments:
        parsed["seller_comments"] = seller_comments

    title_fields = _parse_labeled_section(
        lines,
        "Title",
        "CONDITION DETAILS",
        ("Title Status", "Title State", "Title Branding"),
    )
    if title_fields.get("Title Status"):
        parsed["title_status"] = title_fields["Title Status"][0]
    elif title_status := (_line_after(lines, "Title Status") or _extract_line_value(text, "Title Status")):
        parsed["title_status"] = title_status
    if title_fields.get("Title State"):
        parsed["title_state"] = title_fields["Title State"][0]
    elif title_state := (_line_after(lines, "Title State") or _extract_line_value(text, "Title State")):
        parsed["title_state"] = title_state
    if title_fields.get("Title Branding"):
        parsed["title_branding"] = title_fields["Title Branding"][0]
    elif title_branding := (_line_after(lines, "Title Branding") or _extract_line_value(text, "Title Branding")):
        parsed["title_branding"] = title_branding

    if re.search(r"No Structural Damage", text, flags=re.IGNORECASE):
        parsed["structural_damage"] = False
    elif re.search(r"Structural Damage", text, flags=re.IGNORECASE):
        parsed["structural_damage"] = True

    if re.search(r"No Prior\s+Paint", text, flags=re.IGNORECASE):
        parsed["paint_condition"] = "No Prior Paint"
    elif re.search(r"Prior\s+Paint", text, flags=re.IGNORECASE):
        parsed["paint_condition"] = "Prior Paint"

    owners = _extract_numeric_count(text, "Owners")
    accidents = _extract_numeric_count(text, "ACDNT")
    if accidents is None:
        accidents = _extract_numeric_count(text, "Accidents")
    if owners is not None:
        parsed["owners"] = owners
    if accidents is not None:
        parsed["accidents"] = accidents

    engine_starts = _extract_yes_no(text, "Engine Starts")
    if engine_starts is None:
        starts_value = _extract_key_value_block(text, "VEHICLE STARTS")
        if starts_value:
            engine_starts = starts_value.lower().startswith("yes")
    if engine_starts is not None:
        parsed["engine_starts"] = engine_starts

    drivable = _extract_yes_no(text, "Drivable")
    if drivable is None:
        drives_value = _extract_key_value_block(text, "VEHICLE DRIVES")
        if drives_value:
            drivable = drives_value.lower().startswith("yes")
    if drivable is not None:
        parsed["drivable"] = drivable

    if parsed.get("overall_grade") and parsed.get("grade_label"):
        parsed["ai_summary"] = f"Condition grade {parsed['overall_grade']} ({parsed['grade_label']})."

    return parsed


def _parse_manheim_inspectionreport(report_text: str) -> dict[str, Any]:
    text = report_text.replace("\r", "")
    lines = _clean_lines(text)
    parsed: dict[str, Any] = {}

    grade_match = re.search(
        r"(?:^|\n)\s*([0-5](?:\.\d)?)\s*\n\s*(Clean|Average|Rough|Below Average|Above Average)\s*(?:\n|$)",
        text,
        flags=re.IGNORECASE,
    )
    if grade_match:
        parsed["overall_grade"] = grade_match.group(1)
        parsed["grade_label"] = grade_match.group(2)

    announcements = _extract_section_values(text, "Announcements", "Remarks/Comments")
    if announcements and announcements != ["--"]:
        parsed["announcements"] = announcements

    remarks = _extract_section_values(text, "Remarks/Comments", "Title")
    if remarks and remarks != ["--"]:
        parsed["remarks"] = remarks

    title_state = _line_after(lines, "TITLE STATE")
    if title_state and title_state != "--":
        parsed["title_state"] = title_state
    title_status = _line_after(lines, "TITLE STATUS")
    if title_status:
        parsed["title_status"] = title_status

    vehicle_starts = _extract_key_value_block(text, "VEHICLE STARTS")
    if vehicle_starts:
        parsed["engine_starts"] = vehicle_starts.lower().startswith("yes")

    vehicle_drives = _extract_key_value_block(text, "VEHICLE DRIVES")
    if vehicle_drives:
        parsed["drivable"] = vehicle_drives.lower().startswith("yes")

    tire_depths = _parse_inspectionreport_tires(text)
    if tire_depths:
        parsed["tire_depths"] = tire_depths

    damage_items, mechanical_findings, diagnostic_codes = _parse_inspectionreport_findings(lines)
    if damage_items:
        parsed["damage_items"] = damage_items
        parsed["damage_summary"] = _summarize_damage_items(damage_items)
    if mechanical_findings:
        parsed["mechanical_findings"] = mechanical_findings
    if diagnostic_codes:
        parsed["diagnostic_codes"] = diagnostic_codes
    highlights = _build_damage_highlights(damage_items) + _build_mechanical_highlights(mechanical_findings, diagnostic_codes)
    if highlights:
        parsed["problem_highlights"] = highlights
    severity_summary = _summarize_damage_severity(damage_items)
    if mechanical_findings and severity_summary in {None, "minor"}:
        severity_summary = "major" if any(
            "scan not available" in str(item.get("condition", "")).lower() for item in mechanical_findings
        ) else "moderate"
    if severity_summary:
        parsed["severity_summary"] = severity_summary
    parsed["ai_summary"] = _build_inspection_ai_summary(parsed)

    return {key: value for key, value in parsed.items() if value not in (None, [], {})}


def _parse_manheim_ecr(report_text: str) -> dict[str, Any]:
    text = report_text.replace("\r", "")
    parsed: dict[str, Any] = {}

    grade_match = re.search(r"Grade\s+([0-5](?:\.\d)?)\s+([A-Za-z]+)", text)
    if grade_match:
        parsed["overall_grade"] = grade_match.group(1)
        parsed["grade_label"] = grade_match.group(2)

    engine_starts = _extract_yes_no(text, "Engine Starts")
    if engine_starts is not None:
        parsed["engine_starts"] = engine_starts

    drivable = _extract_yes_no(text, "Drivable")
    if drivable is not None:
        parsed["drivable"] = drivable

    odor = _extract_line_value(text, "Int Odor")
    if odor:
        parsed["interior_condition"] = odor

    title_state = _extract_line_value(text, "Title State")
    if title_state:
        parsed["title_state"] = title_state

    title_received_date = _extract_line_value(text, "Title Received Date")
    if title_received_date:
        parsed["title_received_date"] = title_received_date

    additional_info = _extract_block_lines(text, "ADDITIONAL INFORMATION", "Common Abbreviations")
    if additional_info:
        parsed["remarks"] = additional_info

    tire_depths = _parse_manheim_ecr_tires(text)
    if tire_depths:
        parsed["tire_depths"] = tire_depths

    damage_items = _parse_manheim_ecr_damage_items(text)
    if damage_items:
        parsed["damage_items"] = damage_items
        parsed["damage_summary"] = _summarize_damage_items(damage_items)
        parsed["problem_highlights"] = _build_damage_highlights(damage_items)
        parsed["severity_summary"] = _summarize_damage_severity(damage_items)

    return parsed


def _parse_manheim_insightcr(report_text: str) -> dict[str, Any]:
    text = report_text.replace("\r", "")
    lines = _clean_lines(text)
    parsed: dict[str, Any] = {}

    grade_match = re.search(r"(?:^|\n)\s*([0-5](?:\.\d)?)\s*\n\s*(Extra|Clean|Average|Rough|Above|Below)\s*\n\s*(Clean|Average|Rough)?", text, flags=re.IGNORECASE)
    if grade_match:
        parsed["overall_grade"] = grade_match.group(1)
        grade_parts = [part for part in (grade_match.group(2), grade_match.group(3)) if part]
        if grade_parts:
            parsed["grade_label"] = " ".join(grade_parts)

    announcement_fields = _parse_labeled_section(
        lines,
        "ANNOUNCEMENTS & COMMENTS",
        "Title",
        ("Announcements", "Remarks", "Additional Announcements", "Seller Comments"),
    )
    announcements = [
        value
        for label in ("Announcements", "Additional Announcements")
        for value in announcement_fields.get(label, [])
        if value and value.lower() not in {"no announcements", "none"}
    ]
    if announcements:
        parsed["announcements"] = announcements
    remarks = [
        value
        for value in announcement_fields.get("Remarks", [])
        if value and value.lower() not in {"no remarks", "none"}
    ]
    if remarks:
        parsed["remarks"] = remarks
    seller_comments = [
        value
        for value in announcement_fields.get("Seller Comments", [])
        if value and value.lower() not in {"no comments", "none"}
    ]
    if seller_comments:
        parsed["seller_comments"] = seller_comments

    title_fields = _parse_labeled_section(
        lines,
        "Title",
        "CONDITION DETAILS",
        ("Title Status", "Title State", "Title Branding"),
    )
    if title_fields.get("Title Status"):
        parsed["title_status"] = title_fields["Title Status"][0]
    if title_fields.get("Title State"):
        parsed["title_state"] = title_fields["Title State"][0]
    if title_fields.get("Title Branding"):
        parsed["title_branding"] = title_fields["Title Branding"][0]

    if re.search(r"No Structural Damage", text, flags=re.IGNORECASE):
        parsed["structural_damage"] = False
    elif re.search(r"Structural Damage", text, flags=re.IGNORECASE):
        parsed["structural_damage"] = True

    if re.search(r"No Prior\s+Paint", text, flags=re.IGNORECASE):
        parsed["paint_condition"] = "No Prior Paint"
    elif re.search(r"Prior\s+Paint", text, flags=re.IGNORECASE):
        parsed["paint_condition"] = "Prior Paint"

    owners = _extract_numeric_count(text, "Owners")
    accidents = _extract_numeric_count(text, "ACDNT")
    if owners is not None:
        parsed["owners"] = owners
    if accidents is not None:
        parsed["accidents"] = accidents
    if re.search(r"(?:^|\n)\s*Drivable\s*(?:\n|$)", text, flags=re.IGNORECASE):
        parsed["drivable"] = True
    if re.search(r"(?:^|\n)\s*Start\s*(?:\n|$)", text, flags=re.IGNORECASE):
        parsed["engine_starts"] = True

    tire_depths = _parse_manheim_insightcr_tires(lines)
    if tire_depths:
        parsed["tire_depths"] = tire_depths

    damage_summary = _parse_manheim_insightcr_damage_summary(lines)
    if damage_summary:
        parsed["damage_summary"] = damage_summary
        if damage_summary.get("structural_issue"):
            parsed["severity_summary"] = "major"

    highlights: list[str] = []
    highlights.extend(announcements)
    highlights.extend(remarks)
    highlights.extend(seller_comments)
    if parsed.get("title_status") and str(parsed["title_status"]).lower() not in {"--", "not specified"}:
        highlights.append(f"Title status: {parsed['title_status']}")
    if highlights:
        parsed["problem_highlights"] = highlights[:8]

    if parsed.get("overall_grade") and parsed.get("grade_label"):
        parsed["ai_summary"] = f"Condition grade {parsed['overall_grade']} ({parsed['grade_label']})."

    return {key: value for key, value in parsed.items() if value not in (None, [], {})}


def _extract_yes_no(text: str, label: str) -> bool | None:
    match = re.search(rf"{re.escape(label)}-([A-Za-z]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    value = match.group(1).strip().lower()
    if value == "yes":
        return True
    if value == "no":
        return False
    return None


def _extract_line_value(text: str, label: str) -> str | None:
    match = re.search(rf"{re.escape(label)}:\s*([^\n]*)", text, flags=re.IGNORECASE)
    if not match:
        return None
    value = " ".join(match.group(1).split()).strip()
    return value or None


def _extract_block_lines(text: str, start_label: str, end_label: str) -> list[str]:
    match = re.search(
        rf"{re.escape(start_label)}\s*(.*?){re.escape(end_label)}",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    lines = [
        " ".join(line.split()).strip(" *")
        for line in match.group(1).splitlines()
        if line.strip()
    ]
    return [line for line in lines if line]


def _parse_labeled_section(
    lines: list[str],
    start_label: str,
    end_label: str,
    field_labels: tuple[str, ...],
) -> dict[str, list[str]]:
    start_index = next((index for index, line in enumerate(lines) if line == start_label), None)
    if start_index is None:
        return {}
    end_index = next((index for index in range(start_index + 1, len(lines)) if lines[index] == end_label), len(lines))
    fields = {label: [] for label in field_labels}
    current_label: str | None = None
    label_set = set(field_labels)
    for line in lines[start_index + 1 : end_index]:
        if line in label_set:
            current_label = line
            continue
        if current_label is None:
            continue
        fields[current_label].append(line)
    return {label: values for label, values in fields.items() if values}


def _parse_manheim_insightcr_tires(lines: list[str]) -> dict[str, dict[str, str]]:
    start_index = next((index for index, line in enumerate(lines) if line == "TIRES AND WHEELS"), None)
    if start_index is None:
        return {}
    end_index = next((index for index in range(start_index + 1, len(lines)) if lines[index] == "EQUIPMENT & OPTIONS"), len(lines))
    block = lines[start_index + 1 : end_index]

    wheel_type = _value_after_label(block, "Wheels")
    positions = {
        "Left Front": "left_front",
        "Right Front": "right_front",
        "Left Rear": "left_rear",
        "Right Rear": "right_rear",
        "Spare": "spare",
    }
    result: dict[str, dict[str, str]] = {}
    for index, line in enumerate(block):
        if line not in positions:
            continue
        values: list[str] = []
        cursor = index + 1
        while cursor < len(block) and block[cursor] not in positions and len(values) < 4:
            values.append(block[cursor])
            cursor += 1
        if not values:
            continue

        brand = next((value for value in values if not _looks_like_tire_depth(value) and not _looks_like_tire_size(value)), "")
        depth = next((value for value in values if _looks_like_tire_depth(value)), "")
        size = next((value for value in values if _looks_like_tire_size(value)), "")
        item = {
            "position_label": line,
            "wheel_type": wheel_type or "",
            "brand": brand,
            "tread_depth": depth,
            "size": size,
        }
        result[positions[line]] = item
    return result


def _parse_manheim_insightcr_damage_summary(lines: list[str]) -> dict[str, Any]:
    start_index = next((index for index, line in enumerate(lines) if line == "CONDITION DETAILS"), None)
    if start_index is None:
        return {}

    section_counts: dict[str, int] = {}
    structural_issue = False
    for line in lines[start_index + 1 :]:
        match = re.match(r"^(Exterior|Interior|Structure|Other) \((\d+)\)$", line, flags=re.IGNORECASE)
        if match:
            section_key = normalize_section(match.group(1))
            count = int(match.group(2))
            section_counts[section_key] = count
            if section_key == "structure" and count > 0:
                structural_issue = True
            continue
        if line in {"OVERVIEW", "TIRES AND WHEELS"}:
            break
        if line == "Structural Issue":
            continue
        if line.lower().startswith("no structural damage"):
            structural_issue = False

    if not section_counts and not structural_issue:
        return {}

    total_items = sum(section_counts.values())
    return {
        "total_items": total_items,
        "by_section": section_counts,
        "by_color": {},
        "structural_issue": structural_issue,
    }


def _value_after_label(lines: list[str], label: str) -> str | None:
    for index, line in enumerate(lines):
        if line == label and index + 1 < len(lines):
            return lines[index + 1]
    return None


def _extract_numeric_count(text: str, label: str) -> int | None:
    match = re.search(rf"{re.escape(label)}\D*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _looks_like_tire_depth(value: str) -> bool:
    lowered = value.lower()
    return "32" in lowered or lowered in {"n/a", "na"}


def _looks_like_tire_size(value: str) -> bool:
    return bool(re.search(r"\d{2,3}/\d{2,3}r\d{2}", value, flags=re.IGNORECASE))


def _extract_section_values(text: str, start_label: str, end_label: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines()]
    collecting = False
    values: list[str] = []
    for line in lines:
        if not collecting and line.lower() == start_label.lower():
            collecting = True
            continue
        if collecting and line.lower() == end_label.lower():
            break
        if collecting and line:
            values.append(" ".join(line.split()).strip())
    return [value for value in values if value]


def _extract_key_value_block(text: str, label: str) -> str | None:
    match = re.search(rf"{re.escape(label)}\s*\n(.*?)\n", text, flags=re.IGNORECASE)
    if not match:
        return None
    value = " ".join(match.group(1).split()).strip()
    return value or None


def _parse_manheim_ecr_tires(text: str) -> dict[str, dict[str, str]]:
    match = re.search(
        r"TIRES AND WHEELS\s+Wheels:\s*([^\n]+)\s+Tire\s+Tread Depth\s+Brand\s+Size\s+(.*?)(?:\n\s*KEYS|\n\s*OTHER|\n\s*ADDITIONAL INFORMATION|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return {}

    wheels = " ".join(match.group(1).split()).strip()
    rows = {}
    for raw_line in match.group(2).splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        parts = [part.strip() for part in raw_line.split("\t") if part.strip()]
        if len(parts) < 4:
            continue
        position = normalize_position(parts[0].rstrip(":"))
        rows[position] = {
            "position_label": parts[0].rstrip(":"),
            "tread_depth": parts[1],
            "brand": parts[2],
            "size": parts[3],
            "wheel_type": wheels,
        }
    return rows


def _parse_inspectionreport_tires(text: str) -> dict[str, dict[str, str]]:
    positions = {
        "DRIVER FRONT TIRE DEPTH": "driver_front",
        "DRIVER REAR TIRE DEPTH": "driver_rear",
        "PASSENGER FRONT TIRE DEPTH": "passenger_front",
        "PASSENGER REAR TIRE DEPTH": "passenger_rear",
    }
    result: dict[str, dict[str, str]] = {}
    for label, key in positions.items():
        depth = _extract_key_value_block(text, label)
        issue = _extract_key_value_block(text, label.replace("DEPTH", "& WHEEL ISSUE"))
        if depth or issue:
            result[key] = {
                "position_label": label.replace(" TIRE DEPTH", "").title(),
                "tread_depth": depth or "",
                "issue": issue or "",
            }
    return result


def _parse_manheim_ecr_damage_items(text: str) -> list[dict[str, Any]]:
    match = re.search(
        r"DAMAGE SUMMARY AND ADDITIONAL IMAGES\s+(.*?)(?:VIN:\s*[A-HJ-NPR-Z0-9]{17}|©\s*\d{4}\s*Manheim|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []

    block = match.group(1)
    items: list[dict[str, Any]] = []
    current_section: str | None = None
    current_count = 0

    for raw_line in block.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        if line in {"Open All Damages", "Close All Damages"}:
            continue
        if line.startswith("IMAGE DESCRIPTION CONDITION SEVERITY"):
            continue
        if line.startswith("0000 Picture#"):
            continue

        section_match = re.match(r"(.+)-\[(\d+) Items\]$", line)
        if section_match:
            current_section = section_match.group(1).strip()
            current_count = int(section_match.group(2))
            continue

        parts = [part.strip() for part in raw_line.split("\t") if part.strip()]
        if current_section and len(parts) >= 3:
            item = _build_damage_item(current_section, parts)
            if item:
                items.append(item)

    if current_count and not items:
        return []
    return items


def _parse_inspectionreport_findings(lines: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    damage_items: list[dict[str, Any]] = []
    mechanical_findings: list[dict[str, Any]] = []
    diagnostic_codes: list[str] = []

    exterior_lines = _slice_lines(lines, "Exterior", "Interior")
    interior_lines = _slice_lines(lines, "Interior", "Mechanical & Diagnostic Trouble Codes")
    mechanical_lines = _slice_lines(lines, "Mechanical & Diagnostic Trouble Codes", "Tires & Wheels")

    for section_name, section_lines in (("exterior", exterior_lines), ("interior", interior_lines)):
        pairs = _pairwise_damage_findings(section_lines, section_name)
        for label, value in pairs:
            if _is_non_issue_value(value):
                continue
            severity_color = infer_severity_color(section_name, label, value)
            severity_label, severity_rank = normalize_severity_color(severity_color)
            damage_items.append(
                {
                    "section": section_name,
                    "section_label": section_name.title(),
                    "panel": label,
                    "condition": value,
                    "reported_severity": value,
                    "severity_color": severity_color,
                    "severity_label": severity_label,
                    "severity_rank": severity_rank,
                }
            )

    for label, value in _pairwise_mechanical_findings(mechanical_lines):
        if _is_non_issue_value(value):
            continue
        finding = {
            "section": "mechanical",
            "section_label": "Mechanical",
            "system": label,
            "condition": value,
        }
        if label == "NOTE:":
            diagnostic_codes.extend([code.strip() for code in value.split(",") if code.strip()])
            continue
        mechanical_findings.append(finding)

    return _dedupe_damage_items(damage_items), _dedupe_mechanical_findings(mechanical_findings), diagnostic_codes


def _looks_like_issue_label(value: str) -> bool:
    upper = value.upper()
    return upper == value and len(value) > 2 and not value.isdigit()


def _looks_like_issue_value(value: str) -> bool:
    return value not in {"--"} and len(value.strip()) > 0


def _dedupe_damage_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("section")), str(item.get("panel")), str(item.get("condition")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _dedupe_mechanical_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("system")), str(item.get("condition")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _build_damage_item(section: str, parts: list[str]) -> dict[str, Any] | None:
    if len(parts) < 3:
        return None
    panel = parts[0]
    condition = parts[1]
    severity_text = parts[2] if len(parts) >= 3 else ""
    severity_color = infer_severity_color(section, condition, severity_text)
    severity_label, severity_rank = normalize_severity_color(severity_color)
    return {
        "section": normalize_section(section),
        "section_label": section,
        "panel": panel,
        "condition": condition,
        "reported_severity": severity_text,
        "severity_color": severity_color,
        "severity_label": severity_label,
        "severity_rank": severity_rank,
    }


def infer_severity_color(section: str, condition: str, severity_text: str) -> str:
    signal = f"{section} {condition} {severity_text}".lower()
    if "structur" in signal or "severe damage" in signal:
        return "red"
    if any(token in signal for token in ("replace", "cracked", "crack", "cut", "broken", "major", "severe")):
        return "orange"
    if any(token in signal for token in ("dent", "chipped", "chip", "scratch", "worn", "rash", "bug damage")):
        return "yellow"
    if "acceptable" in signal:
        return "yellow"
    return "gray"


def _summarize_damage_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_color: dict[str, int] = {}
    by_section: dict[str, int] = {}
    structural_issue = False
    for item in items:
        color = str(item.get("severity_color") or "gray")
        by_color[color] = by_color.get(color, 0) + 1
        section = str(item.get("section") or "other")
        by_section[section] = by_section.get(section, 0) + 1
        if color == "red" or section == "structure":
            structural_issue = True
    return {
        "total_items": len(items),
        "by_color": by_color,
        "by_section": by_section,
        "structural_issue": structural_issue,
    }


def _build_damage_highlights(items: list[dict[str, Any]]) -> list[str]:
    highlights: list[str] = []
    for item in items[:8]:
        panel = str(item.get("panel") or "Unknown panel")
        condition = str(item.get("condition") or "").strip()
        severity_text = str(item.get("reported_severity") or "").strip()
        text = f"{panel}: {condition}"
        if severity_text:
            text = f"{text} ({severity_text})"
        highlights.append(text)
    return highlights


def _summarize_damage_severity(items: list[dict[str, Any]]) -> str | None:
    ranks = [int(item.get("severity_rank", -1)) for item in items if isinstance(item.get("severity_rank"), int)]
    if not ranks:
        return None
    max_rank = max(ranks)
    if max_rank >= 3:
        return "severe"
    if max_rank == 2:
        return "major"
    if max_rank == 1:
        return "moderate"
    return "minor"


def _clean_lines(text: str) -> list[str]:
    return [" ".join(line.replace("\xa0", " ").split()).strip() for line in text.splitlines() if line.strip()]


def _line_after(lines: list[str], label: str) -> str | None:
    for index, line in enumerate(lines):
        if line == label and index + 1 < len(lines):
            return lines[index + 1]
    return None


def _slice_lines(lines: list[str], start: str, end: str) -> list[str]:
    start_index = next((i for i, line in enumerate(lines) if line == start), None)
    if start_index is None:
        return []
    end_index = next((i for i in range(start_index + 1, len(lines)) if lines[i] == end), len(lines))
    return lines[start_index + 1 : end_index]


def _pairwise_damage_findings(lines: list[str], section_name: str) -> list[tuple[str, str]]:
    context_headers = {
        "FRONT EXTERIOR",
        "DRIVER EXTERIOR",
        "ROOF - EXTERIOR",
        "REAR EXTERIOR",
        "PASSENGER EXTERIOR",
        "FURTHER DISCLOSURES",
        "INTERIOR COSMETIC DAMAGE",
        "AIRBAGS",
        "INFOTAINMENT/RADIO",
        "CLIMATE CONTROL",
        "SUNROOF OPERATION",
        "ELECTRICAL ACCESSORY",
    }
    findings: list[tuple[str, str]] = []
    index = 0
    while index < len(lines) - 1:
        current = lines[index]
        nxt = lines[index + 1]

        if current in context_headers or _looks_like_section_header(current, section_name):
            if _looks_like_finding_label(nxt, section_name) and index + 2 < len(lines):
                findings.append((nxt, lines[index + 2]))
                index += 3
                continue
            if _looks_like_issue_value(nxt):
                findings.append((current, nxt))
                index += 2
                continue
            index += 1
            continue

        if not _looks_like_finding_label(current, section_name):
            index += 1
            continue

        findings.append((current, nxt))
        index += 2
    return findings


def _pairwise_mechanical_findings(lines: list[str]) -> list[tuple[str, str]]:
    known_labels = {
        "DIAGNOSTIC TROUBLE CODES",
        "WARNING LIGHTS & GAUGE CLUSTER",
        "VEHICLE SMOKE",
        "EMISSIONS/CATALYTIC/EXHAUST",
        "ACTIVE VISIBLE LEAKS FROM ENGINE OR UNDERCARRIAGE AREA",
        "ENGINE NOISE",
        "ENGINE OIL SLUDGE",
        "OTHER MECHANICAL COMMENTS",
        "NOTE:",
    }
    findings: list[tuple[str, str]] = []
    index = 0
    while index < len(lines) - 1:
        current = lines[index]
        nxt = lines[index + 1]
        if current not in known_labels:
            index += 1
            continue
        findings.append((current, nxt))
        index += 2
    return findings


def _looks_like_section_header(value: str, section_name: str) -> bool:
    upper = value.upper()
    if section_name == "exterior" and upper.endswith("EXTERIOR"):
        return True
    if section_name == "interior" and upper in {
        "INTERIOR COSMETIC DAMAGE",
        "AIRBAGS",
        "INFOTAINMENT/RADIO",
        "CLIMATE CONTROL",
        "SUNROOF OPERATION",
        "ELECTRICAL ACCESSORY",
        "FURTHER DISCLOSURES",
    }:
        return True
    return False


def _looks_like_finding_label(value: str, section_name: str) -> bool:
    upper = value.upper()
    if upper in {"NOTE:", "TITLE STATE", "TITLE STATUS"}:
        return False
    if section_name == "exterior" and upper.endswith("EXTERIOR"):
        return False
    if section_name == "interior" and upper in {
        "INTERIOR COSMETIC DAMAGE",
        "AIRBAGS",
        "INFOTAINMENT/RADIO",
        "CLIMATE CONTROL",
        "SUNROOF OPERATION",
        "ELECTRICAL ACCESSORY",
        "FURTHER DISCLOSURES",
    }:
        return False
    return bool(upper == value and len(value) > 2 and not value.isdigit())


def _is_non_issue_value(value: str) -> bool:
    return value in {"No Damage", "No Issues", "Fully Functional", "Not Specified", "Factory Equipment Installed", "No Oil Sludge", "None", "--"}


def _build_mechanical_highlights(findings: list[dict[str, Any]], diagnostic_codes: list[str]) -> list[str]:
    highlights: list[str] = []
    for finding in findings:
        system = str(finding.get("system") or "").strip()
        condition = str(finding.get("condition") or "").strip()
        if not system or not condition:
            continue
        if system == "DIAGNOSTIC TROUBLE CODES" and "scan not available" in condition.lower():
            highlights.append("Diagnostic trouble code scan not available")
        elif system == "WARNING LIGHTS & GAUGE CLUSTER":
            highlights.append(f"Warning lights / gauge cluster: {condition}")
        elif system == "OTHER MECHANICAL COMMENTS":
            highlights.append(f"Mechanical comments: {condition}")
    if diagnostic_codes:
        highlights.append(f"OBDII codes: {', '.join(diagnostic_codes)}")
    return highlights


def _build_inspection_ai_summary(parsed: dict[str, Any]) -> str | None:
    highlights = list(parsed.get("problem_highlights") or [])
    if not highlights:
        return None
    summary = "; ".join(highlights[:4])
    if parsed.get("severity_summary") == "severe":
        prefix = "Arbitration-relevant concerns identified"
    elif parsed.get("severity_summary") in {"major", "moderate"}:
        prefix = "Condition report flags notable issues"
    else:
        prefix = "Condition report notes issues"
    return f"{prefix}: {summary}."


def normalize_position(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return normalized


def normalize_section(value: str) -> str:
    return normalize_position(value).replace("paint_and_body_requires_conventional_repair", "exterior")

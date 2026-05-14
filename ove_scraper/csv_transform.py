from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ove_scraper.location_zip_lookup import resolve_location_zip
from ove_scraper.schemas import VehiclePayload


REDACTED_COLUMNS = {
    "listing seller",
    "seller name",
    "current bid",
    "high bid",
}

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "vin": ("VIN", "Vin"),
    "year": ("Year",),
    "make": ("Make",),
    "model": ("Model",),
    "trim": ("Trim",),
    "body_type": ("Body Style", "Body Type"),
    "sub_body_type": ("Sub Body Type",),
    "engine_type": ("Engine",),
    "cylinders": ("Cylinders",),
    "forced_induction": ("Forced Induction",),
    "drivetrain": ("Drivetrain", "Drive"),
    "mpg_combined": ("MPG Combined",),
    "ev_range": ("EV Range",),
    "towing_capacity_lbs": ("Towing Capacity", "Towing Capacity Lbs"),
    "odometer": ("Mileage", "Odometer", "Odometer Value"),
    "condition_grade": ("Condition", "Grade", "Condition Report Grade"),
    "price_asking": ("Asking Price", "Buy Now", "Floor Price", "Buy Now Price"),
    "price_wholesale_est": ("MMR", "Wholesale Value"),
    "location_zip": ("Location ZIP", "Seller ZIP"),
    "location_state": ("State", "Pickup Location"),
    "listing_id": ("OVE Listing ID", "Listing ID"),
    "last_seen_active": ("Last Updated", "List Date", "Ends At", "Starts At"),
    "features_raw": ("Features", "Options", "Notes"),
    "transmission": ("Transmission",),
    "transmission_type": ("Transmission Type",),
    "exterior_color": ("Exterior Color", "Color"),
    "interior_color": ("Interior Color",),
    "inventory": ("Inventory",),
    "auction_house": ("Auction House",),
    "pickup_location": ("Pickup Location",),
    "status": ("Status",),
}


@dataclass(slots=True)
class TransformResult:
    vehicles: list[VehiclePayload] = field(default_factory=list)
    duplicates_removed: int = 0
    skipped_no_vin: int = 0
    errors: list[str] = field(default_factory=list)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        lines = handle.readlines()
        header_index = 0
        for index, line in enumerate(lines):
            columns = [part.strip().strip('"').lower() for part in line.split(",")]
            if "vin" in columns:
                header_index = index
                break
        reader = csv.DictReader(lines[header_index:])
        return [dict(row) for row in reader]


def transform_exports(east_path: Path, west_path: Path, source_platform: str) -> tuple[TransformResult, int, int]:
    east_rows = load_csv_rows(east_path)
    west_rows = load_csv_rows(west_path)
    merged = transform_rows(east_rows + west_rows, source_platform=source_platform)
    return merged, len(east_rows), len(west_rows)


def transform_rows(rows: list[dict[str, str]], source_platform: str = "manheim") -> TransformResult:
    result = TransformResult()
    deduped: dict[str, VehiclePayload] = {}

    for index, row in enumerate(rows, start=1):
        cleaned = redact_row(row)
        vin = (get_value(cleaned, "vin") or "").strip().upper()
        if not vin:
            result.skipped_no_vin += 1
            result.errors.append(f"row {index}: missing VIN")
            continue

        try:
            vehicle = map_row_to_vehicle(cleaned, source_platform=source_platform)
        except ValueError as exc:
            result.errors.append(f"row {index} ({vin}): {exc}")
            continue

        existing = deduped.get(vehicle.vin)
        if existing is None or is_newer(vehicle, existing):
            if existing is not None:
                result.duplicates_removed += 1
            deduped[vehicle.vin] = vehicle
        else:
            result.duplicates_removed += 1

    result.vehicles = list(deduped.values())
    return result


def redact_row(row: dict[str, str]) -> dict[str, str]:
    return {
        key: value
        for key, value in row.items()
        if key and key.strip().lower() not in REDACTED_COLUMNS
    }


def map_row_to_vehicle(row: dict[str, str], source_platform: str) -> VehiclePayload:
    vin = require_text(row, "vin").upper()
    year = require_int(row, "year")
    make = require_text(row, "make")
    model = require_text(row, "model")
    price_asking = require_float(row, "price_asking")
    listing_id = get_value(row, "listing_id")
    timestamp = parse_datetime(get_value(row, "last_seen_active"))
    features_raw = split_features(get_value(row, "features_raw"))

    features_normalized = {
        "transmission": get_value(row, "transmission") or get_value(row, "transmission_type"),
        "exterior_color": get_value(row, "exterior_color"),
        "interior_color": get_value(row, "interior_color"),
        "inventory": get_value(row, "inventory"),
        "auction_house": get_value(row, "auction_house"),
        "pickup_location": get_value(row, "pickup_location"),
        "status": get_value(row, "status"),
    }
    features_normalized = {key: value for key, value in features_normalized.items() if value}

    location_state = parse_state(get_value(row, "location_state"))
    auction_house = get_value(row, "auction_house")
    pickup_location = get_value(row, "pickup_location")
    location_zip = get_value(row, "location_zip") or resolve_location_zip(
        pickup_location=pickup_location,
        auction_house=auction_house,
        state=location_state,
    )
    available = (get_value(row, "status") or "").strip().lower() == "live"

    source_url = None
    if listing_id:
        source_url = f"https://www.ove.com/vehicle/{listing_id}"

    return VehiclePayload(
        vin=vin,
        year=year,
        make=make,
        model=model,
        trim=get_value(row, "trim"),
        body_type=get_value(row, "body_type"),
        sub_body_type=get_value(row, "sub_body_type"),
        engine_type=get_value(row, "engine_type"),
        cylinders=parse_int(get_value(row, "cylinders")),
        forced_induction=get_value(row, "forced_induction"),
        drivetrain=get_value(row, "drivetrain"),
        mpg_combined=parse_float(get_value(row, "mpg_combined")),
        ev_range=parse_int(get_value(row, "ev_range")),
        towing_capacity_lbs=parse_int(get_value(row, "towing_capacity_lbs")),
        odometer=parse_int(get_value(row, "odometer")),
        condition_grade=get_value(row, "condition_grade"),
        price_asking=price_asking,
        price_wholesale_est=parse_float(get_value(row, "price_wholesale_est")),
        location_zip=location_zip,
        location_state=location_state,
        listing_id=listing_id,
        source_platform=source_platform,
        source_url=source_url,
        features_raw=features_raw,
        features_normalized=features_normalized,
        available=available,
        ove_listing_timestamp=timestamp,
        last_seen_active=timestamp,
    )


def is_newer(candidate: VehiclePayload, current: VehiclePayload) -> bool:
    candidate_dt = candidate.last_seen_active or datetime.min.replace(tzinfo=timezone.utc)
    current_dt = current.last_seen_active or datetime.min.replace(tzinfo=timezone.utc)
    return candidate_dt >= current_dt


def get_value(row: dict[str, str], field_name: str) -> str | None:
    for alias in COLUMN_ALIASES.get(field_name, ()):
        raw = row.get(alias)
        if raw is not None and raw.strip():
            return raw.strip()
    return None


def require_text(row: dict[str, str], field_name: str) -> str:
    value = get_value(row, field_name)
    if not value:
        raise ValueError(f"missing required field {field_name}")
    return value.strip()


def require_int(row: dict[str, str], field_name: str) -> int:
    value = parse_int(get_value(row, field_name))
    if value is None:
        raise ValueError(f"missing required integer field {field_name}")
    return value


def require_float(row: dict[str, str], field_name: str) -> float:
    value = parse_float(get_value(row, field_name))
    if value is None:
        raise ValueError(f"missing required numeric field {field_name}")
    return value


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return int(digits) if digits else None


def parse_float(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = "".join(ch for ch in value if ch.isdigit() or ch in {".", "-"})
    return float(cleaned) if cleaned not in {"", ".", "-"} else None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    candidates = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"unsupported datetime format: {value}")


def split_features(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [part.strip() for part in value.replace("|", ",").replace(";", ",").split(",")]
    return [part for part in parts if part]


def parse_state(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if " - " in text:
        return text.split(" - ", 1)[0].strip().upper() or None
    if len(text) == 2:
        return text.upper()
    return None

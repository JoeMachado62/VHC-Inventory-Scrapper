from __future__ import annotations

import logging
import os
import re
import json
from functools import lru_cache
from pathlib import Path

import pgeocode

logger = logging.getLogger(__name__)

OVERRIDE_DB_PATH = Path("data") / "auction_location_overrides.json"

AUCTION_CITY_ALIASES: dict[str, str] = {
    "manheim albany": "Albany",
    "manheim atlanta": "Atlanta",
    "manheim baltimore-washington": "Elkridge",
    "manheim birmingham": "Birmingham",
    "manheim california": "Dixon",
    "manheim central florida": "Orlando",
    "manheim chicago": "Matteson",
    "manheim cincinnati": "Cincinnati",
    "manheim cleveland": "North Canton",
    "manheim darlington": "Darlington",
    "manheim dallas": "Dallas",
    "manheim dallas-fort worth": "Dallas",
    "manheim denver": "Denver",
    "manheim detroit": "Detroit",
    "manheim flint": "Flint",
    "manheim fort lauderdale": "Fort Lauderdale",
    "manheim fort myers": "Fort Myers",
    "manheim fredericksburg": "Fredericksburg",
    "manheim georgia": "Atlanta",
    "manheim harrisonburg": "Harrisonburg",
    "manheim houston": "Houston",
    "manheim indianapolis": "Indianapolis",
    "manheim jacksonville": "Jacksonville",
    "manheim kansas city": "Kansas City",
    "manheim lakeland": "Lakeland",
    "manheim louisville": "Louisville",
    "manheim milwaukee": "Milwaukee",
    "manheim minneapolis": "Minneapolis",
    "manheim nashville": "Nashville",
    "manheim nevada": "Las Vegas",
    "manheim new england": "North Haven",
    "manheim new jersey": "Pennsauken",
    "manheim new mexico": "Albuquerque",
    "manheim new orleans": "New Orleans",
    "manheim new york": "New York",
    "manheim north carolina": "Charlotte",
    "manheim northstar minnesota": "Shakopee",
    "manheim ny metro skyline": "Brooklyn",
    "manheim oceanside": "Oceanside",
    "manheim ohio": "Columbus",
    "manheim omaha": "Omaha",
    "manheim orlando": "Orlando",
    "manheim palm beach": "Delray Beach",
    "manheim pennsylvania": "Manheim",
    "manheim pensacola": "Pensacola",
    "manheim philadelphia": "Hatfield",
    "manheim phoenix": "Phoenix",
    "manheim pittsburgh": "Pittsburgh",
    "manheim portland": "Portland",
    "manheim riverside": "Riverside",
    "manheim rochester": "Rochester",
    "manheim san antonio": "San Antonio",
    "manheim san francisco bay": "Hayward",
    "manheim seattle": "Seattle",
    "manheim southern california": "Fontana",
    "manheim st louis": "St Louis",
    "manheim st pete": "St Petersburg",
    "manheim statesville": "Statesville",
    "manheim tallahassee": "Tallahassee",
    "manheim tampa": "Tampa",
    "manheim texas hobby": "Houston",
    "manheim tulsa": "Tulsa",
    "manheim utah": "Draper",
    "manheim wilmington": "Wilmington",
    "mycentralauction": "Charlotte",
    "rome auto auction powered by manheim": "Marietta",
}

PICKUP_CITY_ALIASES: dict[str, str] = {
    "fenton": "Fenton",
    "waukesha": "Waukesha",
    "mount pleasant": "Mount Pleasant",
    "racine": "Racine",
    "pelham": "Pelham",
    "springfield": "Springfield",
    "murfreesboro": "Murfreesboro",
    "brooklyn": "Brooklyn",
    "troy": "Troy",
    "garland": "Garland",
    "palos hills": "Palos Hills",
    "cuyahoga falls": "Cuyahoga Falls",
    "delray beach": "Delray Beach",
    "draper": "Draper",
    "marietta": "Marietta",
    "hatfield": "Hatfield",
    "winston salem": "Winston-Salem",
    "east windsor": "East Windsor",
    "north haven": "North Haven",
    "bellingham": "Bellingham",
}


def resolve_location_zip(pickup_location: str | None, auction_house: str | None, state: str | None) -> str | None:
    state_code = normalize_state(state or pickup_location)
    if not state_code:
        return None

    override = lookup_override(pickup_location, auction_house, state_code)
    if override:
        return override

    city = normalize_city_from_pickup(pickup_location)
    if city and not city.lower().startswith("manheim "):
        zip_code = query_zip(city, state_code)
        if zip_code:
            return zip_code

    auction_city = normalize_city_from_auction(auction_house)
    if auction_city:
        zip_code = query_zip(auction_city, state_code)
        if zip_code:
            return zip_code

    if city:
        fallback_city = city.removeprefix("Manheim ").strip()
        zip_code = query_zip(fallback_city, state_code)
        if zip_code:
            return zip_code

    ai_zip = _try_ai_resolver(pickup_location, auction_house, state_code)
    if ai_zip:
        return ai_zip

    return _state_centroid_fallback(pickup_location, auction_house, state_code)


def _try_ai_resolver(pickup_location: str | None, auction_house: str | None, state_code: str) -> str | None:
    """Invoke the OpenAI web_search ZIP lookup and persist successful hits.

    Gated by OVE_AI_ZIP_RESOLVER_ENABLED (default true). Disabled
    automatically when OPENAI_API_KEY is missing. Failures are logged but
    never raised — the caller falls through to the state-centroid fallback.
    """
    if os.getenv("OVE_AI_ZIP_RESOLVER_ENABLED", "true").lower() in {"0", "false", "no", "off"}:
        return None
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    model = os.getenv("OPENAI_MODEL", "gpt-5.4").strip() or "gpt-5.4"

    from ove_scraper.ai_zip_resolver import resolve_zip_via_ai, append_override

    try:
        zip_code = resolve_zip_via_ai(
            pickup_location=pickup_location,
            auction_house=auction_house,
            state=state_code,
            api_key=api_key,
            model=model,
        )
    except Exception as exc:
        logger.warning(
            "AI ZIP resolver crashed for auction=%r state=%s: %s",
            auction_house, state_code, exc,
        )
        return None

    if not zip_code:
        return None

    try:
        append_override(pickup_location, auction_house, state_code, zip_code, overrides_path=OVERRIDE_DB_PATH)
        load_override_db.cache_clear()
    except Exception as exc:
        logger.warning(
            "Failed to persist AI-resolved ZIP for auction=%r state=%s: %s",
            auction_house, state_code, exc,
        )
    return zip_code


def _state_centroid_fallback(pickup_location: str | None, auction_house: str | None, state_code: str) -> str | None:
    from ove_scraper.state_centroid_zips import state_centroid_zip

    centroid = state_centroid_zip(state_code)
    if centroid:
        logger.warning(
            "Using state-centroid ZIP fallback %s for auction=%r pickup=%r state=%s",
            centroid, auction_house, pickup_location, state_code,
        )
    return centroid


def normalize_state(value: str | None) -> str | None:
    if not value:
        return None
    match = re.match(r"\s*([A-Za-z]{2})\b", value.strip())
    return match.group(1).upper() if match else None


def normalize_city_from_pickup(value: str | None) -> str | None:
    if not value:
        return None
    city = value.split(" - ", 1)[1].strip() if " - " in value else value.strip()
    alias = PICKUP_CITY_ALIASES.get(city.lower())
    return alias or title_case_city(city)


def normalize_city_from_auction(value: str | None) -> str | None:
    if not value:
        return None
    alias = AUCTION_CITY_ALIASES.get(value.strip().lower())
    return alias or title_case_city(value.replace("Manheim", "").strip())


def title_case_city(value: str) -> str:
    words = re.split(r"(\s+|-)", value.strip())
    return "".join(part.capitalize() if part.strip() and part not in {"-", " "} else part for part in words)


@lru_cache(maxsize=1)
def _nominatim() -> pgeocode.Nominatim:
    cache_dir = Path(".cache") / "pgeocode"
    cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("PGEOCODE_DATA_DIR", str(cache_dir))
    return pgeocode.Nominatim("us")


@lru_cache(maxsize=512)
def query_zip(city: str, state_code: str) -> str | None:
    result = _nominatim().query_location(city, top_k=25)
    if result is None:
        return None

    try:
        rows = result.dropna(subset=["postal_code"])
    except AttributeError:
        postal_code = getattr(result, "postal_code", None)
        return normalize_zip(postal_code)

    if "state_code" in rows.columns:
        state_rows = rows[rows["state_code"].astype(str).str.upper() == state_code.upper()]
        if not state_rows.empty:
            rows = state_rows

    if rows.empty:
        return None

    postal_code = rows.iloc[0]["postal_code"]
    return normalize_zip(postal_code)


def normalize_zip(value: object) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    return None


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


@lru_cache(maxsize=1)
def load_override_db() -> dict[str, dict[str, str]]:
    if not OVERRIDE_DB_PATH.exists():
        return {}
    payload = json.loads(OVERRIDE_DB_PATH.read_text(encoding="utf-8"))
    entries = payload.get("overrides", [])
    db: dict[str, dict[str, str]] = {}
    for entry in entries:
        key = build_override_key(
            entry.get("pickup_location"),
            entry.get("auction_house"),
            entry.get("state"),
        )
        db[key] = entry
    return db


def lookup_override(pickup_location: str | None, auction_house: str | None, state: str | None) -> str | None:
    db = load_override_db()
    keys = [
        build_override_key(pickup_location, auction_house, state),
        build_override_key(pickup_location, None, state),
        build_override_key(None, auction_house, state),
    ]
    for key in keys:
        entry = db.get(key)
        if entry:
            return normalize_zip(entry.get("zip"))
    return None


def build_override_key(pickup_location: str | None, auction_house: str | None, state: str | None) -> str:
    return "|".join(
        [
            normalize_key(pickup_location),
            normalize_key(auction_house),
            normalize_state(state) or "",
        ]
    )

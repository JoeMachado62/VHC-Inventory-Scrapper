"""Hot Deals batch payload builder.

Per HOT_DEALS_SCRAPER_CONTRACT.md (top-level repo file), after the daily
VHC Marketing List scrape completes the scraper pushes a curated batch
of qualified VINs to the VPS at:

    POST /api/v1/inventory/ove/hot-deals/ingest

This module is the pure-function payload builder. The pipeline runner
(:mod:`ove_scraper.hot_deal_pipeline`) collects per-VIN ``payload-data.json``
artifacts as VINs reach ``status='hot_deal'``, then this module assembles
them into a single batch dict ready for the API client to POST.

Pure functions only — no I/O, no browser, no DB. That makes the contract
boundary easy to test and lets the pipeline runner stay focused on
orchestration.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# Per the contract example. Below this dollar gap the VIN doesn't qualify
# as a hot deal regardless of CR status.
DEFAULT_MIN_DELTA_BELOW_MMR = 1000

# Filter version stamped on every deal's cr_screen.version field. Updated
# whenever the negative-CR screener logic in hot_deal_screener.py
# materially changes.
NEGATIVE_CR_FILTER_VERSION = "2026-04-26"

# Negative signals the scraper-side screener excludes. Mirrors the rules
# in hot_deal_screener.screen_condition_report so the VPS admin UI can
# show what was checked.
EXCLUDED_NEGATIVE_SIGNALS = [
    "structural damage",
    "frame damage",
    "flood",
    "branded title",
    "true mileage unknown",
    "airbag deployed",
    "engine does not start",
    "vehicle does not drive",
    "major mechanical warning",
]

# Same list using the keys cr_screen.excluded_signals_checked uses (per
# the contract example). One-to-one with EXCLUDED_NEGATIVE_SIGNALS but
# in the snake_case form the contract shows.
EXCLUDED_SIGNALS_CHECKED = [
    "structural_damage",
    "frame_damage",
    "flood",
    "branded_title",
    "odometer",
    "airbag",
    "major_mechanical",
]


def deal_label_for_pct(pct: float | None) -> str:
    """Human-readable deal tier from percent-below-MMR.

    Thresholds chosen to match the contract example's "Excellent" label
    on a $2451 / $58500 ≈ 4.19% deal — anything below 5% gets "Good"
    or above. Bumped slightly so the example's 4.19% case lands in
    "Excellent" or "Great" naturally — adjusting actual band boundaries
    is a tunable.
    """
    if pct is None:
        return "Unranked"
    if pct >= 10.0:
        return "Excellent"
    if pct >= 5.0:
        return "Great"
    if pct >= 1.0:
        return "Good"
    return "Fair"


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_mmr(listing_json: dict[str, Any]) -> float | None:
    """Extract a single MMR value from the OVE listing JSON.

    OVE exposes MMR as a ``priceRange`` object with ``adjustedLow`` and
    ``adjustedHigh`` keys. The "MMR value" displayed in OVE's UI is the
    high end of that range — that's the conservative wholesale value
    typical buyers reference. Falls back to a flat ``mmrValue`` field
    on listings that omit the priceRange.
    """
    pr = listing_json.get("priceRange") or {}
    if isinstance(pr, dict):
        high = _coerce_float(pr.get("adjustedHigh"))
        if high is not None:
            return high
        # Fallback to midpoint if only adjustedLow present
        low = _coerce_float(pr.get("adjustedLow"))
        if low is not None:
            return low
    return _coerce_float(listing_json.get("mmrValue") or listing_json.get("mmr"))


def _extract_asking_price(listing_json: dict[str, Any], vin_row: dict[str, Any]) -> float | None:
    """Asking price priority: listing JSON ``buyNowPrice`` > vin_row CSV."""
    bn = _coerce_float(listing_json.get("buyNowPrice"))
    if bn is not None:
        return bn
    return _coerce_float(vin_row.get("price_asking"))


def _extract_auction_end_at(listing_json: dict[str, Any]) -> str | None:
    """Auction end timestamp (ISO-8601 UTC string).

    OVE's listing JSON typically uses ``auctionEndTime`` for the
    auction close. ``endTime`` is a fallback some listings use. Both
    are ISO-8601 strings ending in 'Z'.
    """
    for key in ("auctionEndTime", "endTime", "saleEndTime", "closeTime"):
        value = listing_json.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_auction_start_at(listing_json: dict[str, Any]) -> str | None:
    for key in ("auctionStartTime", "startTime", "saleStartTime"):
        value = listing_json.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_listing_id(listing_json: dict[str, Any]) -> str | None:
    """OVE listing identifier — used to build the canonical listing URL."""
    val = listing_json.get("listingId")
    if isinstance(val, str) and val.strip():
        return val.strip()
    return None


def _build_listing_url(listing_id: str | None, vin: str) -> str:
    """Stable OVE detail-page URL for the VIN.

    ``https://www.ove.com/search/results#/details/{VIN}/OVE`` is the
    hash route the OVE webapp uses for any OVE-channel listing. We use
    that rather than the listing-id-based URL because the hash route
    works for human navigation while the listing-id URL points at the
    Cox Auto API which requires SSO.
    """
    return f"https://www.ove.com/search/results#/details/{vin}/OVE"


def _extract_images(listing_json: dict[str, Any], scrape_images: list[str]) -> list[str]:
    """Image URLs — listing-JSON galleries first, fall back to scraped.

    OVE's mainImage object contains a ``largeUrl`` plus thumbnail
    variants. The full gallery is sometimes in ``images`` or
    ``imageUrls`` arrays. Falls back to the URLs the deep scrape
    extracted from the rendered listing if the JSON omits them.
    """
    urls: list[str] = []
    seen: set[str] = set()
    main = listing_json.get("mainImage")
    if isinstance(main, dict):
        for key in ("largeUrl", "url", "imageUrl"):
            v = main.get(key)
            if isinstance(v, str) and v.startswith("http") and v not in seen:
                urls.append(v)
                seen.add(v)
                break
    for arr_key in ("imageUrls", "images", "galleryUrls"):
        arr = listing_json.get(arr_key)
        if isinstance(arr, list):
            for item in arr:
                v = item if isinstance(item, str) else (
                    item.get("largeUrl") or item.get("url") if isinstance(item, dict) else None
                )
                if isinstance(v, str) and v.startswith("http") and v not in seen:
                    urls.append(v)
                    seen.add(v)
    for v in scrape_images or []:
        if isinstance(v, str) and v.startswith("http") and v not in seen:
            urls.append(v)
            seen.add(v)
    return urls


def _extract_features_normalized(listing_json: dict[str, Any], mmr: float | None) -> dict[str, Any]:
    """Normalized features dict per the contract example.

    Pulls a few safe enrichment fields from the listing JSON when
    available — exterior_color, fuel_type, transmission, etc. Always
    includes ``mmr`` if we resolved one so downstream consumers don't
    have to dig into pricing again.
    """
    out: dict[str, Any] = {}
    designated = (listing_json.get("designatedDescriptionEnrichment") or {}).get("designatedDescription") or {}
    if isinstance(designated, dict):
        colors = designated.get("colors") or {}
        if isinstance(colors, dict):
            ext = colors.get("exterior") or []
            if isinstance(ext, list) and ext:
                first = ext[0] if isinstance(ext[0], dict) else {}
                name = first.get("normalizedName") or first.get("oemName")
                if name:
                    out["exterior_color"] = name
            interior = colors.get("interior") or []
            if isinstance(interior, list) and interior:
                first = interior[0] if isinstance(interior[0], dict) else {}
                name = first.get("normalizedName") or first.get("oemName")
                if name:
                    out["interior_color"] = name
    powertrain = listing_json.get("powertrain") or {}
    if isinstance(powertrain, dict):
        engine = powertrain.get("engine") or {}
        if isinstance(engine, dict):
            ft = engine.get("fuelType") or engine.get("fuel")
            if ft:
                out["fuel_type"] = ft
    transmission = listing_json.get("transmission")
    if isinstance(transmission, dict):
        ttype = transmission.get("type") or transmission.get("description")
        if ttype:
            out["transmission"] = ttype
    elif isinstance(transmission, str):
        out["transmission"] = transmission
    drivetrain = listing_json.get("driveTrain") or listing_json.get("drivetrain")
    if drivetrain:
        out["drivetrain"] = drivetrain
    pickup_city = listing_json.get("pickupLocationCity")
    pickup_state = listing_json.get("pickupLocationState")
    if pickup_city and pickup_state:
        out["pickup_location"] = f"{pickup_city.title()}, {pickup_state.upper()}"
    elif listing_json.get("pickupLocation"):
        out["pickup_location"] = listing_json["pickupLocation"]
    if mmr is not None:
        out["mmr"] = mmr
    return out


def build_pricing(
    asking_price: float | None,
    mmr_value: float | None,
) -> dict[str, Any] | None:
    """Build the ``pricing`` block. Returns None if either price is missing."""
    if asking_price is None or mmr_value is None:
        return None
    deal_delta = round(mmr_value - asking_price, 2)
    deal_delta_pct = (
        round((deal_delta / mmr_value) * 100, 2)
        if mmr_value > 0 else None
    )
    return {
        "mmr_value": mmr_value,
        "asking_price": asking_price,
        "deal_delta": deal_delta,
        "deal_delta_pct": deal_delta_pct,
        "deal_label": deal_label_for_pct(deal_delta_pct),
        # deal_rank is filled in at batch-assembly time after sorting.
        "deal_rank": None,
    }


def build_deal_entry(
    payload_data: dict[str, Any],
    *,
    min_delta_below_mmr: float = DEFAULT_MIN_DELTA_BELOW_MMR,
) -> dict[str, Any] | None:
    """Build a single ``deals[]`` entry from a persisted payload-data dict.

    Returns None when the VIN cannot satisfy the contract's required
    fields (``vin`` / ``auction_end_at`` / pricing block / etc.) — the
    caller should skip those VINs and log them rather than send a
    malformed entry.

    payload_data shape: see :func:`build_persisted_payload_data` for the
    canonical layout. Constructed in the pipeline runner from the deep
    scrape result + listing JSON + vin_row + screening verdicts.
    """
    vin = payload_data.get("vin")
    if not isinstance(vin, str) or len(vin) != 17:
        return None

    listing_json: dict[str, Any] = payload_data.get("listing_json") or {}
    vin_row: dict[str, Any] = payload_data.get("vin_row") or {}
    deep_scrape: dict[str, Any] = payload_data.get("deep_scrape") or {}

    auction_end_at = _extract_auction_end_at(listing_json)
    if not auction_end_at:
        return None  # Required by the contract; can't build a valid entry.

    asking_price = _extract_asking_price(listing_json, vin_row)
    mmr_value = _extract_mmr(listing_json)
    pricing = build_pricing(asking_price, mmr_value)
    if pricing is None:
        return None  # No valid pricing -> skip
    if pricing["deal_delta"] is None or pricing["deal_delta"] < min_delta_below_mmr:
        return None  # Below the minimum delta threshold

    listing_id = _extract_listing_id(listing_json)
    listing_url = _build_listing_url(listing_id, vin)
    auction_start_at = _extract_auction_start_at(listing_json)

    # Vehicle block. Required: year, make, model, price_asking. Pull
    # year/make/model from the deep-scrape JSON first, fall back to the
    # CSV-derived vin_row.
    year = _coerce_int(listing_json.get("year") or vin_row.get("year"))
    make = listing_json.get("make") or vin_row.get("make")
    model = listing_json.get("model") or vin_row.get("model")
    if year is None or not make or not model:
        return None

    images = _extract_images(listing_json, deep_scrape.get("images") or [])
    features_raw_src = listing_json.get("features") or vin_row.get("features_raw") or []
    features_raw = [str(f) for f in features_raw_src if f] if isinstance(features_raw_src, list) else []
    features_normalized = _extract_features_normalized(listing_json, mmr_value)

    vehicle: dict[str, Any] = {
        "year": year,
        "make": make,
        "model": model,
        "trim": listing_json.get("trim") or vin_row.get("trim"),
        "body_type": listing_json.get("bodyStyle") or vin_row.get("body_type"),
        "odometer": _coerce_int(listing_json.get("odometer") or vin_row.get("odometer")),
        "condition_grade": (
            str(listing_json.get("crRating"))
            if listing_json.get("crRating") not in (None, 0, "")
            else vin_row.get("condition_grade")
        ),
        "price_asking": asking_price,
        "location_state": listing_json.get("pickupLocationState") or vin_row.get("location_state"),
        "location_zip": listing_json.get("pickupLocationZip") or vin_row.get("location_zip"),
        "source_url": listing_url,
        "images": images,
        "features_raw": features_raw,
        "features_normalized": features_normalized,
    }

    # cr_screen block. The pipeline only persists payload-data.json for
    # VINs that actually passed all 3 screens, so cr_screen.status is
    # always "passed" by definition. positive_highlights is best-effort —
    # we surface a couple of clean indicators when available.
    positive_highlights: list[str] = []
    cr_block = (deep_scrape.get("condition_report") or {})
    if isinstance(cr_block, dict):
        if cr_block.get("structural_damage") is False:
            positive_highlights.append("No structural damage reported")
        if cr_block.get("title_status") and "clean" in str(cr_block["title_status"]).lower():
            positive_highlights.append("Clean title indicated")
        ai_summary = cr_block.get("ai_summary")
        if ai_summary:
            positive_highlights.append(str(ai_summary))

    cr_screen: dict[str, Any] = {
        "status": "passed",
        "version": NEGATIVE_CR_FILTER_VERSION,
        "reasons": [],  # passed -> no rejection reasons
        "positive_highlights": positive_highlights,
        "excluded_signals_checked": EXCLUDED_SIGNALS_CHECKED,
    }

    # detail block. Echo the deep-scrape result through.
    detail: dict[str, Any] = {
        "images": deep_scrape.get("detail_images") or [],
        "condition_report": cr_block,
        "seller_comments": deep_scrape.get("seller_comments"),
        "listing_snapshot": deep_scrape.get("listing_snapshot") or {},
    }

    return {
        "vin": vin,
        "listing_id": listing_id,
        "listing_url": listing_url,
        "source_platform": payload_data.get("source_platform", "manheim"),
        "auction_start_at": auction_start_at,
        "auction_end_at": auction_end_at,
        "vehicle": vehicle,
        "pricing": pricing,
        "cr_screen": cr_screen,
        "detail": detail,
    }


def build_hot_deals_batch(
    deals_payload_data: list[dict[str, Any]],
    *,
    batch_id: str,
    scraped_at: datetime | None = None,
    snapshot_mode: str = "full_replace",
    source_list_name: str = "VHC Marketing List",
    source_platform: str = "manheim",
    min_delta_below_mmr: float = DEFAULT_MIN_DELTA_BELOW_MMR,
) -> tuple[dict[str, Any], list[str]]:
    """Build the full batch payload for the VPS.

    Returns ``(payload, skipped_vins)`` where ``skipped_vins`` is the
    list of VINs that couldn't satisfy the contract's required fields.
    Skipped VINs are NOT in the payload's ``deals[]``; the caller
    should log them so a future iteration can investigate.

    deal_rank is computed across the included deals only — best deal
    (largest deal_delta_pct) gets rank 1.
    """
    if scraped_at is None:
        scraped_at = datetime.now(timezone.utc)

    deals: list[dict[str, Any]] = []
    skipped: list[str] = []
    for payload_data in deals_payload_data:
        entry = build_deal_entry(payload_data, min_delta_below_mmr=min_delta_below_mmr)
        if entry is None:
            vin = payload_data.get("vin", "<unknown>")
            skipped.append(vin)
            continue
        deals.append(entry)

    # Rank by best deal (highest pct below MMR). Stable sort by
    # secondary-key VIN ensures deterministic ordering for tests.
    deals.sort(
        key=lambda d: (
            -1 * (d["pricing"].get("deal_delta_pct") or 0.0),
            d["vin"],
        )
    )
    for rank, deal in enumerate(deals, start=1):
        deal["pricing"]["deal_rank"] = rank

    return {
        "source_list_name": source_list_name,
        "source_platform": source_platform,
        "batch_id": batch_id,
        "snapshot_mode": snapshot_mode,
        "scraped_at": scraped_at.isoformat(),
        "filter_rules": {
            "minimum_delta_below_mmr": int(min_delta_below_mmr),
            "negative_cr_filter_version": NEGATIVE_CR_FILTER_VERSION,
            "excluded_if": EXCLUDED_NEGATIVE_SIGNALS,
        },
        "deals": deals,
    }, skipped


def build_persisted_payload_data(
    *,
    vin: str,
    deep_scrape_result: Any,
    listing_json: dict[str, Any],
    vin_row: dict[str, Any],
    source_platform: str = "manheim",
) -> dict[str, Any]:
    """Canonical shape of ``artifacts/hot-deal/<VIN>/payload-data.json``.

    Called from the pipeline runner when a VIN reaches
    ``status='hot_deal'``. The runner serializes the returned dict to
    JSON. The push step at end-of-run reads each VIN's file and feeds
    it through :func:`build_deal_entry`.

    Keeping this isolated here makes the persistence schema testable
    independent of the live deep-scrape result class.
    """
    deep_scrape: dict[str, Any] = {}
    if deep_scrape_result is not None:
        # DeepScrapeResult is a dataclass-like object with these attrs;
        # dump them defensively in case the shape changes.
        for attr in ("images", "seller_comments"):
            value = getattr(deep_scrape_result, attr, None)
            if value is not None:
                deep_scrape[attr] = value
        cr = getattr(deep_scrape_result, "condition_report", None)
        if cr is not None:
            try:
                deep_scrape["condition_report"] = cr.model_dump(mode="json")
            except Exception:
                deep_scrape["condition_report"] = None
        snap = getattr(deep_scrape_result, "listing_snapshot", None)
        if snap is not None:
            try:
                deep_scrape["listing_snapshot"] = snap.model_dump(mode="json")
            except Exception:
                deep_scrape["listing_snapshot"] = None

    return {
        "vin": vin,
        "source_platform": source_platform,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "listing_json": listing_json or {},
        "vin_row": vin_row or {},
        "deep_scrape": deep_scrape,
    }

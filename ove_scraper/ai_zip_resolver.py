"""AI-powered last-mile ZIP resolver for unknown auction houses.

Falls back to OpenAI Responses API with the built-in `web_search` tool when
the alias-driven pgeocode chain in `location_zip_lookup.resolve_location_zip`
can't find a ZIP. Successful results are written into the override JSON so
the same auction is never paid for twice.

Uses the project's existing OpenAI plumbing (settings.openai_api_key,
settings.openai_model) — same model + API surface as
`openai_web_search.search_vin_salvage_history`.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pgeocode

logger = logging.getLogger(__name__)

_RESPONSES_URL = "https://api.openai.com/v1/responses"
_DEFAULT_BUDGET = 50
_BUDGET_ENV = "OVE_AI_ZIP_RESOLVER_BUDGET"
_TELEMETRY_PATH = Path("artifacts") / "zip_resolution.log"

_PROCESS_BUDGET_REMAINING: int | None = None
_PROCESS_CACHE: dict[tuple[str, str, str], str | None] = {}


def reset_process_state() -> None:
    """Test hook: clear in-process budget + memoization."""
    global _PROCESS_BUDGET_REMAINING
    _PROCESS_BUDGET_REMAINING = None
    _PROCESS_CACHE.clear()


def _budget_remaining() -> int:
    global _PROCESS_BUDGET_REMAINING
    if _PROCESS_BUDGET_REMAINING is None:
        try:
            _PROCESS_BUDGET_REMAINING = int(os.getenv(_BUDGET_ENV, str(_DEFAULT_BUDGET)))
        except ValueError:
            _PROCESS_BUDGET_REMAINING = _DEFAULT_BUDGET
    return _PROCESS_BUDGET_REMAINING


def _decrement_budget() -> None:
    global _PROCESS_BUDGET_REMAINING
    if _PROCESS_BUDGET_REMAINING is not None:
        _PROCESS_BUDGET_REMAINING -= 1


def _log_event(event: dict[str, Any]) -> None:
    event = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    try:
        _TELEMETRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _TELEMETRY_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event) + "\n")
    except OSError as exc:
        logger.warning("zip_resolution.log write failed: %s", exc)


_PROMPT_TEMPLATE = (
    "Find the 5-digit US ZIP code for the physical street address of this "
    "auto auction location.\n\n"
    "Auction house name: {auction_house}\n"
    "Pickup location label (from listing): {pickup_location}\n"
    "Known US state: {state}\n\n"
    "Use web search to look up the auction's official website, Google Maps, "
    "or other authoritative sources for the actual street address. The ZIP "
    "MUST correspond to a real address in {state}. If multiple branches "
    "exist, pick the one matching the pickup-location label.\n\n"
    "Return ONLY a JSON object on a single line, no prose, no markdown:\n"
    '{{"zip": "12345", "city": "City Name", "source_url": "https://..."}}\n\n'
    "If you genuinely cannot find a ZIP for an auction in {state}, return:\n"
    '{{"zip": null, "city": null, "source_url": null}}'
)


def resolve_zip_via_ai(
    pickup_location: str | None,
    auction_house: str | None,
    state: str | None,
    *,
    api_key: str,
    model: str,
    timeout: float = 60.0,
) -> str | None:
    """Web-search the auction location and return a state-validated 5-digit ZIP.

    Returns None if budget exhausted, API call fails, no ZIP returned, or
    the returned ZIP doesn't pgeocode-resolve to the requested state.
    Caller is responsible for persisting successful hits to the override JSON.
    """
    if not api_key or not state or not (pickup_location or auction_house):
        return None

    cache_key = ((pickup_location or "").strip(), (auction_house or "").strip(), state.upper())
    if cache_key in _PROCESS_CACHE:
        return _PROCESS_CACHE[cache_key]

    if _budget_remaining() <= 0:
        _log_event({
            "event": "budget_exhausted",
            "pickup_location": pickup_location,
            "auction_house": auction_house,
            "state": state,
        })
        _PROCESS_CACHE[cache_key] = None
        return None

    prompt = _PROMPT_TEMPLATE.format(
        auction_house=auction_house or "(unknown)",
        pickup_location=pickup_location or "(unknown)",
        state=state.upper(),
    )

    payload = {
        "model": model,
        "tools": [{"type": "web_search"}],
        "input": prompt,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    _decrement_budget()
    logger.info(
        "AI ZIP lookup auction=%r state=%s model=%s",
        auction_house, state, model,
    )

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(_RESPONSES_URL, json=payload, headers=headers)
            resp.raise_for_status()
        raw_text = _extract_output_text(resp.json())
    except (httpx.HTTPError, ValueError) as exc:
        _log_event({
            "event": "api_error",
            "pickup_location": pickup_location,
            "auction_house": auction_house,
            "state": state,
            "error": str(exc),
        })
        _PROCESS_CACHE[cache_key] = None
        return None

    candidate = _extract_zip_from_response(raw_text)
    if candidate is None:
        _log_event({
            "event": "no_zip_in_response",
            "pickup_location": pickup_location,
            "auction_house": auction_house,
            "state": state,
            "raw": raw_text[:500],
        })
        _PROCESS_CACHE[cache_key] = None
        return None

    if not _zip_matches_state(candidate, state):
        _log_event({
            "event": "validation_rejected",
            "pickup_location": pickup_location,
            "auction_house": auction_house,
            "state": state,
            "candidate_zip": candidate,
        })
        _PROCESS_CACHE[cache_key] = None
        return None

    _log_event({
        "event": "resolved",
        "pickup_location": pickup_location,
        "auction_house": auction_house,
        "state": state,
        "zip": candidate,
    })
    _PROCESS_CACHE[cache_key] = candidate
    return candidate


def _extract_output_text(response_data: dict) -> str:
    output = response_data.get("output", [])
    parts = []
    for item in output:
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    parts.append(content.get("text", ""))
    return "\n".join(parts) if parts else ""


def _extract_zip_from_response(raw_text: str) -> str | None:
    """Strict: only accept ZIPs that come back inside the JSON we asked for.

    Falling back to bare-regex on the raw text would pick up street numbers
    or area codes embedded in cited address strings. We'd rather return None
    and fall through to the state-centroid fallback than guess.
    """
    if not raw_text:
        return None
    match = re.search(r"\{[^{}]*\"zip\"[^{}]*\}", raw_text, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group())
    except (json.JSONDecodeError, TypeError):
        return None
    zip_value = parsed.get("zip")
    if not isinstance(zip_value, str):
        return None
    digits = re.sub(r"\D", "", zip_value)
    return digits[:5] if len(digits) >= 5 else None


def _zip_matches_state(zip_code: str, state: str) -> bool:
    """Validate that the ZIP pgeocodes back to the expected state."""
    try:
        nomi = _nominatim()
        result = nomi.query_postal_code(zip_code)
        actual_state = getattr(result, "state_code", None)
        if actual_state is None:
            return False
        actual_str = str(actual_state).strip().upper()
        if actual_str in {"NAN", "NONE", ""}:
            return False
        return actual_str == state.strip().upper()
    except Exception as exc:
        logger.warning("pgeocode validation failed for zip=%s state=%s: %s", zip_code, state, exc)
        return False


_NOMINATIM: pgeocode.Nominatim | None = None


def _nominatim() -> pgeocode.Nominatim:
    global _NOMINATIM
    if _NOMINATIM is None:
        cache_dir = Path(".cache") / "pgeocode"
        cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("PGEOCODE_DATA_DIR", str(cache_dir))
        _NOMINATIM = pgeocode.Nominatim("us")
    return _NOMINATIM


def append_override(
    pickup_location: str | None,
    auction_house: str | None,
    state: str | None,
    zip_code: str,
    *,
    overrides_path: Path = Path("data") / "auction_location_overrides.json",
) -> None:
    """Atomically merge a new entry into auction_location_overrides.json.

    Re-reads from disk before writing so concurrent processes don't lose
    each other's entries. After writing, the caller should clear the
    `load_override_db` lru_cache so subsequent lookups see the new entry.
    """
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    if overrides_path.exists():
        try:
            payload = json.loads(overrides_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload = {"version": 1, "overrides": []}
    else:
        payload = {"version": 1, "overrides": []}

    entries = list(payload.get("overrides", []))
    new_entry = {
        "pickup_location": pickup_location or "",
        "auction_house": auction_house or "",
        "state": (state or "").upper(),
        "zip": zip_code,
        "source": "ai_web_search",
        "added_at": datetime.now(timezone.utc).isoformat(),
    }

    def _key(entry: dict) -> tuple[str, str, str]:
        return (
            (entry.get("pickup_location") or "").strip().lower(),
            (entry.get("auction_house") or "").strip().lower(),
            (entry.get("state") or "").strip().upper(),
        )

    target_key = _key(new_entry)
    if any(_key(e) == target_key for e in entries):
        return

    entries.append(new_entry)
    payload["overrides"] = entries

    fd, tmp_path = tempfile.mkstemp(
        prefix=".auction_location_overrides_",
        suffix=".json",
        dir=str(overrides_path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp_path, overrides_path)
    except OSError:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

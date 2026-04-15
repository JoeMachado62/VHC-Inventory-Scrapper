"""OpenAI Responses API client for VIN salvage history search.

Uses GPT-5.4's built-in web_search tool to check if a VIN appears on
known salvage auction websites. Communicates via the Responses API
(POST /v1/responses), NOT the legacy Chat Completions API.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SALVAGE_DOMAINS = [
    "copart.com", "iaai.com", "autobidmaster.com", "salvagereseller.com",
    "abetter.bid", "salvageautosauction.com", "sca.auction", "salvagebid.com",
    "bid.cars", "bidfax.info", "stat.vin", "carfast.express", "poctra.com",
    "usedbidcars.com", "row52.com", "lkqcanada.ca", "copart.co.uk",
    "auctions.synetiq.co.uk", "pickles.com.au", "manheim.com.au",
]

_DOMAIN_LIST_STR = ", ".join(SALVAGE_DOMAINS)

_SEARCH_PROMPT_TEMPLATE = (
    'Search Google for the exact VIN "{vin}". '
    "Check if this VIN appears on any of these salvage/junk auction websites: "
    f"{_DOMAIN_LIST_STR}. "
    "Also check if any Google Image results show severe front-end or structural "
    "damage for this VIN. "
    "Return your findings as JSON with this exact structure: "
    '{{"found_on_sites": ["domain1.com", ...], "damage_images": true/false, '
    '"summary": "brief explanation"}}. '
    "Only include a domain in found_on_sites if the VIN appears verbatim in "
    "the URL or page snippet. If the VIN is not found on any salvage sites, "
    'return {{"found_on_sites": [], "damage_images": false, "summary": "clean"}}.'
)

_RESPONSES_URL = "https://api.openai.com/v1/responses"


def search_vin_salvage_history(
    vin: str,
    api_key: str,
    model: str = "gpt-5.4",
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Search for a VIN on salvage auction sites via OpenAI web_search.

    Returns:
        {
            "found_on_salvage_sites": ["copart.com", ...],
            "damage_images_found": bool,
            "summary": str,
            "raw_response": str,
        }
    """
    prompt = _SEARCH_PROMPT_TEMPLATE.format(vin=vin)

    payload = {
        "model": model,
        "tools": [{"type": "web_search"}],
        "input": prompt,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    logger.info("OpenAI web_search for VIN %s (model=%s)", vin, model)

    with httpx.Client(timeout=timeout) as client:
        resp = client.post(_RESPONSES_URL, json=payload, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    raw_text = _extract_output_text(data)

    logger.debug("OpenAI response for %s: %s", vin, raw_text[:500])

    return _parse_response(vin, raw_text)


def _extract_output_text(response_data: dict) -> str:
    """Pull the assistant's text from the Responses API output array."""
    output = response_data.get("output", [])
    parts = []
    for item in output:
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    parts.append(content.get("text", ""))
    return "\n".join(parts) if parts else str(response_data)


def _parse_response(vin: str, raw_text: str) -> dict[str, Any]:
    """Parse the model's response, strictly validating domain matches."""
    result = {
        "found_on_salvage_sites": [],
        "damage_images_found": False,
        "summary": "",
        "raw_response": raw_text,
    }

    # Try to extract JSON from the response
    json_match = re.search(r"\{[^{}]*\"found_on_sites\"[^{}]*\}", raw_text, re.DOTALL)
    if json_match:
        try:
            parsed = json.loads(json_match.group())
            reported_sites = parsed.get("found_on_sites", [])
            # Strict validation: only accept domains from our known list
            validated_sites = []
            for site in reported_sites:
                site_lower = site.lower().strip()
                for domain in SALVAGE_DOMAINS:
                    if domain in site_lower:
                        validated_sites.append(domain)
                        break
            result["found_on_salvage_sites"] = list(set(validated_sites))
            result["damage_images_found"] = bool(parsed.get("damage_images", False))
            result["summary"] = parsed.get("summary", "")
            return result
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: scan raw text for domain mentions with VIN nearby
    text_lower = raw_text.lower()
    vin_lower = vin.lower()
    for domain in SALVAGE_DOMAINS:
        if domain in text_lower and vin_lower in text_lower:
            # Check if domain and VIN appear in close proximity (within 500 chars)
            for match in re.finditer(re.escape(domain), text_lower):
                pos = match.start()
                window = text_lower[max(0, pos - 250):pos + 250]
                if vin_lower in window:
                    result["found_on_salvage_sites"].append(domain)
                    break

    result["found_on_salvage_sites"] = list(set(result["found_on_salvage_sites"]))

    # Check for damage image mentions
    damage_patterns = re.compile(
        r"severe\s*(?:front|rear|side).*damage|"
        r"(?:front|rear|side).*(?:collision|impact|crash)|"
        r"total(?:ed|loss)",
        re.IGNORECASE,
    )
    if damage_patterns.search(raw_text):
        result["damage_images_found"] = True

    result["summary"] = "Parsed from raw text (no structured JSON in response)"
    return result

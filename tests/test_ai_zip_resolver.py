from __future__ import annotations

import json
from pathlib import Path

import pytest

import ove_scraper.ai_zip_resolver as ai_zip_resolver
import ove_scraper.location_zip_lookup as location_zip_lookup


@pytest.fixture(autouse=True)
def _isolate_state(monkeypatch, tmp_path: Path) -> None:
    ai_zip_resolver.reset_process_state()
    monkeypatch.setattr(ai_zip_resolver, "_TELEMETRY_PATH", tmp_path / "zip_resolution.log")
    overrides_path = tmp_path / "data" / "auction_location_overrides.json"
    monkeypatch.setattr(location_zip_lookup, "OVERRIDE_DB_PATH", overrides_path)
    location_zip_lookup.load_override_db.cache_clear()
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5.4")
    monkeypatch.delenv("OVE_AI_ZIP_RESOLVER_ENABLED", raising=False)
    monkeypatch.delenv(ai_zip_resolver._BUDGET_ENV, raising=False)


def _stub_ai(monkeypatch, **kwargs):
    """Stub the inner web_search call without touching httpx or pgeocode."""
    def fake_resolve(pickup_location, auction_house, state, *, api_key, model, timeout=60.0):
        return kwargs.get("returns")
    monkeypatch.setattr(ai_zip_resolver, "resolve_zip_via_ai", fake_resolve)


def test_ai_resolver_persists_to_override_json(monkeypatch, tmp_path: Path) -> None:
    _stub_ai(monkeypatch, returns="64101")

    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_args, **_kwargs: None)

    zip_code = location_zip_lookup.resolve_location_zip(
        "MO - America's Auto Auction Kansas City",
        "America's Auto Auction Kansas City",
        "MO",
    )

    assert zip_code == "64101"

    overrides_file = location_zip_lookup.OVERRIDE_DB_PATH
    assert overrides_file.exists()
    payload = json.loads(overrides_file.read_text(encoding="utf-8"))
    entries = payload["overrides"]
    assert len(entries) == 1
    assert entries[0]["zip"] == "64101"
    assert entries[0]["state"] == "MO"
    assert entries[0]["source"] == "ai_web_search"


def test_ai_resolver_cache_clear_makes_subsequent_calls_use_override(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_resolve(*args, **kwargs):
        calls["n"] += 1
        return "64101"

    monkeypatch.setattr(ai_zip_resolver, "resolve_zip_via_ai", fake_resolve)
    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_a, **_kw: None)

    location_zip_lookup.resolve_location_zip(
        "MO - America's Auto Auction Kansas City",
        "America's Auto Auction Kansas City",
        "MO",
    )
    location_zip_lookup.resolve_location_zip(
        "MO - America's Auto Auction Kansas City",
        "America's Auto Auction Kansas City",
        "MO",
    )

    assert calls["n"] == 1


def test_state_centroid_fallback_when_ai_returns_none(monkeypatch) -> None:
    _stub_ai(monkeypatch, returns=None)
    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_a, **_kw: None)

    zip_code = location_zip_lookup.resolve_location_zip(
        "TN - United Auto Exchange",
        "United Auto Exchange",
        "TN",
    )

    assert zip_code == "37098"
    assert not location_zip_lookup.OVERRIDE_DB_PATH.exists()


def test_state_centroid_fallback_when_ai_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OVE_AI_ZIP_RESOLVER_ENABLED", "false")
    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_a, **_kw: None)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("AI resolver should not have been invoked")

    monkeypatch.setattr(ai_zip_resolver, "resolve_zip_via_ai", fail_if_called)

    zip_code = location_zip_lookup.resolve_location_zip(
        "OH - America's Auto Auction Columbus Fair",
        "America's Auto Auction Columbus Fair",
        "OH",
    )

    assert zip_code == "43018"


def test_state_centroid_fallback_when_no_api_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_a, **_kw: None)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("AI resolver should not have been invoked")

    monkeypatch.setattr(ai_zip_resolver, "resolve_zip_via_ai", fail_if_called)

    zip_code = location_zip_lookup.resolve_location_zip(
        "KS - Mid Kansas Auto Auction Inc",
        "Mid Kansas Auto Auction Inc",
        "KS",
    )

    assert zip_code == "67432"


def test_returns_none_when_state_unparseable(monkeypatch) -> None:
    monkeypatch.setattr(location_zip_lookup, "query_zip", lambda *_a, **_kw: None)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("AI resolver should not run without a state")

    monkeypatch.setattr(ai_zip_resolver, "resolve_zip_via_ai", fail_if_called)

    assert location_zip_lookup.resolve_location_zip(
        "Some Random Auction",
        "Some Random Auction",
        None,
    ) is None


def test_extract_zip_from_response_handles_clean_json() -> None:
    text = '{"zip": "32503", "city": "Pensacola", "source_url": "https://example.com"}'
    assert ai_zip_resolver._extract_zip_from_response(text) == "32503"


def test_extract_zip_from_response_strips_zip_plus_four() -> None:
    """ZIP-plus-4 inside the JSON value should be truncated to 5 digits."""
    text = '{"zip": "32503-1234", "city": "Pensacola"}'
    assert ai_zip_resolver._extract_zip_from_response(text) == "32503"


def test_extract_zip_from_response_ignores_bare_text() -> None:
    """Without our JSON envelope we return None (refuse to guess from prose)."""
    assert ai_zip_resolver._extract_zip_from_response("Pensacola FL 32503") is None
    assert ai_zip_resolver._extract_zip_from_response("no digits here") is None
    assert ai_zip_resolver._extract_zip_from_response("") is None


def test_validation_rejects_zip_in_wrong_state(monkeypatch) -> None:
    """A returned ZIP that pgeocodes to a different state must be rejected."""
    class FakeResult:
        state_code = "TX"

    class FakeNomi:
        def query_postal_code(self, _zip):
            return FakeResult()

    monkeypatch.setattr(ai_zip_resolver, "_nominatim", lambda: FakeNomi())
    assert ai_zip_resolver._zip_matches_state("75001", "TX") is True
    assert ai_zip_resolver._zip_matches_state("75001", "OH") is False


def test_validation_rejects_when_pgeocode_returns_nan(monkeypatch) -> None:
    class FakeResult:
        state_code = "nan"

    class FakeNomi:
        def query_postal_code(self, _zip):
            return FakeResult()

    monkeypatch.setattr(ai_zip_resolver, "_nominatim", lambda: FakeNomi())
    assert ai_zip_resolver._zip_matches_state("00000", "TX") is False


def test_budget_exhaustion_short_circuits(monkeypatch) -> None:
    """Once the per-process budget hits zero, no API calls are made."""
    monkeypatch.setenv(ai_zip_resolver._BUDGET_ENV, "0")
    ai_zip_resolver.reset_process_state()

    httpx_calls = {"n": 0}

    class FakeClient:
        def __init__(self, *_a, **_kw):
            pass

        def __enter__(self):
            httpx_calls["n"] += 1
            return self

        def __exit__(self, *_exc):
            return False

        def post(self, *_a, **_kw):
            raise AssertionError("budget should have prevented this call")

    monkeypatch.setattr(ai_zip_resolver.httpx, "Client", FakeClient)

    result = ai_zip_resolver.resolve_zip_via_ai(
        "MO - X", "X", "MO", api_key="k", model="gpt-5.4",
    )
    assert result is None
    assert httpx_calls["n"] == 0


def test_append_override_dedupes_existing_entry(tmp_path: Path) -> None:
    path = tmp_path / "overrides.json"
    ai_zip_resolver.append_override("PU", "AH", "FL", "32503", overrides_path=path)
    ai_zip_resolver.append_override("pu ", " AH", "fl", "32503", overrides_path=path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert len(payload["overrides"]) == 1


def test_append_override_preserves_unrelated_existing_entries(tmp_path: Path) -> None:
    path = tmp_path / "overrides.json"
    path.write_text(json.dumps({
        "version": 1,
        "overrides": [
            {"pickup_location": "OLD", "auction_house": "OLD AH", "state": "TX", "zip": "75001"},
        ],
    }))

    ai_zip_resolver.append_override("NEW", "NEW AH", "FL", "32503", overrides_path=path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    keys = {(e["state"], e["zip"]) for e in payload["overrides"]}
    assert keys == {("TX", "75001"), ("FL", "32503")}

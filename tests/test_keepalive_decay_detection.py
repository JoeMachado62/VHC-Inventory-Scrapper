"""Tests for keepalive slow-render decay detection (2026-05-04 fix).

Decay condition: 3 of last 5 keepalive ticks have cards_render_ms >
12000ms. When tripped, raise SavedSearchPageEmpty so the existing
cookie-clear + recover_browser_session handler in
main.py:run_browser_operation does the recovery. This catches a
degrading B session BEFORE it goes fully dead and triggers the
panic-relaunch loop that locked Manheim on 2026-05-04.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from ove_scraper.browser import SavedSearchPageEmpty
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


class _ConfigurableFakePage:
    """Like the persistent-tab fake page but with cards_render_ms
    controllable per-tick via a list passed in by the test."""

    def __init__(self, *, render_times_ms: list[int]) -> None:
        self.url = "https://www.ove.com/saved_searches#/"
        self._closed = False
        self._render_iter = iter(render_times_ms)
        self._is_login = False
        self.wait_for_selector_call_count = 0

    def goto(self, *_args, **_kwargs):
        pass

    def evaluate(self, _script: str):
        return False

    def wait_for_selector(self, _selector: str, *, timeout: int | None = None):
        # Use monotonic-mocking: instead of actually sleeping, the test
        # patches time.monotonic to advance by the desired render time.
        self.wait_for_selector_call_count += 1

    def wait_for_timeout(self, _ms: int) -> None:
        pass

    def close(self) -> None:
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


class _FakeBrowser:
    contexts: list = []


def _make_session(tmp_path: Path) -> PlaywrightCdpBrowserSession:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9223,
        # Decay detection lives in the persistent-tab path. Disable
        # active so we test the persistent-tab path directly.
        keepalive_active=False,
        keepalive_persistent_tab=True,
        keepalive_settle_ms=1,
    )
    return PlaywrightCdpBrowserSession(settings)


def _find_log_record(caplog, needle: str) -> str | None:
    for record in caplog.records:
        if needle in record.getMessage():
            return record.getMessage()
    return None


def _setup_session_with_render_sequence(session, monkeypatch, render_times_ms: list[int]):
    """Set up the session to return a configurable cards_render_ms per tick.

    Critical: the persistent-tab keepalive REUSES the same page across
    ticks (that's the feature being tested), so we set up ONE shared
    page and iterate through render_times_ms via mutable state. The
    fake clock advancement is global (cdp_browser.time.monotonic
    monkey-patch) so every wait_for_selector call advances the SAME
    fake offset — which is what we want.
    """
    import ove_scraper.cdp_browser as cdp_mod

    shared_page = _ConfigurableFakePage(render_times_ms=render_times_ms)
    render_iter = iter(render_times_ms)
    monkeypatch.setattr(session, "_connect_browser", lambda: _FakeBrowser())
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: shared_page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)

    real_monotonic = cdp_mod.time.monotonic
    fake_offset = {"value": 0.0}

    def _fake_monotonic():
        return real_monotonic() + fake_offset["value"]

    def _wait_with_advance(*args, **kwargs):
        try:
            ms = next(render_iter)
        except StopIteration:
            ms = 0
        fake_offset["value"] += ms / 1000.0

    shared_page.wait_for_selector = _wait_with_advance
    monkeypatch.setattr(cdp_mod.time, "monotonic", _fake_monotonic)
    return shared_page


def test_decay_not_tripped_when_renders_are_fast(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path)
    _setup_session_with_render_sequence(session, monkeypatch, [3000] * 5)
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        for _ in range(5):
            session.touch_session()
    assert _find_log_record(caplog, "KEEPALIVE_DECAY_DETECTED") is None
    assert all(ms < 12000 for ms in session._keepalive_render_history)


def test_decay_tripped_when_3_of_last_5_are_slow(tmp_path, monkeypatch, caplog):
    """Feed [3000, 14000, 4000, 14000, 14000] — 3 of last 5 > 12000.

    2026-05-04 fix: decay no longer raises SavedSearchPageEmpty (which
    triggered cookie wipes). It now logs `action=observation_only` and
    returns cleanly. The next REAL operation will hit the failure
    through a path with proper recovery semantics."""
    session = _make_session(tmp_path)
    _setup_session_with_render_sequence(
        session, monkeypatch, [3000, 14000, 4000, 14000, 14000],
    )
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        # All 5 ticks must succeed without raising. Decay is observed
        # on the 5th tick but doesn't propagate.
        for _ in range(5):
            session.touch_session()

    decay_msg = _find_log_record(caplog, "KEEPALIVE_DECAY_DETECTED")
    assert decay_msg is not None
    assert "port=9223" in decay_msg
    assert "action=observation_only" in decay_msg, (
        "decay must be observation-only, NOT cookie_clear_and_relaunch — "
        "the latter wiped device-trust cookies and forced 2FA storms"
    )
    # Deque was reset after firing so we don't fire on EVERY tick after.
    assert session._keepalive_render_history == []


def test_decay_does_not_trip_with_only_2_slow_in_window(tmp_path, monkeypatch, caplog):
    """Feed [14000, 3000, 14000, 3000, 3000] — only 2 of last 5 > 12000, no fire."""
    session = _make_session(tmp_path)
    _setup_session_with_render_sequence(
        session, monkeypatch, [14000, 3000, 14000, 3000, 3000],
    )
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        for _ in range(5):
            session.touch_session()
    assert _find_log_record(caplog, "KEEPALIVE_DECAY_DETECTED") is None


def test_decay_does_not_destroy_keepalive_page(tmp_path, monkeypatch):
    """2026-05-04 fix: decay is now observation-only. It must NOT
    destroy the persistent page (cards_empty / cards_did_not_render
    branches handle that when there's an actual page-state problem).
    Decay just records the slowness signal and lets the page keep
    being reused — slow render isn't a reason to throw away a working
    session."""
    session = _make_session(tmp_path)
    _setup_session_with_render_sequence(
        session, monkeypatch, [3000, 14000, 4000, 14000, 14000],
    )
    for _ in range(5):
        session.touch_session()  # must not raise
    # Persistent page should still be set — slow render isn't a fatal page issue.
    assert session._keepalive_page is not None, (
        "decay must NOT discard the persistent page — it's just an observation"
    )


def test_decay_history_capped_at_max(tmp_path, monkeypatch):
    """Verify the deque stays bounded so we don't grow unbounded memory."""
    session = _make_session(tmp_path)
    # 30 fast ticks — well above the cap of 10.
    _setup_session_with_render_sequence(session, monkeypatch, [3000] * 30)
    for _ in range(30):
        session.touch_session()
    assert len(session._keepalive_render_history) <= 10

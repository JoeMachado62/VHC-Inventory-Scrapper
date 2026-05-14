"""Tests for the persistent dedicated keepalive tab (2026-05-04 fix).

Pre-fix: every keepalive tick opened a new worker tab, did `page.goto`,
waited 1.5s, closed. OVE's session-refreshing polling XHRs may not
have fired before the tab closed, so B's idle session decayed silently
and eventually triggered the panic-relaunch loop that locked the
Manheim account on 2026-05-04.

Post-fix: one long-lived "keepalive page" is opened on first tick and
reused across ticks. Each tick verifies cards rendered (proving
auth + backend token + React all work) and settles for 8s so polling
XHRs can fire. The page is NEVER closed between ticks.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from ove_scraper.browser import BrowserSessionError, SavedSearchPageEmpty
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


class _PersistentFakePage:
    """Stand-in that supports the persistent-tab keepalive flow.

    Tracks goto/close call counts, supports controlled is_login /
    SavedSearchPageEmpty / cards-render-delay simulation. Reuse across
    test functions by passing different constructor args.
    """

    def __init__(
        self,
        *,
        url: str = "https://www.ove.com/saved_searches#/",
        cards_render_ms: int = 100,
        cards_raise: Exception | None = None,
        is_login: bool = False,
    ) -> None:
        self.url = url
        self._closed = False
        self._cards_render_ms = cards_render_ms
        self._cards_raise = cards_raise
        self._is_login = is_login
        self.goto_call_count = 0
        self.wait_for_selector_call_count = 0
        self.wait_for_timeout_calls: list[int] = []
        self.close_call_count = 0

    def goto(self, *_args, **_kwargs):
        self.goto_call_count += 1

    def wait_for_selector(self, _selector: str, *, timeout: int | None = None):
        self.wait_for_selector_call_count += 1
        if self._cards_raise is not None:
            raise self._cards_raise

    def evaluate(self, _script: str):
        # Used by _detect_empty_saved_searches_page and _is_login_page.
        # Return False to signal "not empty / not login" by default.
        return False

    def wait_for_timeout(self, ms: int) -> None:
        self.wait_for_timeout_calls.append(ms)

    def close(self) -> None:
        self.close_call_count += 1
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


class _FakeBrowser:
    contexts: list = []


def _make_session(tmp_path: Path, port: int = 9223, *, settle_ms: int = 8000) -> PlaywrightCdpBrowserSession:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=port,
        # These tests target the persistent-tab path specifically.
        # Disable keepalive_active so the dispatcher routes to
        # _touch_session_persistent_tab. The active path is covered
        # separately in test_keepalive_active.py.
        keepalive_active=False,
        keepalive_persistent_tab=True,
        keepalive_settle_ms=settle_ms,
    )
    return PlaywrightCdpBrowserSession(settings)


def _wire_session_for_persistent_tab(session, monkeypatch, page, browser):
    """Common setup: stub out everything except the persistent-tab logic."""
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: page._is_login)


def _find_keepalive_tick_record(caplog) -> str | None:
    for record in caplog.records:
        if record.levelno >= logging.WARNING and "KEEPALIVE_TICK" in record.getMessage():
            return record.getMessage()
    return None


def test_first_tick_opens_keepalive_page_and_does_not_close_it(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, settle_ms=10)  # short settle for fast test
    page = _PersistentFakePage()
    _wire_session_for_persistent_tab(session, monkeypatch, page, _FakeBrowser())

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    assert page.goto_call_count == 1
    assert page.wait_for_selector_call_count == 1, "should call _wait_for_saved_search_cards"
    assert page.wait_for_timeout_calls, "should call wait_for_timeout for settle"
    assert page.close_call_count == 0, "persistent page must NOT be closed"
    assert session._keepalive_page is page

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=ok" in msg
    assert "tab_strategy=persistent" in msg
    assert "cards_render_ms=" in msg


def test_second_tick_reuses_the_same_page(tmp_path, monkeypatch):
    session = _make_session(tmp_path, settle_ms=10)
    page = _PersistentFakePage()
    open_calls = {"n": 0}

    def _open(_browser):
        open_calls["n"] += 1
        return page

    monkeypatch.setattr(session, "_connect_browser", lambda: _FakeBrowser())
    monkeypatch.setattr(session, "_open_dedicated_ove_page", _open)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)

    session.touch_session()
    session.touch_session()
    session.touch_session()

    assert open_calls["n"] == 1, "should open the persistent page exactly once across ticks"
    assert page.goto_call_count == 3, "each tick goto's the same page"
    assert page.close_call_count == 0


def test_keepalive_settle_ms_is_honored(tmp_path, monkeypatch):
    session = _make_session(tmp_path, settle_ms=4321)
    page = _PersistentFakePage()
    _wire_session_for_persistent_tab(session, monkeypatch, page, _FakeBrowser())

    session.touch_session()

    assert 4321 in page.wait_for_timeout_calls


def test_cards_did_not_render_emits_outcome_without_raising(tmp_path, monkeypatch, caplog):
    """2026-05-04 fix: cards_did_not_render must NOT raise — it would
    propagate to run_browser_operation's BrowserSessionError handler
    which calls recover_browser_session (Chrome relaunch). The
    keepalive's job is OBSERVATION; the next real op handles recovery."""
    session = _make_session(tmp_path, settle_ms=10)
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    page = _PersistentFakePage(cards_raise=PlaywrightTimeoutError("did not render"))
    _wire_session_for_persistent_tab(session, monkeypatch, page, _FakeBrowser())

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        # MUST NOT raise.
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=cards_did_not_render" in msg
    # Persistent page must be cleared so next tick gets a fresh chance.
    assert session._keepalive_page is None


def test_cards_empty_does_not_trigger_cookie_clear(tmp_path, monkeypatch, caplog):
    """2026-05-04 fix: cards_empty must NOT propagate SavedSearchPageEmpty
    upward. The pre-fix behavior caused run_browser_operation to call
    _clear_chrome_cookies, which wiped the Manheim device-trust cookie
    and forced a fresh 2FA text on every recovery cycle."""
    session = _make_session(tmp_path, settle_ms=10)
    page = _PersistentFakePage(cards_raise=SavedSearchPageEmpty("empty"))
    _wire_session_for_persistent_tab(session, monkeypatch, page, _FakeBrowser())

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        # MUST NOT raise — that's the whole point of the fix.
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=cards_empty" in msg
    # Persistent page reset so next tick gets fresh tab on a fresh nav.
    assert session._keepalive_page is None
    # Browser handle must NOT be reset — CDP itself is fine.
    # (we didn't set it to a sentinel in this test; just confirming
    # the function returned cleanly without raising)


def test_persistent_tab_skipped_by_sweep_orphan_tabs(tmp_path, monkeypatch):
    """The persistent keepalive page must NOT be killed by sweep_orphan_tabs
    even if its URL transiently shows about:blank during navigation."""
    session = _make_session(tmp_path, settle_ms=10)
    page = _PersistentFakePage(url="about:blank")  # transient about:blank state
    session._keepalive_page = page

    class _FakeContext:
        pages = [page]

    class _BrowserWithKeepalive:
        contexts = [_FakeContext()]

    session._browser = _BrowserWithKeepalive()
    closed = session.sweep_orphan_tabs()

    assert closed == 0, "keepalive page must be skipped by sweep"
    assert page.close_call_count == 0


def test_setting_off_falls_back_to_worker_tab_path(tmp_path, monkeypatch, caplog):
    """KEEPALIVE_PERSISTENT_TAB=false must give byte-identical behavior
    to the pre-2026-05-04 worker-tab pattern."""
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9222,
        keepalive_active=False,
        keepalive_persistent_tab=False,
    )
    session = PlaywrightCdpBrowserSession(settings)

    page = _PersistentFakePage()
    monkeypatch.setattr(session, "_connect_browser", lambda: _FakeBrowser())
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    # Worker-tab path closes the page after each tick.
    assert page.close_call_count == 1
    assert session._keepalive_page is None

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "tab_strategy=worker" in msg

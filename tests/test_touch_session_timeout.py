"""Tests for touch_session's transient-timeout handling.

The keepalive (touch_session) used to escalate every Playwright
timeout to a full Chrome kill+relaunch, which was the proximate cause
of the "about:blank tab opens over and over on Login B" symptom on
2026-04-30. This module verifies that PlaywrightTimeoutError now
returns cleanly without raising or resetting the browser handle.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from ove_scraper.browser import BrowserSessionError
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


class _FakePage:
    def __init__(self, *, goto_raises: Exception | None = None) -> None:
        self._goto_raises = goto_raises
        self._closed = False
        self.url = "https://www.ove.com/saved_searches#/"

    def goto(self, *_args, **_kwargs):
        if self._goto_raises is not None:
            raise self._goto_raises

    def close(self) -> None:
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


class _FakeBrowser:
    pass


def _make_session(tmp_path: Path) -> PlaywrightCdpBrowserSession:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        # Worker-tab path (open/goto/close). Persistent-tab and active
        # paths are tested separately in their own files.
        keepalive_persistent_tab=False,
        keepalive_active=False,
    )
    return PlaywrightCdpBrowserSession(settings)


def test_touch_session_swallows_playwright_timeout(tmp_path, monkeypatch):
    """A PlaywrightTimeoutError during keepalive must NOT raise and
    must NOT reset _browser. This is the 2026-04-30 fix for the
    repeating about:blank tabs on Login B."""
    session = _make_session(tmp_path)
    sentinel_browser = _FakeBrowser()
    session._browser = sentinel_browser  # pretend we're connected

    monkeypatch.setattr(session, "_connect_browser", lambda: sentinel_browser)
    fake_page = _FakePage(goto_raises=PlaywrightTimeoutError("Timeout 30000ms exceeded"))
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: fake_page)

    # Must NOT raise.
    session.touch_session()

    # Page must have been closed in the inner finally (no leaked tab).
    assert fake_page.is_closed() is True

    # Must NOT have reset _browser — the CDP connection was fine, only
    # the page load was slow. Resetting would force a needless
    # reconnect on the next tick.
    assert session._browser is sentinel_browser


def test_touch_session_still_raises_on_real_auth_error(tmp_path, monkeypatch):
    """A genuine auth-loss (raised as BrowserSessionError from
    underlying calls) must still propagate as BrowserSessionError so
    run_browser_operation can drive recovery. The timeout exemption
    is narrow: ONLY PlaywrightTimeoutError is treated as transient."""
    session = _make_session(tmp_path)
    sentinel_browser = _FakeBrowser()
    session._browser = sentinel_browser

    monkeypatch.setattr(session, "_connect_browser", lambda: sentinel_browser)
    fake_page = _FakePage(
        goto_raises=BrowserSessionError("OVE session is not authenticated"),
    )
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: fake_page)

    with pytest.raises(BrowserSessionError):
        session.touch_session()

    # On a real failure, _browser SHOULD be reset so the next call
    # reconnects cleanly.
    assert session._browser is None


def test_touch_session_other_exception_still_raises_browser_session_error(tmp_path, monkeypatch):
    """A non-timeout, non-BrowserSessionError exception during
    keepalive should still wrap as BrowserSessionError so the main
    loop's circuit breaker counts it. Only PlaywrightTimeoutError is
    exempt."""
    session = _make_session(tmp_path)
    sentinel_browser = _FakeBrowser()
    session._browser = sentinel_browser

    monkeypatch.setattr(session, "_connect_browser", lambda: sentinel_browser)
    fake_page = _FakePage(goto_raises=RuntimeError("net::ERR_ABORTED"))
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: fake_page)

    with pytest.raises(BrowserSessionError):
        session.touch_session()

    assert session._browser is None

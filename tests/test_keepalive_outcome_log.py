"""Tests for the structured KEEPALIVE_TICK log line (2026-05-01).

Pre-telemetry, the only log of a keepalive cycle was either silence
(success) or a stack trace (failure). The about:blank-on-Login-B
symptom could be three different causes and the logs couldn't tell
them apart. The new outcome enum makes each cause distinct in
`Select-String "KEEPALIVE_TICK"` output.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from ove_scraper.browser import BrowserSessionError
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


class _FakePage:
    def __init__(
        self,
        *,
        url: str = "https://www.ove.com/saved_searches#/",
        goto_raises: Exception | None = None,
        is_login: bool = False,
    ) -> None:
        self.url = url
        self._goto_raises = goto_raises
        self._is_login = is_login
        self._closed = False

    def goto(self, *_args, **_kwargs):
        if self._goto_raises is not None:
            raise self._goto_raises

    def close(self) -> None:
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


class _FakeBrowser:
    """Stand-in for a Playwright Browser. Exposes `contexts` so the
    new _snapshot_first_blank_or_auth_url helper can iterate without
    raising."""

    contexts: list = []


def _make_session(tmp_path: Path, port: int = 9223) -> PlaywrightCdpBrowserSession:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=port,
        # These tests validate the WORKER-TAB outcome paths
        # (open/goto/close). The persistent-tab and active paths are
        # exercised separately in their own test files.
        keepalive_persistent_tab=False,
        keepalive_active=False,
    )
    return PlaywrightCdpBrowserSession(settings)


def _find_keepalive_tick_record(caplog) -> str | None:
    for record in caplog.records:
        if record.levelno >= logging.WARNING and "KEEPALIVE_TICK" in record.getMessage():
            return record.getMessage()
    return None


def test_keepalive_logs_outcome_ok_on_success(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    page = _FakePage(url="https://www.ove.com/saved_searches#/")
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None, f"no KEEPALIVE_TICK line emitted; records: {[r.getMessage() for r in caplog.records]}"
    assert "port=9223" in msg
    assert "outcome=ok" in msg
    assert page._closed is True


def test_keepalive_logs_outcome_goto_timeout_on_worker_page_timeout(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    page = _FakePage(
        url="https://auth.manheim.com/as/authorization",
        goto_raises=PlaywrightTimeoutError("Page.goto: Timeout 30000ms exceeded"),
    )
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=goto_timeout" in msg
    assert "url_at_failure=https://auth.manheim.com/as/authorization" in msg
    assert page._closed is True


def test_keepalive_logs_outcome_goto_timeout_on_seed_page_timeout(tmp_path, monkeypatch, caplog):
    """When _open_dedicated_ove_page raises PlaywrightTimeoutError (the
    seed-page navigation path failed before the worker tab even opened),
    the outcome must still be tagged as goto_timeout."""
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)

    def _raise_timeout(_b):
        raise PlaywrightTimeoutError("Page.goto: Timeout 30000ms exceeded")

    monkeypatch.setattr(session, "_open_dedicated_ove_page", _raise_timeout)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=goto_timeout" in msg


def test_keepalive_logs_outcome_login_blocked_when_lockout_active(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    page = _FakePage(url="https://auth.manheim.com/as/authorization")
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: True)
    monkeypatch.setattr(session, "_try_single_shot_login_click", lambda _p: False)

    # Force the lockout state read to report blocked.
    from ove_scraper import auth_lockout as al
    from dataclasses import replace as _dataclass_replace
    blocked = al.LockoutState(
        blocked=True, reason="test rate limit", blocked_until_utc=None,
        requires_manual_unlock=False,
    )
    monkeypatch.setattr(al, "get_state", lambda _d, port=None: blocked)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        with pytest.raises(BrowserSessionError):
            session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=landed_on_login_then_click_blocked_by_lockout" in msg


def test_keepalive_logs_outcome_login_failed_when_click_didnt_take(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    page = _FakePage(url="https://auth.manheim.com/as/authorization")
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: True)
    monkeypatch.setattr(session, "_try_single_shot_login_click", lambda _p: False)

    # Lockout reports unblocked — distinguishes click_failed vs
    # blocked_by_lockout.
    from ove_scraper import auth_lockout as al
    unblocked = al.LockoutState(
        blocked=False, reason=None, blocked_until_utc=None,
        requires_manual_unlock=False,
    )
    monkeypatch.setattr(al, "get_state", lambda _d, port=None: unblocked)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        with pytest.raises(BrowserSessionError):
            session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=landed_on_login_then_click_failed" in msg


def test_keepalive_logs_outcome_login_ok_when_click_succeeds(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)
    browser = _FakeBrowser()
    page = _FakePage(url="https://auth.manheim.com/as/authorization")
    monkeypatch.setattr(session, "_connect_browser", lambda: browser)
    monkeypatch.setattr(session, "_open_dedicated_ove_page", lambda _b: page)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: True)
    monkeypatch.setattr(session, "_try_single_shot_login_click", lambda _p: True)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=landed_on_login_then_clicked_ok" in msg
    assert page._closed is True


def test_keepalive_logs_outcome_connect_failed_when_connect_raises(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, port=9223)

    def _raise(*_args):
        raise RuntimeError("CDP connect refused")
    monkeypatch.setattr(session, "_connect_browser", _raise)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        with pytest.raises(BrowserSessionError):
            session.touch_session()

    msg = _find_keepalive_tick_record(caplog)
    assert msg is not None
    assert "outcome=connect_failed" in msg

"""Tests for the per-port saved-search-timeout streak escalation
(2026-05-01).

Pre-fix: a goto timeout against the saved-searches URL was matched by
_is_transient_per_operation_error, so the main loop's `except` would
log "transient, retry" up to 10 times before triggering a runtime
rebuild — and the rebuild left Chrome's dead session intact, so the
next attempt produced the same timeout, infinite loop. This was the
2026-05-01 production failure on Login B.

Post-fix: run_browser_operation tracks consecutive saved-search goto
timeouts per port; at threshold (3) it routes the failure into
recover_browser_session (Chrome kill+relaunch, lockout-gated)
instead of letting the transient retry loop continue.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ove_scraper import main as main_module
from ove_scraper.browser import BrowserSessionError
from ove_scraper.config import Settings


def _make_settings(tmp_path: Path, port: int) -> Settings:
    return Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=port,
    )


def _saved_search_timeout_exc():
    return RuntimeError(
        "Page.goto: Timeout 60000ms exceeded.\n"
        "Call log:\n  - navigating to \"https://www.ove.com/saved_searches#/\""
    )


def _reset_streak():
    main_module._SAVED_SEARCH_TIMEOUT_STREAK.clear()


def test_classifier_matches_saved_search_goto_timeout():
    assert main_module._is_saved_search_goto_timeout(_saved_search_timeout_exc()) is True


def test_classifier_does_not_match_other_timeouts():
    other = RuntimeError("Page.goto: Timeout 60000ms exceeded.\n  - navigating to \"https://example.com/foo\"")
    assert main_module._is_saved_search_goto_timeout(other) is False


def test_classifier_does_not_match_non_timeout_errors():
    not_timeout = RuntimeError("net::ERR_ABORTED at https://www.ove.com/saved_searches#/")
    assert main_module._is_saved_search_goto_timeout(not_timeout) is False


def test_session_probe_classifier_matches_sync_and_keepalive():
    assert main_module._is_saved_search_session_probe("hourly sync") is True
    assert main_module._is_saved_search_session_probe("browser keepalive") is True
    assert main_module._is_saved_search_session_probe("browser keepalive (sync)") is True


def test_session_probe_classifier_skips_unrelated_operations():
    assert main_module._is_saved_search_session_probe("detail poll") is False
    assert main_module._is_saved_search_session_probe("hot-deal screen") is False


def test_streak_escalates_to_recover_after_threshold(tmp_path, monkeypatch):
    """Three consecutive saved-search goto timeouts must trigger
    recover_browser_session (instead of letting the transient retry
    loop continue forever)."""
    _reset_streak()
    settings = _make_settings(tmp_path, port=9223)
    browser = MagicMock()

    op_calls = {"n": 0}
    def operation():
        op_calls["n"] += 1
        # First three calls fail with the saved-search timeout. The
        # fourth (the post-recovery retry) succeeds.
        if op_calls["n"] <= 3:
            raise _saved_search_timeout_exc()
        return "ok"

    recover_calls = []
    def fake_recover(_settings, _browser, _logger, notifier=None):
        recover_calls.append(_settings.chrome_debug_port)
    monkeypatch.setattr(main_module, "recover_browser_session", fake_recover)
    monkeypatch.setattr(main_module, "ensure_browser_session", lambda *a, **k: None)

    # OveAutomationLock would normally block in tests; bypass by stubbing
    # to a no-op context manager.
    class _NoLock:
        def __init__(self, *_a, **_kw): pass
        def __enter__(self): return self
        def __exit__(self, *exc): return False
    monkeypatch.setattr(main_module, "OveAutomationLock", _NoLock)

    # Calls 1 and 2 return None (run_browser_operation re-raises after
    # logging "letting transient handler retry"). Wrap in pytest.raises
    # since the op error escapes the outer try.
    for _expected_streak in (1, 2):
        with pytest.raises(RuntimeError):
            main_module.run_browser_operation(
                settings, browser, MagicMock(),
                operation, "hourly sync",
            )

    # Call 3 hits the threshold; recover is invoked, then operation
    # retries and returns "ok".
    result = main_module.run_browser_operation(
        settings, browser, MagicMock(),
        operation, "hourly sync",
    )
    assert result == "ok"
    assert recover_calls == [9223]
    # Streak resets to 0 after escalation.
    assert main_module._SAVED_SEARCH_TIMEOUT_STREAK.get(9223, 0) == 0


def test_streak_is_per_port(tmp_path, monkeypatch):
    """A port-9222 timeout streak must NOT increment port 9223's
    counter — that was the architectural property the per-port split
    was designed to provide."""
    _reset_streak()
    settings_a = _make_settings(tmp_path / "a", port=9222)
    browser = MagicMock()

    def operation():
        raise _saved_search_timeout_exc()

    monkeypatch.setattr(main_module, "ensure_browser_session", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "recover_browser_session", lambda *a, **k: None)
    class _NoLock:
        def __init__(self, *_a, **_kw): pass
        def __enter__(self): return self
        def __exit__(self, *exc): return False
    monkeypatch.setattr(main_module, "OveAutomationLock", _NoLock)

    with pytest.raises(RuntimeError):
        main_module.run_browser_operation(
            settings_a, browser, MagicMock(),
            operation, "hourly sync",
        )

    assert main_module._SAVED_SEARCH_TIMEOUT_STREAK.get(9222, 0) == 1
    assert main_module._SAVED_SEARCH_TIMEOUT_STREAK.get(9223, 0) == 0


def test_streak_resets_on_successful_operation(tmp_path, monkeypatch):
    """A success after partial-streak failures clears the counter so
    the next blip starts at zero (we don't want a slow accumulation
    of unrelated timeouts to silently escalate weeks later)."""
    _reset_streak()
    settings = _make_settings(tmp_path, port=9222)
    browser = MagicMock()
    main_module._SAVED_SEARCH_TIMEOUT_STREAK[9222] = 2  # prior failures

    def operation():
        return "ok"

    monkeypatch.setattr(main_module, "ensure_browser_session", lambda *a, **k: None)
    class _NoLock:
        def __init__(self, *_a, **_kw): pass
        def __enter__(self): return self
        def __exit__(self, *exc): return False
    monkeypatch.setattr(main_module, "OveAutomationLock", _NoLock)

    result = main_module.run_browser_operation(
        settings, browser, MagicMock(),
        operation, "hourly sync",
    )
    assert result == "ok"
    assert main_module._SAVED_SEARCH_TIMEOUT_STREAK.get(9222, 0) == 0


def test_non_session_probe_operation_is_not_counted(tmp_path, monkeypatch):
    """Detail-poll and hot-deal operations don't probe the saved-search
    URL; their timeouts must NOT increment the streak counter (the
    streak is specifically for session-liveness probes)."""
    _reset_streak()
    settings = _make_settings(tmp_path, port=9222)
    browser = MagicMock()

    def operation():
        raise _saved_search_timeout_exc()

    monkeypatch.setattr(main_module, "ensure_browser_session", lambda *a, **k: None)
    class _NoLock:
        def __init__(self, *_a, **_kw): pass
        def __enter__(self): return self
        def __exit__(self, *exc): return False
    monkeypatch.setattr(main_module, "OveAutomationLock", _NoLock)

    with pytest.raises(RuntimeError):
        main_module.run_browser_operation(
            settings, browser, MagicMock(),
            operation, "detail poll",
        )
    assert 9222 not in main_module._SAVED_SEARCH_TIMEOUT_STREAK

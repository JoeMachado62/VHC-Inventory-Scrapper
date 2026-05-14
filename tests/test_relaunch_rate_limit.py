"""Tests for the per-port Chrome relaunch rate limit (2026-05-04 fix).

The 2026-05-04 incident was caused by 6 Chrome relaunches on port 9223
in 9 minutes (~90s spacing). The existing per-port lockout cooldown
only kicks in AFTER an account-lock event has been recorded; the rate
gate kicks in BEFORE, so even a panic-relaunch loop cannot fire faster
than once per 5 minutes per port.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ove_scraper import auth_lockout
from ove_scraper.browser import BrowserSessionError


# ---------------------------------------------------------------------------
# Layer 1: ledger helpers
# ---------------------------------------------------------------------------


def test_record_chrome_relaunch_writes_per_port_entry(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    auth_lockout.record_chrome_relaunch(artifact_dir, port=9222)
    state_path = artifact_dir / "_state" / "auth_lockout_9222.json"
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "relaunch_history_utc" in state
    assert len(state["relaunch_history_utc"]) == 1


def test_is_relaunch_rate_limited_true_immediately_after_record(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    auth_lockout.record_chrome_relaunch(artifact_dir, port=9222)
    is_limited, age_s = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9222)
    assert is_limited is True
    assert age_s >= 0


def test_is_relaunch_rate_limited_false_when_no_history(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    is_limited, age_s = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9222)
    assert is_limited is False
    assert age_s == -1


def test_relaunch_rate_limit_is_per_port(tmp_path):
    """A relaunch on 9222 must NOT rate-limit 9223."""
    artifact_dir = tmp_path / "artifacts"
    auth_lockout.record_chrome_relaunch(artifact_dir, port=9222)

    is_limited_a, _ = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9222)
    is_limited_b, _ = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9223)

    assert is_limited_a is True
    assert is_limited_b is False


def test_unlock_clears_relaunch_history(tmp_path):
    """Manual unlock should reset the relaunch ledger so the operator
    has a fresh start after fixing whatever caused the panic."""
    artifact_dir = tmp_path / "artifacts"
    auth_lockout.record_chrome_relaunch(artifact_dir, port=9222)
    auth_lockout.record_chrome_relaunch(artifact_dir, port=9223)

    auth_lockout.unlock(artifact_dir)

    is_limited_a, _ = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9222)
    is_limited_b, _ = auth_lockout.is_relaunch_rate_limited(artifact_dir, port=9223)
    assert is_limited_a is False
    assert is_limited_b is False


def test_relaunch_history_capped(tmp_path):
    """Don't grow unbounded — keep last 20 entries."""
    artifact_dir = tmp_path / "artifacts"
    for _ in range(30):
        auth_lockout.record_chrome_relaunch(artifact_dir, port=9222)
    state = json.loads((artifact_dir / "_state" / "auth_lockout_9222.json").read_text(encoding="utf-8"))
    assert len(state["relaunch_history_utc"]) <= 20


# ---------------------------------------------------------------------------
# Layer 2: recover_browser_session integration
# ---------------------------------------------------------------------------


def test_recover_browser_session_raises_when_rate_limited(tmp_path, monkeypatch):
    from ove_scraper import main as main_module
    from ove_scraper.config import Settings

    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9223,
    )
    # Pre-record a relaunch so the rate gate is tripped.
    auth_lockout.record_chrome_relaunch(settings.artifact_dir, port=9223)

    browser = MagicMock()
    logger = MagicMock()

    with pytest.raises(BrowserSessionError) as exc_info:
        main_module.recover_browser_session(settings, browser, logger)
    assert "Browser recovery skipped" in str(exc_info.value)
    assert "9223" in str(exc_info.value)
    # browser.close() must NOT have been called — the gate refuses BEFORE
    # killing Chrome.
    browser.close.assert_not_called()


def test_recover_browser_session_records_relaunch_after_launch(tmp_path, monkeypatch):
    from ove_scraper import main as main_module
    from ove_scraper.config import Settings

    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9222,
    )
    browser = MagicMock()
    logger = MagicMock()

    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "launch_browser_script", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "wait_for_cdp", lambda *a, **k: None)
    monkeypatch.setattr(browser, "ensure_session", lambda: None)

    main_module.recover_browser_session(settings, browser, logger)

    is_limited, _ = auth_lockout.is_relaunch_rate_limited(settings.artifact_dir, port=9222)
    assert is_limited is True, "relaunch must be recorded so subsequent attempts are gated"


def test_consecutive_recover_calls_are_rate_gated(tmp_path, monkeypatch):
    """The 2026-05-04 scenario regression test: simulate a panic loop
    of 6 recover_browser_session calls in quick succession on the same
    port. Only the first should succeed; the rest must all raise."""
    from ove_scraper import main as main_module
    from ove_scraper.config import Settings

    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9223,
    )
    browser = MagicMock()
    logger = MagicMock()

    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "launch_browser_script", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "wait_for_cdp", lambda *a, **k: None)
    monkeypatch.setattr(browser, "ensure_session", lambda: None)

    # First call succeeds.
    main_module.recover_browser_session(settings, browser, logger)
    # Next 5 must all be refused — the May 4 incident class.
    refused_count = 0
    for _ in range(5):
        try:
            main_module.recover_browser_session(settings, browser, logger)
        except BrowserSessionError:
            refused_count += 1
    assert refused_count == 5, (
        "all 5 follow-up recover calls within 5 min must be rate-gated; "
        "exactly the panic-loop pattern that locked Manheim on 2026-05-04"
    )

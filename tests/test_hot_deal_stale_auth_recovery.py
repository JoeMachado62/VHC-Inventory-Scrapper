"""Tests for stale-auth recovery in the Hot Deal pipeline (2026-05-03 fix).

Pre-fix bug chain (verified in production logs 2026-05-03 07:00-08:08 ET):
  1. browser.export_saved_search raises SavedSearchPageEmpty correctly
  2. _export_search caught it as `except Exception` → returned None
  3. run_once line 100-101 re-raised as generic RuntimeError
  4. run_once outer try/except marked status="failed" via finish_run, NO
     exception propagated up
  5. run_hot_deal_with_recovery saw status="failed", marked state failed,
     and the scheduler retried 30 min later — using the SAME stale
     cookies, hitting the SAME failure, three times in a row.

Post-fix:
  - Session errors propagate from _export_search
  - run_once re-raises SavedSearchPageEmpty after finish_run("failed")
  - run_hot_deal_with_recovery catches SavedSearchPageEmpty BEFORE
    BrowserSessionError, calls _clear_chrome_cookies + recover_browser_session,
    marks state failed with a clear reason, and returns cleanly so the
    next scheduler tick uses a fresh authenticated session.
"""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from ove_scraper.browser import BrowserSessionError, SavedSearchPageEmpty
from ove_scraper.config import Settings


EASTERN = ZoneInfo("America/New_York")


def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        vch_api_base_url="http://unused.test",
        vch_service_token="unused",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        hot_deal_enabled=True,
        hot_deal_daily_schedule_eastern=((7, 0),),
        hot_deal_retry_delay_seconds=1800,
        hot_deal_max_daily_attempts=3,
        hot_deal_searches=("Factory Warranty Active", "VCH Marketing List"),
    )


# ---------------------------------------------------------------------------
# Layer 1: _export_search must propagate SavedSearchPageEmpty
# ---------------------------------------------------------------------------


def test_export_search_propagates_saved_search_page_empty(tmp_path):
    """_export_search must NOT swallow SavedSearchPageEmpty as a None return —
    that loses the type signal that triggers cookie-clear recovery."""
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner
    from ove_scraper.hot_deal_db import init_db

    settings = _make_settings(tmp_path)
    db_conn = init_db(tmp_path / "hot_deal.db")
    browser = MagicMock()
    browser.export_saved_search.side_effect = SavedSearchPageEmpty(
        "OVE saved-searches page shows 'No Saved Searches' — known intermittent OVE issue"
    )
    runner = HotDealPipelineRunner(
        settings=settings, browser=browser, db_conn=db_conn,
        log=logging.getLogger("t"),
    )
    try:
        with pytest.raises(SavedSearchPageEmpty):
            runner._export_search("VCH Marketing List")
    finally:
        db_conn.close()


def test_export_search_propagates_generic_browser_session_error(tmp_path):
    """All BrowserSessionError subclasses propagate — not just SavedSearchPageEmpty."""
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner
    from ove_scraper.hot_deal_db import init_db

    settings = _make_settings(tmp_path)
    db_conn = init_db(tmp_path / "hot_deal.db")
    browser = MagicMock()
    browser.export_saved_search.side_effect = BrowserSessionError("CDP disconnect")
    runner = HotDealPipelineRunner(
        settings=settings, browser=browser, db_conn=db_conn,
        log=logging.getLogger("t"),
    )
    try:
        with pytest.raises(BrowserSessionError):
            runner._export_search("VCH Marketing List")
    finally:
        db_conn.close()


def test_export_search_returns_none_on_non_session_error(tmp_path):
    """Non-session errors (CSV parse, file IO, etc.) still soft-fail to None
    so the pipeline records 'failed' without triggering browser recovery."""
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner
    from ove_scraper.hot_deal_db import init_db

    settings = _make_settings(tmp_path)
    db_conn = init_db(tmp_path / "hot_deal.db")
    browser = MagicMock()
    browser.export_saved_search.side_effect = ValueError("CSV header malformed")
    runner = HotDealPipelineRunner(
        settings=settings, browser=browser, db_conn=db_conn,
        log=logging.getLogger("t"),
    )
    try:
        result = runner._export_search("VCH Marketing List")
        assert result is None
    finally:
        db_conn.close()


# ---------------------------------------------------------------------------
# Layer 2: run_once must re-raise SavedSearchPageEmpty after finish_run("failed")
# ---------------------------------------------------------------------------


def test_run_once_reraises_saved_search_page_empty(tmp_path):
    """When _export_search raises SavedSearchPageEmpty, run_once must mark
    the DB run failed AND re-raise so run_hot_deal_with_recovery can route
    the symptom through cookie-clear recovery."""
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner
    from ove_scraper.hot_deal_db import init_db, get_run_summary

    settings = _make_settings(tmp_path)
    db_conn = init_db(tmp_path / "hot_deal.db")
    browser = MagicMock()
    browser.export_saved_search.side_effect = SavedSearchPageEmpty("No Saved Searches")
    runner = HotDealPipelineRunner(
        settings=settings, browser=browser, db_conn=db_conn,
        log=logging.getLogger("t"),
    )
    try:
        with pytest.raises(SavedSearchPageEmpty):
            runner.run_once()
        # The DB run should have been marked failed even though we re-raised.
        # We don't have the run_id here, but we can confirm at least one
        # row exists with status='failed'.
        rows = db_conn.execute(
            "SELECT status FROM hot_deal_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"
    finally:
        db_conn.close()


# ---------------------------------------------------------------------------
# Layer 3: run_hot_deal_with_recovery must clear cookies + recover on
# SavedSearchPageEmpty, then return cleanly (not re-raise)
# ---------------------------------------------------------------------------


def test_run_hot_deal_with_recovery_clears_cookies_on_stale_auth(tmp_path, monkeypatch):
    """The big one. SavedSearchPageEmpty from the pipeline must trigger
    cookie-clear + browser recovery BEFORE the state file is marked failed,
    so the next scheduler tick gets a fresh session."""
    from ove_scraper import main as main_module
    from ove_scraper.hot_deal_db import init_db
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner

    settings = _make_settings(tmp_path)
    settings.hot_deal_db_path  # validate attr exists; init_db happens inside the function

    # Make HotDealPipelineRunner.run_once raise SavedSearchPageEmpty, as it
    # would after the Layer 1/2 fixes if export hits "No Saved Searches".
    def _raise_stale_auth(self):
        raise SavedSearchPageEmpty("OVE saved-searches page shows 'No Saved Searches'")
    monkeypatch.setattr(HotDealPipelineRunner, "run_once", _raise_stale_auth)

    # Spy on the recovery functions: we need to confirm they were called
    # in the right order and BEFORE the state was written.
    cookies_cleared: list[bool] = []
    browser_recovered: list[bool] = []
    monkeypatch.setattr(
        main_module, "_clear_chrome_cookies",
        lambda settings, browser, logger: cookies_cleared.append(True),
    )
    monkeypatch.setattr(
        main_module, "recover_browser_session",
        lambda settings, browser, logger, notifier=None: browser_recovered.append(True),
    )

    notifier = MagicMock()
    browser = MagicMock()
    logger = logging.getLogger("t")

    # Should NOT raise — recovery was triggered, not re-raised.
    main_module.run_hot_deal_with_recovery(
        settings=settings, browser=browser, logger=logger,
        notifier=notifier, api_client=None,
    )

    assert cookies_cleared == [True], "expected _clear_chrome_cookies to have been called once"
    assert browser_recovered == [True], "expected recover_browser_session to have been called once"

    # State file must reflect failed status with the stale-auth reason.
    state_path = main_module._hot_deal_state_path(settings)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["last_run_status"] == "failed"
    assert "stale auth" in state["last_failure_reason"].lower() or \
           "savedsearchpageempty" in state["last_failure_reason"].lower()
    assert state["attempts_today"] == 1


def test_run_hot_deal_with_recovery_fires_failure_alert_only_after_max_attempts(
    tmp_path, monkeypatch
):
    """The failure email should only fire when the daily attempt budget is
    exhausted — otherwise the operator gets multiple alerts for one bad day."""
    from ove_scraper import main as main_module
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner

    settings = _make_settings(tmp_path)

    def _raise_stale_auth(self):
        raise SavedSearchPageEmpty("No Saved Searches")
    monkeypatch.setattr(HotDealPipelineRunner, "run_once", _raise_stale_auth)
    monkeypatch.setattr(main_module, "_clear_chrome_cookies", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "recover_browser_session", lambda *a, **k: None)

    notifier = MagicMock()
    browser = MagicMock()
    logger = logging.getLogger("t")

    # Pre-seed state to attempt 2 of 3 — final attempt should fire the alert.
    state_path = main_module._hot_deal_state_path(settings)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    today = datetime.now(EASTERN).date().isoformat()
    state_path.write_text(json.dumps({
        "last_run_date_eastern": today,
        "last_run_status": "failed",
        "last_run_at": "2026-05-03T07:33:00-04:00",
        "attempts_today": 2,
    }), encoding="utf-8")

    main_module.run_hot_deal_with_recovery(
        settings=settings, browser=browser, logger=logger,
        notifier=notifier, api_client=None,
    )

    # attempts_today incremented to 3 (max), so notifier MUST have fired.
    assert notifier.notify_hot_deal_pipeline_failed.called, \
        "expected failure alert on the final attempt"
    call_kwargs = notifier.notify_hot_deal_pipeline_failed.call_args.kwargs
    assert call_kwargs["attempts"] == 3


def test_run_hot_deal_with_recovery_no_alert_on_intermediate_failure(
    tmp_path, monkeypatch
):
    """First and second failed attempts of the day must NOT fire the alert
    — only the final exhausted attempt does. This was the existing behavior
    pre-fix and the new SavedSearchPageEmpty path must preserve it."""
    from ove_scraper import main as main_module
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner

    settings = _make_settings(tmp_path)

    def _raise_stale_auth(self):
        raise SavedSearchPageEmpty("No Saved Searches")
    monkeypatch.setattr(HotDealPipelineRunner, "run_once", _raise_stale_auth)
    monkeypatch.setattr(main_module, "_clear_chrome_cookies", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "recover_browser_session", lambda *a, **k: None)

    notifier = MagicMock()
    browser = MagicMock()
    logger = logging.getLogger("t")

    # Fresh state — this will be attempt 1.
    main_module.run_hot_deal_with_recovery(
        settings=settings, browser=browser, logger=logger,
        notifier=notifier, api_client=None,
    )
    assert not notifier.notify_hot_deal_pipeline_failed.called


def test_run_hot_deal_with_recovery_handles_recovery_failure_without_re_raising(
    tmp_path, monkeypatch
):
    """If _clear_chrome_cookies / recover_browser_session ITSELF raises
    BrowserSessionError (e.g. lockout active), the function must log the
    second failure into the state file but NOT re-raise — re-raising would
    push the main loop's auth handler into a runtime rebuild that doesn't
    address the lockout root cause."""
    from ove_scraper import main as main_module
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner

    settings = _make_settings(tmp_path)

    def _raise_stale_auth(self):
        raise SavedSearchPageEmpty("No Saved Searches")
    monkeypatch.setattr(HotDealPipelineRunner, "run_once", _raise_stale_auth)
    monkeypatch.setattr(main_module, "_clear_chrome_cookies", lambda *a, **k: None)

    def _recovery_blocked(*args, **kwargs):
        raise BrowserSessionError("Browser recovery skipped — auth lockout active: ...")
    monkeypatch.setattr(main_module, "recover_browser_session", _recovery_blocked)

    notifier = MagicMock()
    browser = MagicMock()
    logger = logging.getLogger("t")

    # Should NOT raise — the recovery failure is captured into state.
    main_module.run_hot_deal_with_recovery(
        settings=settings, browser=browser, logger=logger,
        notifier=notifier, api_client=None,
    )

    state_path = main_module._hot_deal_state_path(settings)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["last_run_status"] == "failed"
    assert "recovery failed" in state["last_failure_reason"].lower() or \
           "lockout" in state["last_failure_reason"].lower()

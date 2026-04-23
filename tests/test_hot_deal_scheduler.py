"""Tests for the daily Hot Deal scheduler decision logic in main.py.

Exercises should_run_hot_deal_now() — the single source of truth for
"should the main loop fire the Hot Deal pipeline right now". Covers:
  - new-day / catch-up after downtime
  - one-run-per-Eastern-day invariant
  - stale "started" state recovery after a crash
  - retry budget + cooldown between attempts
  - IMS refresh window skip
  - pre-slot skip
"""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ove_scraper.config import Settings
from ove_scraper.main import should_run_hot_deal_now, _hot_deal_state_path


EASTERN = ZoneInfo("America/New_York")


@pytest.fixture()
def base_settings(tmp_path: Path) -> Settings:
    return Settings(
        vch_api_base_url="http://unused.test",
        vch_service_token="unused",
        artifact_dir=tmp_path,
        hot_deal_enabled=True,
        hot_deal_daily_schedule_eastern=((7, 0),),
        hot_deal_retry_delay_seconds=1800,
        hot_deal_max_daily_attempts=3,
        hot_deal_stale_start_seconds=7200,
        ims_refresh_start_hour_eastern=16,
        ims_refresh_end_hour_eastern=17,
    )


def _write_state(settings: Settings, state: dict) -> None:
    path = _hot_deal_state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


def test_before_slot_does_not_run(base_settings: Settings) -> None:
    now = datetime(2026, 4, 22, 6, 30, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert decision["reason"] == "before_first_slot_today"


def test_new_day_runs(base_settings: Settings) -> None:
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-21",
        "last_run_status": "completed",
        "last_run_at": "2026-04-21T07:05:00-04:00",
        "attempts_today": 1,
    })
    now = datetime(2026, 4, 22, 7, 15, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is True
    assert decision["reason"] == "new_day"


def test_already_completed_today_skips(base_settings: Settings) -> None:
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "completed",
        "last_run_at": "2026-04-22T07:45:00-04:00",
        "attempts_today": 1,
    })
    now = datetime(2026, 4, 22, 14, 0, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert decision["reason"] == "already_completed_today"


def test_catch_up_after_downtime(base_settings: Settings, tmp_path: Path) -> None:
    # Machine was off during the 7:00 slot; we boot up at 10:00 AM and
    # should catch up today. State is either empty or from yesterday.
    now = datetime(2026, 4, 22, 10, 0, tzinfo=EASTERN)
    # No state file at all
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is True
    assert decision["reason"] == "new_day"


def test_ims_refresh_window_skips(base_settings: Settings) -> None:
    # 4:30 PM ET — during the 16:00-17:00 IMS refresh window
    now = datetime(2026, 4, 22, 16, 30, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert decision["reason"] == "ims_refresh_window"


def test_run_in_progress_skips(base_settings: Settings) -> None:
    # status=started, last_run_at 30 min ago, well under the 2h stale threshold
    now = datetime(2026, 4, 22, 7, 30, tzinfo=EASTERN)
    started_at = (now - timedelta(minutes=30)).isoformat()
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "started",
        "last_run_at": started_at,
        "attempts_today": 1,
    })
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert decision["reason"].startswith("run_in_progress_")


def test_stale_started_reclaimed(base_settings: Settings) -> None:
    # status=started but last_run_at is 3h old, past 2h stale threshold
    now = datetime(2026, 4, 22, 12, 0, tzinfo=EASTERN)
    started_at = (now - timedelta(hours=3)).isoformat()
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "started",
        "last_run_at": started_at,
        "attempts_today": 1,
    })
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is True
    assert decision["reason"].startswith("stale_started_")


def test_failed_then_retry_cooldown(base_settings: Settings) -> None:
    # Failed 10 minutes ago; retry delay is 30 minutes -> still cooling down
    now = datetime(2026, 4, 22, 7, 40, tzinfo=EASTERN)
    failed_at = (now - timedelta(minutes=10)).isoformat()
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "failed",
        "last_run_at": failed_at,
        "attempts_today": 1,
        "last_failure_reason": "export timeout",
    })
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert "retry_cooldown_" in decision["reason"]


def test_failed_after_cooldown_retries(base_settings: Settings) -> None:
    # Failed 45 minutes ago; retry delay is 30 minutes -> cooldown cleared
    now = datetime(2026, 4, 22, 8, 0, tzinfo=EASTERN)
    failed_at = (now - timedelta(minutes=45)).isoformat()
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "failed",
        "last_run_at": failed_at,
        "attempts_today": 1,
        "last_failure_reason": "export timeout",
    })
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is True
    assert "retry_attempt_2_of_3" in decision["reason"]


def test_attempt_cap_reached_waits_for_tomorrow(base_settings: Settings) -> None:
    now = datetime(2026, 4, 22, 14, 0, tzinfo=EASTERN)
    failed_at = (now - timedelta(hours=1)).isoformat()
    _write_state(base_settings, {
        "last_run_date_eastern": "2026-04-22",
        "last_run_status": "failed",
        "last_run_at": failed_at,
        "attempts_today": 3,
        "last_failure_reason": "export timeout",
    })
    decision = should_run_hot_deal_now(base_settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert "daily_attempt_cap_reached" in decision["reason"]


def test_no_schedule_configured_skips(base_settings: Settings) -> None:
    settings = replace(base_settings, hot_deal_daily_schedule_eastern=())
    now = datetime(2026, 4, 22, 9, 0, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is False
    assert decision["reason"] == "no_schedule_configured"


def test_multiple_slots_first_past_slot_triggers(base_settings: Settings) -> None:
    # Two slots: 07:00 and 13:00. At 10:00 AM on a brand-new day the
    # 07:00 slot is past (eligible) and the 13:00 slot has not yet been
    # reached. Eligibility should come from the 07:00 past slot.
    settings = replace(base_settings, hot_deal_daily_schedule_eastern=((7, 0), (13, 0)))
    now = datetime(2026, 4, 22, 10, 0, tzinfo=EASTERN)
    decision = should_run_hot_deal_now(settings, logging.getLogger("t"), now=now)
    assert decision["should_run"] is True

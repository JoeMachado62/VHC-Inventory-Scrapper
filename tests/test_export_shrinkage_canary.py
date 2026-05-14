"""Tests for the export row-count delta canary (2026-05-06 fix).

Pre-fix: silent data-loss bugs went undetected. 2026-05-06 sync
exported East-Hub-2025-2026 with 4948 rows at 18:33, then 4305 rows
at 18:57 — losing 643 vehicles (including a specific BMW the operator
was tracking). No alert fired because the export "succeeded" at the
file-write level.

Post-fix: per-search history of recent row counts. After each export,
compare current count to the recent max for the same search. If the
new count is < 75% of recent max AND the absolute drop is > 100 rows,
fire EXPORT_SHRINKAGE_DETECTED log + admin alert.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


def _make_session(tmp_path: Path) -> PlaywrightCdpBrowserSession:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
    )
    return PlaywrightCdpBrowserSession(settings)


def _write_csv(path: Path, row_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["vin,year,make"]  # header
    for i in range(row_count):
        lines.append(f"VIN{i:013d},2025,BMW")
    path.write_text("\n".join(lines), encoding="utf-8")


def _read_history(session) -> dict:
    history_path = session.settings.artifact_dir / "_state" / "saved_search_export_history.json"
    if not history_path.exists():
        return {}
    return json.loads(history_path.read_text(encoding="utf-8"))


def test_first_export_establishes_baseline_no_alert(tmp_path, caplog):
    """First export for a search has no baseline; just record + log info."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "east-hub.csv"
    _write_csv(csv_path, 4948)

    with caplog.at_level(logging.INFO, logger="ove_scraper.cdp_browser"):
        session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    assert any(
        "baseline established" in rec.getMessage() and "4948" in rec.getMessage()
        for rec in caplog.records
    ), "first export should log baseline established"
    notifier.notify_export_shrinkage.assert_not_called()
    history = _read_history(session)
    assert "East-Hub-2025-2026" in history
    assert len(history["East-Hub-2025-2026"]) == 1
    assert history["East-Hub-2025-2026"][0]["row_count"] == 4948


def test_normal_churn_under_threshold_no_alert(tmp_path, caplog):
    """A modest drop (e.g. 4948 -> 4800, ~3%) should NOT alert — that's
    legitimate auction churn, not partial export."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "east-hub.csv"

    _write_csv(csv_path, 4948)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    _write_csv(csv_path, 4800)
    with caplog.at_level(logging.INFO, logger="ove_scraper.cdp_browser"):
        session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    assert not any(
        "EXPORT_SHRINKAGE_DETECTED" in rec.getMessage() for rec in caplog.records
    ), "normal churn must NOT trigger the canary"
    notifier.notify_export_shrinkage.assert_not_called()


def test_2026_05_06_actual_drop_does_not_trip_canary(tmp_path, caplog):
    """HONESTY TEST: documents what the canary CAN'T catch.

    The exact scenario that motivated this work was 4948 -> 4305 on
    East-Hub-2025-2026 between two consecutive syncs (a real partial-
    export bug that lost a specific VIN the operator was tracking).

    But that's only a 13% drop — ratio = 4305/4948 = 0.87, above the
    0.75 threshold. The canary will NOT fire on this case at default
    thresholds.

    This is a deliberate trade-off:
      - Lower threshold (e.g. 0.90) would catch this case but also
        produce false positives on normal hourly auction churn
        (vehicles selling/expiring throughout the day).
      - Current threshold catches catastrophic losses (>25% drop) which
        are unambiguously broken — partial exports of <70% of the
        recent max almost certainly indicate a render-timing failure.

    For the actual 4948->4305 drop, the more reliable detection is the
    'Vehicle results selector not found before export; proceeding
    anyway' WARNING that the export code already emits when the page
    didn't render in time. The 60s timeout fix (2026-05-06) addresses
    the root cause — this canary is the catastrophic-loss net.
    """
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "east-hub.csv"

    _write_csv(csv_path, 4948)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")
    _write_csv(csv_path, 4305)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    # 4305/4948 = 0.870 > 0.75 threshold, so NO alert. By design —
    # documented above.
    notifier.notify_export_shrinkage.assert_not_called()


def test_catastrophic_drop_fires_alert(tmp_path, caplog):
    """The canary catches catastrophic losses: e.g. 4948 -> 2000 (40% ratio)."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "east-hub.csv"

    _write_csv(csv_path, 4948)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    _write_csv(csv_path, 2000)
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    assert any(
        "EXPORT_SHRINKAGE_DETECTED" in rec.getMessage()
        and "current_rows=2000" in rec.getMessage()
        and "recent_max=4948" in rec.getMessage()
        for rec in caplog.records
    ), "catastrophic drop should emit EXPORT_SHRINKAGE_DETECTED"
    notifier.notify_export_shrinkage.assert_called_once()


def test_small_absolute_drop_does_not_alert_even_if_ratio_low(tmp_path, caplog):
    """A search with only 50 vehicles dropping to 30 is 60% ratio (BELOW
    threshold) BUT only 20 absolute rows lost — likely just churn, not
    a partial-export bug. The MIN_ABSOLUTE=100 guard prevents alerts on
    tiny searches."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "tiny-search.csv"

    _write_csv(csv_path, 50)
    session._record_export_count_and_check_canary(csv_path, "tiny-search")

    _write_csv(csv_path, 30)
    session._record_export_count_and_check_canary(csv_path, "tiny-search")

    # ratio=0.6 < 0.75 (would normally trip) but absolute_drop=20 < 100
    # (gate kicks in) → no alert
    notifier.notify_export_shrinkage.assert_not_called()


def test_canary_compares_against_recent_max_not_just_last(tmp_path, caplog):
    """If exports go 5000 -> 4900 -> 2000, the third must alert based on
    the 5000 recent max, not on the 4900 immediate predecessor."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_path = tmp_path / "exports" / "east-hub.csv"

    _write_csv(csv_path, 5000)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")
    _write_csv(csv_path, 4900)
    session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    notifier.notify_export_shrinkage.assert_not_called()  # 4900/5000 = 0.98 fine

    _write_csv(csv_path, 2000)
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session._record_export_count_and_check_canary(csv_path, "East-Hub-2025-2026")

    # 2000/5000 = 0.40 < 0.75 + drop=3000 > 100 → alert
    assert any(
        "EXPORT_SHRINKAGE_DETECTED" in rec.getMessage()
        and "recent_max=5000" in rec.getMessage()
        for rec in caplog.records
    )
    notifier.notify_export_shrinkage.assert_called_once()


def test_history_capped_at_max(tmp_path):
    """History must stay bounded so disk usage doesn't grow unbounded."""
    session = _make_session(tmp_path)
    csv_path = tmp_path / "exports" / "east-hub.csv"
    for i in range(20):
        _write_csv(csv_path, 5000 + i)
        session._record_export_count_and_check_canary(csv_path, "East-Hub")

    history = _read_history(session)
    assert len(history["East-Hub"]) <= 10, "history must be capped"


def test_per_search_isolation(tmp_path):
    """Shrinkage on search A must not affect baseline for search B."""
    session = _make_session(tmp_path)
    notifier = MagicMock()
    session.set_notifier(notifier)
    csv_a = tmp_path / "exports" / "search-a.csv"
    csv_b = tmp_path / "exports" / "search-b.csv"

    _write_csv(csv_a, 4000)
    session._record_export_count_and_check_canary(csv_a, "search-a")
    _write_csv(csv_b, 1000)
    session._record_export_count_and_check_canary(csv_b, "search-b")
    # search-b is first run, no baseline → no alert
    notifier.notify_export_shrinkage.assert_not_called()

    # search-a stays normal
    _write_csv(csv_a, 3950)
    session._record_export_count_and_check_canary(csv_a, "search-a")
    notifier.notify_export_shrinkage.assert_not_called()

    # search-b crashes — should fire based on its OWN history, not search-a's
    _write_csv(csv_b, 100)
    session._record_export_count_and_check_canary(csv_b, "search-b")
    notifier.notify_export_shrinkage.assert_called_once()
    call_kwargs = notifier.notify_export_shrinkage.call_args.kwargs
    assert call_kwargs["search_name"] == "search-b"

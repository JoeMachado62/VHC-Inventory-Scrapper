from __future__ import annotations

from ove_scraper.main import _is_process_state_error


def test_matches_classic_sync_inside_asyncio_loop():
    exc = RuntimeError(
        "Error: It looks like you are using Playwright Sync API "
        "inside the asyncio loop. Please use the Async API instead."
    )
    assert _is_process_state_error(exc) is True


def test_matches_short_form_sync_inside_asyncio_loop():
    # Defensive variant the function also accepts in case the leading
    # "Playwright" word is dropped by upstream wrapping.
    exc = RuntimeError("Sync API inside the asyncio loop")
    assert _is_process_state_error(exc) is True


def test_matches_future_attached_to_different_loop():
    # 2026-04-29 post-power-outage symptom of the same asyncio-state
    # pollution. Pre-fix this fell through as a "real" auth error,
    # which contributed to the Login B Chrome relaunch storm.
    exc = RuntimeError(
        "Page.goto: Task <Task pending name='Task-3864' "
        "coro=<Page.goto() running at ...> cb=[...]> got Future "
        "<Future pending> attached to a different loop"
    )
    assert _is_process_state_error(exc) is True


def test_does_not_match_real_auth_error():
    exc = RuntimeError("OVE session is not authenticated; browser is on the login page")
    assert _is_process_state_error(exc) is False


def test_does_not_match_network_error():
    exc = RuntimeError("Page.goto: net::ERR_ABORTED at https://www.ove.com/saved_searches#/")
    assert _is_process_state_error(exc) is False


def test_does_not_match_empty_string():
    exc = RuntimeError("")
    assert _is_process_state_error(exc) is False

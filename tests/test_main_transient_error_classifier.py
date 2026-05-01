"""Tests for Fix 3 (2026-04-30): the main loop's
_is_transient_per_operation_error classifier prevents transient
Playwright errors from triggering a full runtime rebuild.

The 22:12 incident root cause was main.py's bare `except Exception`
treating every non-BrowserSessionError exception as a "crash"
warranting a runtime rebuild. The rebuild re-attached to OVE,
re-triggered auth, and started the kill+relaunch loop that ended
with a Manheim SMS challenge.
"""
from __future__ import annotations

from ove_scraper.main import _is_transient_per_operation_error


# --- Positive cases: should be classified as transient ---


def test_target_page_closed_is_transient():
    exc = RuntimeError(
        "Page.goto: Target page, context or browser has been closed"
    )
    assert _is_transient_per_operation_error(exc) is True


def test_frame_detached_is_transient():
    exc = RuntimeError("Frame was detached")
    assert _is_transient_per_operation_error(exc) is True


def test_navigation_interrupted_is_transient():
    exc = RuntimeError(
        "Page.goto: Navigation interrupted by another navigation"
    )
    assert _is_transient_per_operation_error(exc) is True


def test_page_goto_timeout_is_transient():
    exc = RuntimeError("Page.goto: Timeout 30000ms exceeded")
    assert _is_transient_per_operation_error(exc) is True


def test_net_err_aborted_is_transient():
    exc = RuntimeError(
        "Page.goto: net::ERR_ABORTED at https://www.ove.com/saved_searches#/"
    )
    assert _is_transient_per_operation_error(exc) is True


def test_protocol_error_is_transient():
    exc = RuntimeError("Protocol error: Target.attachToTarget")
    assert _is_transient_per_operation_error(exc) is True


def test_browser_closed_is_transient():
    exc = RuntimeError("Browser has been closed")
    assert _is_transient_per_operation_error(exc) is True


def test_websocket_closed_is_transient():
    exc = RuntimeError("WebSocket is not open")
    assert _is_transient_per_operation_error(exc) is True


# --- Negative cases: should NOT be classified as transient ---


def test_real_attribute_error_not_transient():
    """Genuine bugs should still trigger a rebuild. AttributeError
    on internal code is NOT a Chrome blip."""
    exc = AttributeError("'NoneType' object has no attribute 'foo'")
    assert _is_transient_per_operation_error(exc) is False


def test_value_error_not_transient():
    exc = ValueError("Could not parse VIN")
    assert _is_transient_per_operation_error(exc) is False


def test_runtime_error_with_unrelated_message_not_transient():
    exc = RuntimeError("Something else entirely went wrong")
    assert _is_transient_per_operation_error(exc) is False


def test_asyncio_loop_error_not_transient():
    """Asyncio loop pollution is matched by _is_process_state_error,
    not _is_transient_per_operation_error. They route differently:
    asyncio errors require a Python restart, transient errors only
    need a per-tick log+continue."""
    exc = RuntimeError("Sync API inside the asyncio loop")
    assert _is_transient_per_operation_error(exc) is False

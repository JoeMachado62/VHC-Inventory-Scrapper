"""Tests for Fix 1 (2026-04-30): deep-scrape worker must catch
playwright.sync_api.Error and route it through the standard /fail
flow, not let it leak past the worker into main.py's bare except
(which used to rebuild the runtime — see the 22:12 incident).
"""
from __future__ import annotations

import logging

from playwright.sync_api import (
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
)

from ove_scraper.deep_scrape import DeepScrapeWorker
from ove_scraper.config import Settings
from ove_scraper.schemas import PendingDetailRequest


def _make_settings(tmp_path):
    return Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
    )


class _NoopApi:
    pass


class _NoopBrowser:
    pass


def test_classify_failure_routes_target_closed_to_browser_error(tmp_path):
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightError(
        "Page.goto: Target page, context or browser has been closed"
    )
    assert worker._classify_failure(exc) == "browser_error"


def test_classify_failure_routes_frame_detached_to_browser_error(tmp_path):
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightError("Frame was detached")
    assert worker._classify_failure(exc) == "browser_error"


def test_classify_failure_routes_browser_closed_to_browser_error(tmp_path):
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightError("browser has been closed")
    assert worker._classify_failure(exc) == "browser_error"


def test_classify_failure_routes_net_err_to_transient_network(tmp_path):
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightError("Page.goto: net::ERR_ABORTED at https://www.ove.com/")
    assert worker._classify_failure(exc) == "transient_network"


def test_classify_failure_routes_playwright_timeout_to_transient_network(tmp_path):
    """PlaywrightTimeoutError is a subclass of PlaywrightError, so the
    isinstance order in _classify_failure matters. Verify the more
    specific class is checked first."""
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightTimeoutError("Page.goto: Timeout 30000ms exceeded")
    assert worker._classify_failure(exc) == "transient_network"


def test_classify_failure_routes_unknown_playwright_error_to_browser_error(tmp_path):
    """Default routing for un-recognized Playwright error messages.
    Better to surface as browser_error (gets retried via /fail) than
    fall through to transient_network."""
    worker = DeepScrapeWorker(_NoopApi(), _NoopBrowser(), logging.getLogger("test"), _make_settings(tmp_path))
    exc = PlaywrightError("Some new Playwright error message we haven't seen before")
    assert worker._classify_failure(exc) == "browser_error"

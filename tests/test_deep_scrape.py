from __future__ import annotations

import logging

from ove_scraper.api_client import ApiClientError
from ove_scraper.browser import BrowserSessionError, ListingNotFoundError
from ove_scraper.cdp_browser import unique_urls
from ove_scraper.deep_scrape import DeepScrapeWorker
from ove_scraper.config import Settings
from ove_scraper.resource_utils import SystemResources, recommend_deep_scrape_workers
from ove_scraper.schemas import PendingDetailRequest


class FakeApiClient:
    def __init__(self, requests):
        self._requests = requests
        self.detail_pushes = 0
        self.completions = []
        self.failures = []
        self.terminals = []

    def claim_pending_detail_requests(self, *, worker_id, limit=1, lease_seconds=900):
        return list(self._requests)

    def push_ove_detail(self, vin, payload):
        self.detail_pushes += 1
        return {"data": {"vin": vin}}

    def complete_detail_request(self, request_id, *, worker_id, result="success"):
        self.completions.append((request_id, worker_id, result))
        return {"data": {"request_id": request_id, "status": "completed"}}

    def fail_detail_request(self, request_id, *, worker_id, error_category, error_message, retry_after_seconds):
        self.failures.append((request_id, worker_id, error_category, retry_after_seconds))
        return {"data": {"request_id": request_id, "status": "failed"}}

    def terminal_detail_request(self, request_id, *, worker_id, reason, message):
        self.terminals.append((request_id, worker_id, reason))
        return {"data": {"request_id": request_id, "status": "terminal"}}

    def heartbeat_detail_request(self, request_id, *, worker_id, lease_seconds):
        return {"data": {"request_id": request_id, "lease_expires_at": "2026-04-07T00:10:00+00:00"}}


class Terminal404ApiClient(FakeApiClient):
    def push_ove_detail(self, vin, payload):
        self.detail_pushes += 1
        raise ApiClientError('VCH API rejected request with status 404: {"detail":"Vehicle not found"}')


class PassiveBrowser:
    def deep_scrape_vin(self, vin: str):
        raise AssertionError("This browser should not be called in this test")


class MissingVinBrowser:
    def __init__(self) -> None:
        self.calls = 0

    def deep_scrape_vin(self, vin: str):
        self.calls += 1
        raise ListingNotFoundError(f"VIN {vin} is not available in OVE search results")


class BrokenBrowser:
    def __init__(self) -> None:
        self.calls = 0

    def deep_scrape_vin(self, vin: str):
        self.calls += 1
        raise BrowserSessionError(f"Unable to open detail page for VIN {vin}")


def make_request(vin: str, *, request_id: str | None = None) -> PendingDetailRequest:
    return PendingDetailRequest.model_validate(
        {
            "request_id": request_id or f"req-{vin}",
            "vin": vin,
            "source_platform": "manheim",
            "status": "PENDING",
            "priority": 100,
            "attempts": 0,
            "requested_at": "2026-03-23T00:00:00+00:00",
            "request_source": "test",
            "requested_by": "pytest",
            "reason": None,
            "metadata": {},
        }
    )


def test_recommend_deep_scrape_workers_is_conservative_for_16gb_8thread_machine() -> None:
    workers = recommend_deep_scrape_workers(
        SystemResources(
            logical_processors=8,
            total_memory_bytes=16 * 1024**3,
        )
    )
    assert workers == 5


def test_process_pending_once_dedupes_request_ids_and_forces_sequential(monkeypatch, tmp_path) -> None:
    request_one = make_request("VIN1", request_id="same-request")
    request_duplicate = make_request("VIN1", request_id="same-request")
    request_two = make_request("VIN2", request_id="different-request")
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        deep_scrape_max_workers=3,
        scraper_node_id="node1",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
    )
    worker = DeepScrapeWorker(
        FakeApiClient([request_one, request_duplicate, request_two]),
        PassiveBrowser(),
        logging.getLogger("test"),
        settings,
    )
    seen: list[str] = []

    def fake_process_request(request, browser, api_client):
        seen.append(request.request_id)
        return request.vin

    monkeypatch.setattr(worker, "_process_request", fake_process_request)

    processed = sorted(worker.process_pending_once())

    assert processed == ["VIN1", "VIN2"]
    assert seen == ["same-request", "different-request"]


def test_browser_failures_enter_cooldown_and_are_skipped(tmp_path) -> None:
    request = make_request("VIN1", request_id="cooldown-request")
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        scraper_node_id="node1",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
    )
    browser = BrokenBrowser()
    worker = DeepScrapeWorker(FakeApiClient([request]), browser, logging.getLogger("test"), settings)

    first = worker.process_pending_once()
    second = worker.process_pending_once()

    assert first == []
    assert second == []
    assert browser.calls == 1
    assert worker.api_client.failures == [("cooldown-request", settings.detail_worker_id, "browser_error", 300)]


def test_terminal_404_request_is_suppressed_after_finalize_failure(tmp_path) -> None:
    request = make_request("VIN1", request_id="terminal-request")
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        scraper_node_id="node1",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
        not_found_confirm_attempts=1,
    )
    browser = MissingVinBrowser()
    api_client = Terminal404ApiClient([request])
    worker = DeepScrapeWorker(api_client, browser, logging.getLogger("test"), settings)
    worker._is_ims_refresh_window = lambda now: False  # type: ignore[method-assign]

    first = worker.process_pending_once()
    second = worker.process_pending_once()

    assert first == ["VIN1"]
    assert second == []
    assert browser.calls == 1
    assert api_client.detail_pushes == 1
    assert api_client.terminals == [("terminal-request", settings.detail_worker_id, "vehicle_missing_on_vps")]


def test_not_found_attempts_reset_when_request_id_changes(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        scraper_node_id="node1",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
        not_found_confirm_attempts=3,
    )
    api_client = FakeApiClient([])
    worker = DeepScrapeWorker(api_client, MissingVinBrowser(), logging.getLogger("test"), settings)
    worker._is_ims_refresh_window = lambda now: False  # type: ignore[method-assign]

    first_request = make_request("VIN1", request_id="request-a")
    second_request = make_request("VIN1", request_id="request-b")

    first = worker._process_request(first_request, worker.browser, api_client)
    second = worker._process_request(second_request, worker.browser, api_client)

    assert first is None
    assert second is None
    tracker = worker._load_not_found_tracker()
    assert tracker["VIN1"]["request_id"] == "request-b"
    assert tracker["VIN1"]["attempt_count"] == 1


def test_successful_detail_push_completes_claim(tmp_path) -> None:
    request = make_request("VIN1", request_id="complete-request")
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        scraper_node_id="node1",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
    )
    api_client = FakeApiClient([])
    worker = DeepScrapeWorker(api_client, MissingVinBrowser(), logging.getLogger("test"), settings)

    monkey_payload = {
        "source_platform": "manheim",
        "images": [],
        "condition_report": None,
        "seller_comments": None,
        "listing_snapshot": None,
        "sync_metadata": {"request_id": request.request_id},
    }

    class SuccessBrowser:
        def deep_scrape_vin(self, vin: str):
            from ove_scraper.browser import DeepScrapeResult

            return DeepScrapeResult()

    worker = DeepScrapeWorker(api_client, SuccessBrowser(), logging.getLogger("test"), settings)

    from ove_scraper import deep_scrape as deep_scrape_module

    original_redact = deep_scrape_module.redact_detail
    deep_scrape_module.redact_detail = lambda detail, req, s: monkey_payload
    try:
        result = worker._process_request(request, worker.browser, api_client)
    finally:
        deep_scrape_module.redact_detail = original_redact

    assert result == "VIN1"
    assert api_client.completions == [("complete-request", settings.detail_worker_id, "success")]


def test_unique_urls_filters_out_non_vehicle_assets() -> None:
    urls = [
        "https://www.ove.com/assets/ove/header/hdr_logo-b74195.gif",
        "https://images.cdn.manheim.com/example-vehicle.jpg?size=w344h256",
        "https://images.cdn.manheim.com/example-vehicle.jpg",
        "https://strike-assets.manheim.com/build/images/autocheck_vertical.svg",
        "https://strike-assets.manheim.com/build/images/greenCheck.svg",
        "https://images.cdn.manheim.com/example-vehicle.jpg?size=w344h256",
        "https://images.cdn.manheim.com/example-vehicle-2.png",
    ]

    assert unique_urls(urls) == [
        "https://images.cdn.manheim.com/example-vehicle.jpg",
        "https://images.cdn.manheim.com/example-vehicle-2.png",
    ]

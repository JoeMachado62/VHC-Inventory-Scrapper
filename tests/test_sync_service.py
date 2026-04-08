from __future__ import annotations

import logging
from datetime import datetime

from ove_scraper.browser import BrowserSession, BrowserSessionError, ListingNotFoundError
from ove_scraper.deep_scrape import DeepScrapeWorker
from ove_scraper.config import Settings
from ove_scraper.main import EASTERN_TZ, is_within_sync_window, seconds_until_next_sync_window
from ove_scraper.sync_service import HourlySyncRunner


class FakeBrowser(BrowserSession):
    def __init__(self, saved_searches=()) -> None:
        self.saved_searches = tuple(saved_searches)

    def ensure_session(self):
        raise AssertionError("should not be called in this test")

    def list_saved_searches(self):
        return self.saved_searches

    def export_saved_search(self, search_name, export_dir):
        raise AssertionError("should not be called in this test")

    def deep_scrape_vin(self, vin):
        raise AssertionError("should not be called in this test")


class MissingVinBrowser(BrowserSession):
    def ensure_session(self):
        return None

    def list_saved_searches(self):
        return ()

    def export_saved_search(self, search_name, export_dir):
        raise AssertionError("should not be called in this test")

    def deep_scrape_vin(self, vin):
        raise ListingNotFoundError(f"VIN {vin} is not available in OVE search results")


class FakeApiClient:
    def __init__(self) -> None:
        self.calls = 0
        self.detail_pushes = 0
        self.last_ingest_vehicles = None
        self.last_sync_metadata = None

    def check_health(self) -> bool:
        return True

    def push_ove_ingest(self, vehicles, sync_metadata):
        self.calls += 1
        self.last_ingest_vehicles = vehicles
        self.last_sync_metadata = sync_metadata
        return {
            "status": "ok",
            "data": {
                "inserted": len(vehicles),
                "updated": 0,
                "skipped_priority": 0,
            },
        }

    def push_ove_detail(self, vin, payload):
        self.detail_pushes += 1
        return {"status": "ok", "data": {"vin": vin, "detail_saved": True}}


class DummyVehicle:
    def __init__(self, vin: str) -> None:
        self.vin = vin

    def model_dump(self, mode="json"):
        return {
            "vin": self.vin,
            "year": 2021,
            "make": "Honda",
            "model": "Accord",
            "price_asking": 100.0,
            "source_type": "ove",
            "source_platform": "manheim",
        }


class DummyTransform:
    def __init__(self, count: int) -> None:
        self.vehicles = [DummyVehicle(f"VIN{index:014d}") for index in range(count)]


class DummyPendingRequest:
    def __init__(self, vin: str = "19XFL2G86RE014238") -> None:
        self.request_id = "req-1"
        self.vin = vin
        self.source_platform = "manheim"
        self.status = "PENDING"
        self.priority = 100
        self.attempts = 0
        self.requested_at = __import__("datetime").datetime.fromisoformat("2026-03-23T12:00:00+00:00")
        self.last_polled_at = None
        self.request_source = "test"
        self.requested_by = "test"
        self.reason = "test"
        self.metadata = {}


class PartialExportBrowser(FakeBrowser):
    def __init__(self, exported_rows_by_search, failing_searches=(), saved_searches=()) -> None:
        super().__init__(saved_searches=saved_searches)
        self.exported_rows_by_search = exported_rows_by_search
        self.failing_searches = set(failing_searches)

    def export_saved_search(self, search_name, export_dir):
        if search_name in self.failing_searches:
            raise BrowserSessionError(f"Unable to locate saved search '{search_name}'")
        path = export_dir / f"{search_name}.csv"
        rows = self.exported_rows_by_search.get(search_name, [])
        if rows and isinstance(rows[0], dict):
            headers = list(rows[0].keys())
            body = ",".join(headers) + "\n"
            body += "".join(",".join(str(row.get(header, "")) for header in headers) + "\n" for row in rows)
        else:
            body = "VIN,Year,Make,Model\n" + "".join(",".join(row) + "\n" for row in rows)
        path.write_text(body, encoding="utf-8")
        return path


def test_push_snapshot_uses_single_request_even_when_over_500(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
    )
    api_client = FakeApiClient()
    runner = HourlySyncRunner(settings, FakeBrowser(), api_client, logging.getLogger("test"))

    result = runner.push_snapshot(
        DummyTransform(501),
        {
            "east_hub_record_count": 0,
            "west_hub_record_count": 0,
            "duplicates_removed": 0,
            "skipped_no_vin": 0,
            "scraper_node_id": "node",
            "scraper_version": "0.1.0",
        },
    )

    assert api_client.calls == 1
    assert result.db_records_added == 501


def test_resolve_saved_searches_prefers_live_discovery(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
        ove_east_searches=("Old East",),
        ove_west_searches=("Old West",),
    )
    browser = FakeBrowser(saved_searches=("East Hub 2022", "West Hub 2024-2026"))
    runner = HourlySyncRunner(settings, browser, FakeApiClient(), logging.getLogger("test"))

    result = runner.resolve_saved_searches()

    assert result == ("East Hub 2022", "West Hub 2024-2026")


def test_resolve_saved_searches_falls_back_to_configured_names(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
        ove_east_searches=("East Hub 2025-2026",),
        ove_west_searches=("West Hub 2024-2026",),
    )
    browser = FakeBrowser(saved_searches=())
    runner = HourlySyncRunner(settings, browser, FakeApiClient(), logging.getLogger("test"))

    result = runner.resolve_saved_searches()

    assert result == ("East Hub 2025-2026", "West Hub 2024-2026")


def test_export_search_group_skips_missing_saved_searches(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
    )
    browser = PartialExportBrowser(
        exported_rows_by_search={
            "East Hub 2022-2024": [("1FTFW1E50PFA84928", "2023", "Ford", "F-150")],
        },
        failing_searches={"East Hub 2024 or Newer"},
    )
    runner = HourlySyncRunner(settings, browser, FakeApiClient(), logging.getLogger("test"))

    try:
        runner.export_search_group(("East Hub 2022-2024", "East Hub 2024 or Newer"))
    except BrowserSessionError as exc:
        assert str(exc) == "Unable to locate saved search 'East Hub 2024 or Newer'"
    else:
        raise AssertionError("expected BrowserSessionError")


def test_run_once_fails_when_any_saved_search_is_missing(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
        ove_required_search_count=4,
        ove_east_searches=("East Hub 2022-2024", "East Hub 2024 or Newer"),
        ove_west_searches=("West Hub 2015 - 2021", "West Hub 2015-2023"),
    )
    browser = PartialExportBrowser(
        exported_rows_by_search={
            "East Hub 2022-2024": [
                {
                    "VIN": "1FTFW1E50PFA84928",
                    "Year": "2023",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "12345",
                }
            ],
            "East Hub 2024 or Newer": [
                {
                    "VIN": "1FTFW1E50PFA84929",
                    "Year": "2024",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "22345",
                }
            ],
            "West Hub 2015 - 2021": [
                {
                    "VIN": "1FTFW1E50PFA84930",
                    "Year": "2021",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "32345",
                }
            ],
        },
        failing_searches={"West Hub 2015-2023"},
        saved_searches=("East Hub 2022-2024", "East Hub 2024 or Newer", "West Hub 2015 - 2021", "West Hub 2015-2023"),
    )
    api_client = FakeApiClient()
    runner = HourlySyncRunner(settings, browser, api_client, logging.getLogger("test"))

    result = runner.run_once()

    assert result.execution_status == "Failure"
    assert result.api_push_status == "Failure"
    assert any("Unable to locate saved search 'West Hub 2015-2023'" in detail for detail in result.error_details)
    assert api_client.calls == 0


def test_run_once_fails_when_discovered_search_count_is_incomplete(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
        ove_required_search_count=6,
    )
    api_client = FakeApiClient()
    runner = HourlySyncRunner(
        settings,
        FakeBrowser(saved_searches=("East Hub 2022-2024", "West Hub 2024 or Newer")),
        api_client,
        logging.getLogger("test"),
    )

    result = runner.run_once()

    assert result.execution_status == "Failure"
    assert api_client.calls == 0
    assert any("Expected 6 saved searches" in detail for detail in result.error_details)


def test_run_once_uploads_complete_deduped_snapshot_in_single_batch(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        export_dir=tmp_path,
        log_file_path=tmp_path / "sync.log",
        data_dir=tmp_path / "data",
        ove_required_search_count=6,
    )
    saved_searches = (
        "East Hub 2022-2024",
        "East Hub 2024 or Newer",
        "West Hub 2015 - 2021",
        "West Hub 2015-2023",
        "West Hub 2022-2024",
        "West Hub 2024 or Newer",
    )
    browser = PartialExportBrowser(
        exported_rows_by_search={
            "East Hub 2022-2024": [
                {
                    "VIN": "1FTFW1E50PFA84928",
                    "Year": "2023",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "30000",
                    "Last Updated": "2026-04-07T12:00:00+0000",
                    "Status": "Live",
                }
            ],
            "East Hub 2024 or Newer": [
                {
                    "VIN": "1FTFW1E50PFA84928",
                    "Year": "2023",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "31000",
                    "Last Updated": "2026-04-07T13:00:00+0000",
                    "Status": "Live",
                }
            ],
            "West Hub 2015 - 2021": [
                {
                    "VIN": "1FTFW1E51PFA84928",
                    "Year": "2020",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "25000",
                    "Last Updated": "2026-04-07T12:00:00+0000",
                    "Status": "Live",
                }
            ],
            "West Hub 2015-2023": [
                {
                    "VIN": "1FTFW1E52PFA84928",
                    "Year": "2021",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "26000",
                    "Last Updated": "2026-04-07T12:00:00+0000",
                    "Status": "Live",
                }
            ],
            "West Hub 2022-2024": [
                {
                    "VIN": "1FTFW1E53PFA84928",
                    "Year": "2024",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "32000",
                    "Last Updated": "2026-04-07T12:00:00+0000",
                    "Status": "Live",
                }
            ],
            "West Hub 2024 or Newer": [
                {
                    "VIN": "1FTFW1E54PFA84928",
                    "Year": "2025",
                    "Make": "Ford",
                    "Model": "F-150",
                    "Asking Price": "33000",
                    "Last Updated": "2026-04-07T12:00:00+0000",
                    "Status": "Live",
                }
            ],
        },
        saved_searches=saved_searches,
    )
    api_client = FakeApiClient()
    runner = HourlySyncRunner(settings, browser, api_client, logging.getLogger("test"))

    result = runner.run_once()

    assert result.execution_status == "Success"
    assert result.api_push_status == "Success"
    assert api_client.calls == 1
    assert len(api_client.last_ingest_vehicles) == 5
    assert api_client.last_sync_metadata["verified_complete_snapshot"] is True
    assert api_client.last_sync_metadata["upload_mode"] == "single_batch_replace"
    assert api_client.last_sync_metadata["completed_saved_search_count"] == 6


def test_not_found_is_deferred_during_ims_window(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
    )
    api_client = FakeApiClient()
    worker = DeepScrapeWorker(api_client, MissingVinBrowser(), logging.getLogger("test"), settings)
    worker._is_ims_refresh_window = lambda now: True  # type: ignore[method-assign]

    result = worker._process_request(DummyPendingRequest(), worker.browser, api_client)

    assert result is None
    assert api_client.detail_pushes == 0
    assert (settings.artifact_dir / "19XFL2G86RE014238" / "availability-audit.json").exists()


def test_not_found_requires_multiple_misses_before_finalize(tmp_path) -> None:
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
        not_found_confirm_attempts=3,
    )
    api_client = FakeApiClient()
    worker = DeepScrapeWorker(api_client, MissingVinBrowser(), logging.getLogger("test"), settings)
    worker._is_ims_refresh_window = lambda now: False  # type: ignore[method-assign]
    request = DummyPendingRequest()

    first = worker._process_request(request, worker.browser, api_client)
    second = worker._process_request(request, worker.browser, api_client)
    third = worker._process_request(request, worker.browser, api_client)

    assert first is None
    assert second is None
    assert third == request.vin
    assert api_client.detail_pushes == 1


def test_sync_window_allows_daytime_hours(tmp_path) -> None:
    # Default sync window is 6 AM – 11 PM ET (extended from 9–21 per the
    # 2026-04-08 VPS handoff, item VPS-5).
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        log_file_path=tmp_path / "sync.log",
    )

    allowed_morning = is_within_sync_window(settings, datetime(2026, 3, 23, 7, 0, tzinfo=EASTERN_TZ))
    allowed_evening = is_within_sync_window(settings, datetime(2026, 3, 23, 22, 0, tzinfo=EASTERN_TZ))
    paused = is_within_sync_window(settings, datetime(2026, 3, 23, 23, 30, tzinfo=EASTERN_TZ))

    assert allowed_morning is True
    assert allowed_evening is True
    assert paused is False


def test_seconds_until_next_sync_window_targets_next_morning(tmp_path) -> None:
    # Window is 6 AM – 11 PM ET. From 23:30 ET, the next 6 AM is 6.5 hours
    # away → 23400 seconds.
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        log_file_path=tmp_path / "sync.log",
    )

    seconds = seconds_until_next_sync_window(
        settings,
        datetime(2026, 3, 23, 23, 30, tzinfo=EASTERN_TZ),
    )

    assert seconds == 23400.0

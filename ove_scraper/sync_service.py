from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import re

from ove_scraper.api_client import ApiClientError, VCHApiClient
from ove_scraper.browser import BrowserSession, BrowserSessionError, SavedSearchPageEmpty
from ove_scraper.config import Settings
from ove_scraper.csv_transform import TransformResult, load_csv_rows, transform_rows
from ove_scraper.logging_utils import append_sync_log
from ove_scraper.notifier import AdminNotifier
from ove_scraper.schemas import SyncExecutionLog


def _normalize_search_name(value: str) -> str:
    """Strip non-alphanumeric chars and lowercase so minor name variations
    (spacing around dashes, extra whitespace, punctuation changes) won't
    break the configured-vs-discovered matching.

    Examples:
        "West Hub 2022-2024"   -> "westhub20222024"
        "West Hub 2022 - 2024" -> "westhub20222024"
    """
    return re.sub(r"[^a-z0-9]+", "", value.lower())


@dataclass(slots=True)
class BatchResult:
    responses: list[dict[str, Any]]
    db_records_added: int = 0
    db_records_updated: int = 0
    db_records_skipped_priority: int = 0


@dataclass(slots=True)
class ExportGroupResult:
    rows: list[dict[str, str]]
    total_count: int
    completed_searches: tuple[str, ...]


class SnapshotSafetyGateError(ValueError):
    """Raised when the merged snapshot is too small to safely replace the
    last successful snapshot. Carries the row counts so the caller / notifier
    can render a useful message."""

    def __init__(self, *, proposed: int, last: int, threshold_pct: int) -> None:
        self.proposed = proposed
        self.last = last
        self.threshold_pct = threshold_pct
        minimum = int(last * threshold_pct / 100)
        super().__init__(
            f"Snapshot safety gate blocked push: proposed={proposed} rows, "
            f"last_successful={last} rows, required>={minimum} ({threshold_pct}% of last)"
        )


class HourlySyncRunner:
    def __init__(
        self,
        settings: Settings,
        browser: BrowserSession,
        api_client: VCHApiClient,
        logger: logging.Logger,
        notifier: AdminNotifier | None = None,
    ) -> None:
        self.settings = settings
        self.browser = browser
        self.api_client = api_client
        self.logger = logger
        self.notifier = notifier

    def run_once(self) -> SyncExecutionLog:
        execution_log = SyncExecutionLog(execution_status="Failure")
        try:
            if not self.api_client.check_health():
                raise ApiClientError("VCH API unreachable")

            # Discover ALL saved searches on OVE, exclude only the ones
            # that belong to other pipelines (e.g. Hot Deal), then export
            # everything that remains.  This is name-agnostic: adding,
            # removing, or renaming searches on OVE won't break the sync
            # — the 75% safety gate is the real correctness firewall.
            discovered = self.resolve_saved_searches()

            # Exclude searches that feed other pipelines (Hot Deal, etc.)
            exclude_keys = {
                _normalize_search_name(n)
                for n in getattr(self.settings, "hot_deal_searches", ())
            }
            inventory_searches = tuple(
                name for name in discovered
                if _normalize_search_name(name) not in exclude_keys
            )
            excluded = [n for n in discovered if _normalize_search_name(n) in exclude_keys]
            if excluded:
                self.logger.info(
                    "Excluding %d non-inventory searches: %s",
                    len(excluded), ", ".join(excluded),
                )

            if not inventory_searches:
                raise BrowserSessionError(
                    f"No inventory searches found after excluding {len(excluded)} "
                    f"non-inventory searches from {len(discovered)} discovered"
                )

            # Classify into east/west by name for metadata logging.
            east_searches = tuple(n for n in inventory_searches if "east" in n.lower())
            west_searches = tuple(n for n in inventory_searches if "west" in n.lower())
            unclassified = [
                n for n in inventory_searches
                if "east" not in n.lower() and "west" not in n.lower()
            ]
            if unclassified:
                # Searches that don't match east/west still get exported
                # — just lump them into west for metadata purposes.
                self.logger.info(
                    "Unclassified searches (adding to west group): %s",
                    ", ".join(unclassified),
                )
                west_searches = west_searches + tuple(unclassified)

            self.logger.info(
                "Inventory sync: %d searches (%d east, %d west) from %d discovered",
                len(inventory_searches), len(east_searches), len(west_searches),
                len(discovered),
            )

            east_export = self.export_search_group(east_searches)
            west_export = self.export_search_group(west_searches)
            completed_searches = east_export.completed_searches + west_export.completed_searches
            all_required = east_searches + west_searches
            missing_searches = [name for name in all_required if name not in completed_searches]
            if missing_searches:
                raise BrowserSessionError(
                    "Saved-search export incomplete; missing "
                    f"{len(missing_searches)} of {len(all_required)} searches: {', '.join(missing_searches)}"
                )

            east_rows, east_count = east_export.rows, east_export.total_count
            west_rows, west_count = west_export.rows, west_export.total_count
            transformed = transform_rows(
                east_rows + west_rows,
                source_platform=self.settings.ove_source_platform,
            )

            execution_log.east_hub_record_count = east_count
            execution_log.west_hub_record_count = west_count
            execution_log.duplicates_removed = transformed.duplicates_removed
            execution_log.skipped_no_vin = transformed.skipped_no_vin
            execution_log.error_details.extend(transformed.errors)

            if not transformed.vehicles:
                raise ValueError("No valid vehicles available for ingest")

            # Snapshot safety gate. Refuse to push if the merged snapshot is
            # smaller than ove_ingest_size_threshold_pct% of the last
            # successful snapshot. Without this guard, a partial export
            # (e.g., one saved-search returned a fraction of its real rows
            # because of an OVE UI hiccup) silently overwrites the live VPS
            # inventory. The user explicitly designed the local pipeline
            # around this gate as the primary correctness firewall.
            self._enforce_snapshot_safety_gate(transformed)

            sync_metadata = {
                "east_hub_record_count": east_count,
                "west_hub_record_count": west_count,
                "duplicates_removed": transformed.duplicates_removed,
                "skipped_no_vin": transformed.skipped_no_vin,
                "scraper_node_id": self.settings.scraper_node_id,
                "scraper_version": self.settings.scraper_version,
                "source_platform": self.settings.ove_source_platform,
                "saved_search_names": list(all_required),
                "completed_saved_search_names": list(completed_searches),
                "expected_saved_search_count": len(all_required),
                "completed_saved_search_count": len(completed_searches),
                "missing_saved_search_names": [],
                "full_snapshot": True,
                "snapshot_mode": "full_replace",
                "verified_complete_snapshot": True,
                "upload_mode": "single_batch_replace",
                "replace_existing_snapshot": True,
                "single_batch_upload": True,
            }

            batch_result = self.push_snapshot(transformed, sync_metadata)
            execution_log.api_response = {
                "inserted": batch_result.db_records_added,
                "updated": batch_result.db_records_updated,
                "skipped_priority": batch_result.db_records_skipped_priority,
                "batches": batch_result.responses,
            }
            # Persist the new snapshot ONLY after a successful push so the
            # comparison baseline always reflects what is actually live on
            # the VPS, never a snapshot we tried but failed to upload.
            self._save_successful_snapshot(transformed)
            execution_log.api_push_status = "Success"
            execution_log.execution_status = "Success"

            if self.notifier is not None:
                self.notifier.notify_sync_success(
                    east_count=east_count,
                    west_count=west_count,
                    total_vehicles=len(transformed.vehicles),
                    duplicates_removed=transformed.duplicates_removed,
                    searches_exported=list(completed_searches),
                    logger=self.logger,
                )

            return execution_log
        except SavedSearchPageEmpty as exc:
            # Log the failure but RE-RAISE so run_browser_operation()
            # can trigger cookie-clearing + full browser recovery.
            # Swallowing this here would prevent the recovery from firing.
            execution_log.api_push_status = "Failure"
            execution_log.error_details.append(str(exc))
            self.logger.error("Hourly sync failed: %s", exc)
            raise
        except (ApiClientError, BrowserSessionError, ValueError) as exc:
            execution_log.api_push_status = "Failure"
            execution_log.error_details.append(str(exc))
            self.logger.error("Hourly sync failed: %s", exc)
            return execution_log
        finally:
            append_sync_log(self.settings.log_file_path, execution_log)

    def export_search_group(self, search_names: tuple[str, ...]) -> ExportGroupResult:
        rows: list[dict[str, str]] = []
        total_count = 0
        completed_searches: list[str] = []
        for search_name in search_names:
            try:
                csv_path = self.browser.export_saved_search(search_name, self.settings.export_dir)
            except BrowserSessionError as exc:
                # The browser exhausted its in-process retries (default 5).
                # Per the user's "fail loud, never silently skip" rule, we
                # do NOT swallow this — we alert the admin and re-raise so
                # the entire sync run aborts. The 75% safety gate would
                # also catch this downstream, but firing the alert here
                # gives the operator a more specific failure reason.
                self._alert_export_failed(search_name, exc)
                raise
            exported_rows = load_csv_rows(csv_path)
            if not exported_rows:
                empty_exc = BrowserSessionError(
                    f"Saved search '{search_name}' exported no inventory rows"
                )
                self._alert_export_failed(search_name, empty_exc)
                raise empty_exc
            rows.extend(exported_rows)
            total_count += len(exported_rows)
            completed_searches.append(search_name)
        return ExportGroupResult(
            rows=rows,
            total_count=total_count,
            completed_searches=tuple(completed_searches),
        )

    def _alert_export_failed(self, search_name: str, exc: Exception) -> None:
        debug_dir = getattr(exc, "debug_artifact_dir", "(not captured)")
        if self.notifier is not None:
            self.notifier.notify_export_failed(
                search_name=search_name,
                attempts=self.settings.ove_export_max_attempts,
                last_error=str(exc),
                debug_artifact_dir=str(debug_dir),
                logger=self.logger,
            )

    def resolve_saved_searches(self) -> tuple[str, ...]:
        configured = self.settings.ove_east_searches + self.settings.ove_west_searches
        try:
            discovered = self.browser.list_saved_searches()
            if discovered:
                self.logger.info("Discovered %s saved searches from OVE", len(discovered))
                return discovered
        except SavedSearchPageEmpty:
            # "No Saved Searches" is stale auth, not an OVE outage. Falling
            # back to configured names would just retry every export on the
            # same bad cookie. Propagate so run_browser_operation clears
            # cookies and fires the 'OVE scraper login required' alert.
            raise
        except BrowserSessionError as exc:
            self.logger.warning("Saved-search discovery failed; falling back to configured search names: %s", exc)

        if not configured:
            raise BrowserSessionError("No configured saved searches available for hourly sync fallback")
        self.logger.warning("Using configured saved-search fallback list with %s entries", len(configured))
        return configured

    def push_snapshot(self, transformed: TransformResult, sync_metadata: dict[str, Any]) -> BatchResult:
        vehicles = [vehicle.model_dump(mode="json") for vehicle in transformed.vehicles]
        response = self.api_client.push_ove_ingest(vehicles, sync_metadata)
        data = response.get("data", {})
        return BatchResult(
            responses=[response],
            db_records_added=int(data.get("inserted", data.get("db_records_added", 0))),
            db_records_updated=int(data.get("updated", data.get("db_records_updated", 0))),
            db_records_skipped_priority=int(
                data.get("skipped_priority", data.get("db_records_skipped_priority", 0))
            ),
        )

    @property
    def _last_snapshot_path(self) -> Path:
        return self.settings.data_dir / "ove_snapshot_last_successful.csv"

    @property
    def _previous_snapshot_path(self) -> Path:
        return self.settings.data_dir / "ove_snapshot_previous.csv"

    def _enforce_snapshot_safety_gate(self, transformed: TransformResult) -> None:
        threshold_pct = self.settings.ove_ingest_size_threshold_pct
        if threshold_pct <= 0:
            self.logger.warning(
                "Snapshot safety gate is DISABLED (OVE_INGEST_SIZE_THRESHOLD_PCT=%s). "
                "A partial OVE export could clobber the live VPS DB.",
                threshold_pct,
            )
            return
        proposed = len(transformed.vehicles)
        last = self._load_last_snapshot_count()
        if last is None:
            self.logger.info(
                "Snapshot safety gate: no last_successful baseline found at %s; "
                "first run is allowed to push %s rows unconditionally",
                self._last_snapshot_path,
                proposed,
            )
            return
        minimum = int(last * threshold_pct / 100)
        if proposed >= minimum:
            self.logger.info(
                "Snapshot safety gate: PASS (proposed=%s >= minimum=%s, last_successful=%s, threshold=%s%%)",
                proposed,
                minimum,
                last,
                threshold_pct,
            )
            return
        self.logger.error(
            "Snapshot safety gate: BLOCK (proposed=%s, last_successful=%s, minimum=%s, threshold=%s%%) — refusing to push to VPS",
            proposed,
            last,
            minimum,
            threshold_pct,
        )
        if self.notifier is not None:
            self.notifier.notify_snapshot_safety_gate_blocked(
                proposed_count=proposed,
                last_count=last,
                threshold_pct=threshold_pct,
                context={
                    "scraper_node_id": self.settings.scraper_node_id,
                    "last_snapshot_path": str(self._last_snapshot_path),
                },
                logger=self.logger,
            )
        raise SnapshotSafetyGateError(proposed=proposed, last=last, threshold_pct=threshold_pct)

    def _load_last_snapshot_count(self) -> int | None:
        path = self._last_snapshot_path
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                rows = list(reader)
            # First row is the header. Empty file or header-only counts as 0.
            return max(0, len(rows) - 1)
        except OSError as exc:
            self.logger.warning(
                "Could not read last successful snapshot at %s: %s. Treating as missing baseline.",
                path,
                exc,
            )
            return None

    def _save_successful_snapshot(self, transformed: TransformResult) -> None:
        path = self._last_snapshot_path
        previous = self._previous_snapshot_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Rotate prior snapshot if present, so we always keep one
            # generation of history for diagnostic diffing.
            if path.exists():
                try:
                    if previous.exists():
                        previous.unlink()
                    path.replace(previous)
                except OSError as exc:
                    self.logger.warning(
                        "Could not rotate previous snapshot %s -> %s: %s",
                        path,
                        previous,
                        exc,
                    )
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["vin", "year", "make", "model", "captured_at"])
                captured_at = datetime.now(timezone.utc).isoformat()
                for vehicle in transformed.vehicles:
                    writer.writerow([
                        vehicle.vin,
                        vehicle.year,
                        vehicle.make,
                        vehicle.model,
                        captured_at,
                    ])
            self.logger.info(
                "Snapshot baseline updated: wrote %s rows to %s",
                len(transformed.vehicles),
                path,
            )
        except OSError as exc:
            # A snapshot persistence failure must NOT mask a successful VPS
            # push. Log loudly and continue — the next sync run will see no
            # baseline (or the rotated previous one) and the user will be
            # alerted on the next safety-gate evaluation.
            self.logger.error(
                "Failed to persist successful snapshot baseline at %s: %s",
                path,
                exc,
            )

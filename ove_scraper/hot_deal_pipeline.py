"""Hot Deal vehicle screening pipeline orchestrator.

Exports two saved searches, loads VINs into SQLite, and runs each VIN
through a 3-step screening pipeline: CR analysis → AutoCheck modal →
Google VIN search. Shares the browser via OveAutomationLock.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from ove_scraper.automation_lock import OveAutomationLock
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings
from ove_scraper.csv_transform import load_csv_rows
from ove_scraper.hot_deal_db import (
    advance_status,
    claim_next_pending,
    create_run,
    finish_run,
    get_hot_deals,
    get_run_summary,
    insert_vins,
)
from ove_scraper.hot_deal_report import format_hot_deal_summary
from ove_scraper.hot_deal_screener import (
    screen_autocheck,
    screen_condition_report,
    screen_vin_web_search,
)
from ove_scraper.notifier import AdminNotifier
from ove_scraper.openai_web_search import search_vin_salvage_history
from ove_scraper.condition_report_normalizer import normalize_condition_report
from ove_scraper.cr_parsers import parse_condition_report_text

logger = logging.getLogger(__name__)


class HotDealPipelineRunner:
    """Orchestrates the daily Hot Deal screening pipeline."""

    def __init__(
        self,
        settings: Settings,
        browser: PlaywrightCdpBrowserSession,
        db_conn,
        log: logging.Logger | None = None,
        notifier: AdminNotifier | None = None,
    ):
        self.settings = settings
        self.browser = browser
        self.db = db_conn
        self.log = log or logger
        self.notifier = notifier

    def run_once(self) -> dict[str, Any]:
        """Run the full pipeline: export → load → screen → report."""
        search_names = list(self.settings.hot_deal_searches)
        run_id = create_run(self.db, search_names)
        errors: list[str] = []
        self.log.info("Hot Deal pipeline started: run_id=%s searches=%s", run_id, search_names)

        try:
            # Phase 1: Export saved searches
            csv_paths = self._export_searches(search_names)
            if not csv_paths:
                raise RuntimeError("No CSV files exported from saved searches")

            # Phase 2: Load VINs into SQLite
            total = self._load_vins(run_id, csv_paths)
            self.log.info("Hot Deal: loaded %d VINs into run %s", total, run_id)

            # Phase 3: Process each VIN through 3-step pipeline
            processed = 0
            while True:
                vin_row = claim_next_pending(self.db, run_id)
                if vin_row is None:
                    break
                vin = vin_row["vin"]
                processed += 1
                self.log.info("Hot Deal [%d]: processing VIN %s", processed, vin)
                try:
                    self._process_vin(vin, run_id)
                except Exception as exc:
                    err_msg = f"VIN {vin}: {type(exc).__name__}: {exc}"
                    self.log.error("Hot Deal pipeline error: %s", err_msg)
                    errors.append(err_msg)
                    advance_status(
                        self.db, vin, run_id, "step1_fail",
                        rejection_step="error", rejection_reason=str(exc),
                    )

            finish_run(self.db, run_id, "completed", errors or None)

        except Exception as exc:
            self.log.error("Hot Deal pipeline failed: %s", exc)
            errors.append(str(exc))
            finish_run(self.db, run_id, "failed", errors)

        # Phase 4: Report
        summary = get_run_summary(self.db, run_id)
        hot_deals = get_hot_deals(self.db, run_id)
        report_text = format_hot_deal_summary(summary, hot_deals)
        self.log.info("Hot Deal pipeline complete:\n%s", report_text)

        if self.notifier:
            try:
                self.notifier.notify_hot_deal_complete(
                    run_summary=summary, hot_deals=hot_deals,
                )
            except Exception as exc:
                self.log.warning("Failed to send Hot Deal notification: %s", exc)

        return summary

    def _export_searches(self, search_names: list[str]) -> list[Path]:
        """Export each saved search CSV using the browser, with locking."""
        csv_paths: list[Path] = []
        export_dir = self.settings.export_dir / "hot-deal"
        export_dir.mkdir(parents=True, exist_ok=True)

        for name in search_names:
            self.log.info("Hot Deal: exporting saved search '%s'", name)
            try:
                with OveAutomationLock(timeout_seconds=600):
                    path = self.browser.export_saved_search(
                        search_name=name,
                        export_dir=export_dir,
                    )
                if path and path.exists():
                    csv_paths.append(path)
                    self.log.info("Hot Deal: exported '%s' -> %s", name, path)
                else:
                    self.log.warning("Hot Deal: export returned no file for '%s'", name)
            except Exception as exc:
                self.log.error("Hot Deal: export failed for '%s': %s", name, exc)
        return csv_paths

    def _load_vins(self, run_id: str, csv_paths: list[Path]) -> int:
        """Load and dedup VINs from exported CSVs into the database."""
        all_rows: list[dict] = []
        seen_vins: set[str] = set()

        for path in csv_paths:
            rows = load_csv_rows(path)
            for row in rows:
                # OVE CSV column is "Vin" (title case). Also accept "VIN" /
                # "vin" for robustness if OVE changes its casing.
                vin = (row.get("Vin") or row.get("VIN") or row.get("vin") or "").strip().upper()
                if vin and len(vin) == 17 and vin not in seen_vins:
                    seen_vins.add(vin)
                    all_rows.append({
                        "vin": vin,
                        "year": _safe_int(row.get("Year") or row.get("year")),
                        "make": row.get("Make") or row.get("make"),
                        "model": row.get("Model") or row.get("model"),
                        "trim": row.get("Trim") or row.get("trim"),
                        "odometer": _safe_int(
                            row.get("Odometer Value") or row.get("Mileage")
                            or row.get("Odometer") or row.get("odometer")
                        ),
                        "price_asking": _safe_float(
                            row.get("Buy Now Price") or row.get("Asking Price")
                            or row.get("Buy Now") or row.get("Floor Price")
                        ),
                        "condition_grade": (
                            row.get("Condition Report Grade") or row.get("Condition")
                            or row.get("Grade")
                        ),
                        "location_state": row.get("Pickup Location") or row.get("State"),
                    })

        return insert_vins(self.db, run_id, all_rows)

    def _process_vin(self, vin: str, run_id: str) -> None:
        """Run a single VIN through all 3 screening steps."""
        artifact_dir = self.settings.artifact_dir / "hot-deal" / vin
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Condition Report
        self.log.info("Step 1 (CR screen) for VIN %s", vin)
        with OveAutomationLock(timeout_seconds=600):
            scrape_result = self.browser.deep_scrape_vin(vin)

        cr = scrape_result.condition_report
        listing_json = {}
        if cr and cr.metadata:
            listing_json = cr.metadata.get("listing_json", {})

        if cr is None:
            advance_status(
                self.db, vin, run_id, "step1_fail",
                rejection_step="step1", rejection_reason="No condition report available",
            )
            return

        step1 = screen_condition_report(cr, listing_json)
        cr_json = json.dumps({"passed": step1.passed, "reason": step1.reason})
        if not step1.passed:
            advance_status(
                self.db, vin, run_id, "step1_fail",
                rejection_step="step1", rejection_reason=step1.reason,
                data_column="cr_data", data_value=cr_json,
            )
            self.log.info("VIN %s FAILED step1: %s", vin, step1.reason)
            return

        advance_status(self.db, vin, run_id, "step1_pass", data_column="cr_data", data_value=cr_json)
        self.log.info("VIN %s PASSED step1", vin)

        # Step 2: AutoCheck modal
        self.log.info("Step 2 (AutoCheck) for VIN %s", vin)
        advance_status(self.db, vin, run_id, "step2_running")
        with OveAutomationLock(timeout_seconds=600):
            autocheck_data = self.browser.scrape_autocheck_modal(vin, artifact_dir)

        step2 = screen_autocheck(autocheck_data)
        ac_json = json.dumps({"passed": step2.passed, "reason": step2.reason})
        if not step2.passed:
            advance_status(
                self.db, vin, run_id, "step2_fail",
                rejection_step="step2", rejection_reason=step2.reason,
                data_column="autocheck_data", data_value=ac_json,
            )
            self.log.info("VIN %s FAILED step2: %s", vin, step2.reason)
            return

        advance_status(self.db, vin, run_id, "step2_pass", data_column="autocheck_data", data_value=ac_json)
        self.log.info("VIN %s PASSED step2", vin)

        # Step 3: Google VIN search (no browser lock needed)
        self.log.info("Step 3 (web search) for VIN %s", vin)
        advance_status(self.db, vin, run_id, "step3_running")

        if not self.settings.openai_api_key:
            self.log.warning("OPENAI_API_KEY not set; skipping step3 for VIN %s", vin)
            advance_status(self.db, vin, run_id, "hot_deal")
            return

        search_result = search_vin_salvage_history(
            vin=vin,
            api_key=self.settings.openai_api_key,
            model=self.settings.openai_model,
        )

        step3 = screen_vin_web_search(search_result)
        ws_json = json.dumps({"passed": step3.passed, "reason": step3.reason, "sites": search_result.get("found_on_salvage_sites", [])})
        if not step3.passed:
            advance_status(
                self.db, vin, run_id, "step3_fail",
                rejection_step="step3", rejection_reason=step3.reason,
                data_column="websearch_data", data_value=ws_json,
            )
            self.log.info("VIN %s FAILED step3: %s", vin, step3.reason)
            return

        advance_status(self.db, vin, run_id, "hot_deal", data_column="websearch_data", data_value=ws_json)
        self.log.info("VIN %s is a HOT DEAL", vin)


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None

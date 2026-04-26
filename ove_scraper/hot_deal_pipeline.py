"""Hot Deal vehicle screening pipeline orchestrator.

Daily workflow:
1. Export "VCH Marketing List" saved search (single list)
2. Reconcile against persistent SQLite DB:
   - VINs on list + in DB → skip (already processed)
   - VINs on list + NOT in DB → new, add and screen
   - VINs in DB + NOT on list → sold, hard-delete
3. Screen each NEW VIN through 3-step pipeline
4. Report results
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from datetime import datetime, timezone

from ove_scraper.api_client import ApiClientError, VCHApiClient
from ove_scraper.automation_lock import OveAutomationLock
from ove_scraper.browser import (
    BrowserSessionError,
    ConditionReportClickFailedError,
    ListingNotFoundError,
)
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings
from ove_scraper.csv_transform import load_csv_rows
from ove_scraper.hot_deal_db import (
    advance_status,
    claim_next_pending,
    create_run,
    delete_sold_vins,
    finish_run,
    get_active_vins,
    get_hot_deals,
    get_run_summary,
    insert_new_vins,
    reset_scrape_failed_to_pending,
    touch_last_seen,
)
from ove_scraper.hot_deal_payload import (
    build_hot_deals_batch,
    build_persisted_payload_data,
)
from ove_scraper.hot_deal_report import format_hot_deal_summary
from ove_scraper.hot_deal_screener import (
    screen_autocheck,
    screen_condition_report,
    screen_vin_web_search,
)
from ove_scraper.notifier import AdminNotifier
from ove_scraper.openai_web_search import search_vin_salvage_history

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
        api_client: VCHApiClient | None = None,
    ):
        self.settings = settings
        self.browser = browser
        self.db = db_conn
        self.log = log or logger
        self.notifier = notifier
        # Optional VCH API client for the post-screen Hot Deals batch
        # push (HOT_DEALS_SCRAPER_CONTRACT.md). When None, the push step
        # is skipped — the screening pipeline still runs and persists
        # payload-data.json files, so a future push can replay the
        # captured data.
        self.api_client = api_client

    def run_once(self) -> dict[str, Any]:
        """Run the full pipeline: export → reconcile → screen → report."""
        # Use only VCH Marketing List (Factory Warranty Active is a subset)
        search_name = self.settings.hot_deal_searches[-1]  # "VCH Marketing List"
        run_id = create_run(self.db, [search_name])
        errors: list[str] = []
        new_count = 0
        sold_count = 0
        self.log.info("Hot Deal pipeline started: run_id=%s search=%s", run_id, search_name)

        try:
            # Phase 1: Export saved search
            csv_path = self._export_search(search_name)
            if csv_path is None:
                raise RuntimeError(f"Failed to export saved search '{search_name}'")

            # Phase 2: Reconcile — dedup, find new, delete sold
            today_vins, today_rows = self._parse_csv(csv_path)
            stored_vins = get_active_vins(self.db)

            # VINs on list but not in DB → new
            new_vins = today_vins - stored_vins
            # VINs in DB but not on list → sold
            sold_vins = stored_vins - today_vins
            # VINs on both → still active (update last_seen)
            still_active = stored_vins & today_vins

            if sold_vins:
                sold_count = delete_sold_vins(self.db, sold_vins)
                self.log.info("Hot Deal: deleted %d sold VINs", sold_count)

            if still_active:
                touch_last_seen(self.db, still_active)

            new_rows = [r for r in today_rows if r["vin"] in new_vins]
            new_count = insert_new_vins(self.db, new_rows)

            # Reset any VINs that were left in scrape_failed state from a
            # previous run back to pending so the scraper gets another
            # shot today. These are VINs where the browser couldn't open
            # the CR — a transient capture bug, not a real rejection —
            # and they deserve a retry with today's warmer session.
            retry_yesterday = reset_scrape_failed_to_pending(self.db)
            if retry_yesterday:
                self.log.info(
                    "Hot Deal: reset %d previously-scrape-failed VIN(s) to pending for retry",
                    retry_yesterday,
                )

            self.log.info(
                "Hot Deal: %d on list, %d already in DB, %d new, %d sold, %d retries",
                len(today_vins), len(still_active), new_count, sold_count, retry_yesterday,
            )

            # Phase 3: Screen each pending VIN through 3-step pipeline.
            # Count *all* pending rows for the progress denominator, not
            # just new_count + retry_yesterday: an operator may have
            # pushed additional VINs into pending via hot-deal-reprocess
            # --rescreen (promoting previously-false-positive step1_fail
            # rows back for re-screening). Those are real work in this
            # run but fall outside the new/retry budget.
            total_pending_at_start = self.db.execute(
                "SELECT COUNT(*) FROM hot_deal_vins WHERE status='pending'"
            ).fetchone()[0]
            to_process = total_pending_at_start
            processed = 0
            while True:
                vin_row = claim_next_pending(self.db)
                if vin_row is None:
                    break
                vin = vin_row["vin"]
                processed += 1
                self.log.info("Hot Deal [%d/%d]: processing VIN %s", processed, to_process, vin)
                self._screen_vin_with_classification(vin, errors, in_retry_pass=False, vin_row=vin_row)

            # Phase 3b: one-shot in-run retry for VINs that scrape_failed
            # on their first attempt during this run. Rationale: the
            # 2026-04-23 Ferrari-loop fix dropped CR-click retries from
            # 4 to 2, which made transient popup flakiness terminal for
            # any VIN that hit it. A single re-attempt after the rest
            # of the list has run usually works — the browser session
            # has drifted back to a good state, and re-claiming a VIN
            # costs ~2-3 minutes vs losing it for the day. Only VINs
            # that still fail on the retry are truly marked scrape_failed.
            retried = reset_scrape_failed_to_pending(self.db)
            if retried:
                self.log.info(
                    "Hot Deal: one-shot retry pass for %d VIN(s) that scrape-failed this run",
                    retried,
                )
                retry_processed = 0
                while True:
                    vin_row = claim_next_pending(self.db)
                    if vin_row is None:
                        break
                    vin = vin_row["vin"]
                    retry_processed += 1
                    self.log.info(
                        "Hot Deal retry [%d/%d]: processing VIN %s",
                        retry_processed, retried, vin,
                    )
                    self._screen_vin_with_classification(vin, errors, in_retry_pass=True, vin_row=vin_row)

            # Phase 3c: push the curated batch to the VPS. Done BEFORE
            # finish_run() so any push error is recorded in error_details
            # and the daily summary reflects the actual batch outcome.
            # No-op when push is disabled or no api_client is wired in.
            push_summary = self._push_hot_deals_to_vps(run_id)
            if push_summary and push_summary.get("error"):
                errors.append(f"Hot Deals push: {push_summary['error']}")

            finish_run(self.db, run_id, "completed", new_vins=new_count, sold_vins=sold_count, error_details=errors or None)

        except Exception as exc:
            self.log.error("Hot Deal pipeline failed: %s", exc)
            errors.append(str(exc))
            finish_run(self.db, run_id, "failed", new_vins=new_count, sold_vins=sold_count, error_details=errors)

        # Phase 4: Report
        summary = get_run_summary(self.db, run_id)
        hot_deals = get_hot_deals(self.db)
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

    def _export_search(self, search_name: str) -> Path | None:
        """Export a single saved search CSV using the browser."""
        export_dir = self.settings.export_dir / "hot-deal"
        export_dir.mkdir(parents=True, exist_ok=True)
        self.log.info("Hot Deal: exporting saved search '%s'", search_name)
        try:
            with OveAutomationLock(timeout_seconds=600):
                path = self.browser.export_saved_search(
                    search_name=search_name,
                    export_dir=export_dir,
                )
            if path and path.exists():
                self.log.info("Hot Deal: exported '%s' -> %s", search_name, path)
                return path
            self.log.warning("Hot Deal: export returned no file for '%s'", search_name)
        except Exception as exc:
            self.log.error("Hot Deal: export failed for '%s': %s", search_name, exc)
        return None

    def _parse_csv(self, csv_path: Path) -> tuple[set[str], list[dict]]:
        """Parse exported CSV into a set of VINs and a list of row dicts."""
        rows_raw = load_csv_rows(csv_path)
        today_vins: set[str] = set()
        today_rows: list[dict] = []

        for row in rows_raw:
            vin = (row.get("Vin") or row.get("VIN") or row.get("vin") or "").strip().upper()
            if not vin or len(vin) != 17 or vin in today_vins:
                continue
            today_vins.add(vin)
            today_rows.append({
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

        return today_vins, today_rows

    def _screen_vin_with_classification(
        self,
        vin: str,
        errors: list[str],
        *,
        in_retry_pass: bool,
        vin_row: dict | None = None,
    ) -> None:
        """Invoke _process_vin for one VIN and classify any exception.

        Scraper-side exceptions (BrowserSessionError and its subclasses:
        ConditionReportClickFailedError, ListingNotFoundError) mean we
        never got enough data to reach the screener. Those become
        status='scrape_failed' — retry-eligible — on the first attempt,
        and permanently step1_fail with a scraper-error category on the
        retry pass. Everything else is a true pipeline error and
        terminals immediately.

        ``vin_row`` is threaded through so _process_vin can persist the
        CSV-derived basics (year/make/model/odometer/asking) into the
        per-VIN payload-data.json without re-querying the DB. Optional
        for callers that don't have it handy (defaults harmlessly).
        """
        try:
            self._process_vin(vin, vin_row=vin_row)
        except (ConditionReportClickFailedError, ListingNotFoundError, BrowserSessionError) as exc:
            err_msg = f"VIN {vin}: {type(exc).__name__}: {exc}"
            self.log.error("Hot Deal scraper error: %s", err_msg)
            errors.append(err_msg)
            if in_retry_pass:
                # Second strike — give up on this VIN for today. Mark
                # it step1_fail with a scraper-error reason so it's
                # visible in the daily summary as a capture problem
                # rather than a screener verdict. Tomorrow's reset
                # won't pick this one back up.
                advance_status(
                    self.db, vin, "step1_fail",
                    rejection_step="scraper_error",
                    rejection_reason=f"{type(exc).__name__} on retry: {exc}",
                )
            else:
                # First strike — retry eligible in the second pass.
                advance_status(
                    self.db, vin, "scrape_failed",
                    rejection_step="scraper_error",
                    rejection_reason=str(exc),
                )
        except Exception as exc:
            err_msg = f"VIN {vin}: {type(exc).__name__}: {exc}"
            self.log.error("Hot Deal pipeline error: %s", err_msg)
            errors.append(err_msg)
            advance_status(
                self.db, vin, "step1_fail",
                rejection_step="error", rejection_reason=str(exc),
            )

    def _process_vin(self, vin: str, *, vin_row: dict | None = None) -> None:
        """Run a single VIN through all 3 screening steps.

        ``vin_row`` (when provided) is the CSV-derived dict from
        claim_next_pending — used to populate the per-VIN
        ``payload-data.json`` artifact when the VIN reaches
        ``status='hot_deal'`` so the Hot Deals batch push at end-of-run
        has all the data it needs without a re-scrape.
        """
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
                self.db, vin, "step1_fail",
                rejection_step="step1", rejection_reason="No condition report available",
            )
            return

        step1 = screen_condition_report(cr, listing_json)
        cr_json = json.dumps({"passed": step1.passed, "reason": step1.reason})
        if not step1.passed:
            advance_status(
                self.db, vin, "step1_fail",
                rejection_step="step1", rejection_reason=step1.reason,
                data_column="cr_data", data_value=cr_json,
            )
            self.log.info("VIN %s FAILED step1: %s", vin, step1.reason)
            return

        advance_status(self.db, vin, "step1_pass", data_column="cr_data", data_value=cr_json)
        self.log.info("VIN %s PASSED step1", vin)

        # Step 2: AutoCheck — read from deep scrape result (already captured)
        self.log.info("Step 2 (AutoCheck) for VIN %s", vin)
        advance_status(self.db, vin, "step2_running")

        autocheck_data = {}
        if cr.autocheck:
            autocheck_data = cr.autocheck.model_dump()
        elif listing_json.get("autocheck"):
            # Fallback to listing JSON autocheck flags
            ac = listing_json["autocheck"]
            autocheck_data = {
                "title_brand_check": "OK" if ac.get("titleAndProblemCheckOK") else "Problem Reported",
                "odometer_check": "OK" if ac.get("odometerCheckOK") else "Problem Reported",
            }

        step2 = screen_autocheck(autocheck_data)
        ac_json = json.dumps({"passed": step2.passed, "reason": step2.reason})
        if not step2.passed:
            advance_status(
                self.db, vin, "step2_fail",
                rejection_step="step2", rejection_reason=step2.reason,
                data_column="autocheck_data", data_value=ac_json,
            )
            self.log.info("VIN %s FAILED step2: %s", vin, step2.reason)
            return

        advance_status(self.db, vin, "step2_pass", data_column="autocheck_data", data_value=ac_json)
        self.log.info("VIN %s PASSED step2", vin)

        # Step 3: Google VIN search (no browser lock needed)
        self.log.info("Step 3 (web search) for VIN %s", vin)
        advance_status(self.db, vin, "step3_running")

        if not self.settings.openai_api_key:
            self.log.warning("OPENAI_API_KEY not set; skipping step3 for VIN %s", vin)
            advance_status(self.db, vin, "hot_deal")
            self._persist_hot_deal_payload(vin, scrape_result, listing_json, vin_row, artifact_dir)
            return

        search_result = search_vin_salvage_history(
            vin=vin,
            api_key=self.settings.openai_api_key,
            model=self.settings.openai_model,
        )

        step3 = screen_vin_web_search(search_result)
        ws_json = json.dumps({
            "passed": step3.passed, "reason": step3.reason,
            "sites": search_result.get("found_on_salvage_sites", []),
        })
        if not step3.passed:
            advance_status(
                self.db, vin, "step3_fail",
                rejection_step="step3", rejection_reason=step3.reason,
                data_column="websearch_data", data_value=ws_json,
            )
            self.log.info("VIN %s FAILED step3: %s", vin, step3.reason)
            return

        advance_status(self.db, vin, "hot_deal", data_column="websearch_data", data_value=ws_json)
        self.log.info("VIN %s is a HOT DEAL", vin)
        self._persist_hot_deal_payload(vin, scrape_result, listing_json, vin_row, artifact_dir)

    def _persist_hot_deal_payload(
        self,
        vin: str,
        scrape_result,
        listing_json: dict | None,
        vin_row: dict | None,
        artifact_dir: Path,
    ) -> None:
        """Serialize the in-memory deep-scrape data to ``payload-data.json``.

        Called whenever a VIN advances to ``status='hot_deal'``. Reuses
        the data the screener already inspected — NO new scraping. The
        end-of-run push step reads these per-VIN files and assembles the
        batch for the VPS endpoint.

        Best-effort: a write failure here is logged but does not abort
        the pipeline (the VIN is still marked hot_deal in the DB; the
        push step will skip VINs whose payload-data.json is missing).
        """
        try:
            payload_data = build_persisted_payload_data(
                vin=vin,
                deep_scrape_result=scrape_result,
                listing_json=listing_json or {},
                vin_row=vin_row or {},
                source_platform=self.settings.ove_source_platform,
            )
            target = artifact_dir / "payload-data.json"
            target.write_text(json.dumps(payload_data, default=str), encoding="utf-8")
            self.log.debug("Hot Deal payload-data persisted for VIN %s -> %s", vin, target)
        except Exception as exc:
            self.log.warning(
                "Hot Deal payload-data persistence failed for VIN %s (non-fatal): %s",
                vin, exc,
            )

    def _load_persisted_payload_data(self, vin: str) -> dict | None:
        """Read a VIN's persisted payload-data.json from disk."""
        path = self.settings.artifact_dir / "hot-deal" / vin / "payload-data.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.log.warning("Hot Deal payload-data unreadable for VIN %s: %s", vin, exc)
            return None

    def _push_hot_deals_to_vps(self, run_id: str) -> dict | None:
        """Push the curated Hot Deals batch to the VPS.

        Reads the persisted ``payload-data.json`` files for every VIN
        currently in ``status='hot_deal'`` (the *full current snapshot*
        per the contract's ``snapshot_mode='full_replace'`` semantics),
        builds the batch payload, and POSTs it. Returns a summary dict
        for logging / inclusion in run errors. Returns None when the
        push is disabled or no api_client is wired.

        Best-effort by design: the screening pipeline must complete
        cleanly regardless of VPS availability, so we catch and log
        ApiClientError rather than re-raise.
        """
        if not self.settings.hot_deal_vps_push_enabled:
            self.log.info("Hot Deals VPS push disabled (hot_deal_vps_push_enabled=False)")
            return None
        if self.api_client is None:
            self.log.warning(
                "Hot Deals VPS push skipped — no api_client wired into HotDealPipelineRunner"
            )
            return None

        hot_deals_rows = get_hot_deals(self.db)
        if not hot_deals_rows:
            self.log.info("Hot Deals VPS push: no hot_deal rows to send")
            return {"pushed": 0, "skipped": [], "skipped_count": 0}

        deals_payload_data: list[dict] = []
        missing_payload: list[str] = []
        for row in hot_deals_rows:
            vin = row["vin"]
            data = self._load_persisted_payload_data(vin)
            if data is None:
                missing_payload.append(vin)
                continue
            deals_payload_data.append(data)

        if missing_payload:
            self.log.warning(
                "Hot Deals VPS push: %d VIN(s) have no payload-data.json on disk "
                "(rows: %s) — likely from runs predating the persistence change. "
                "These will not be in the batch.",
                len(missing_payload), missing_payload[:5],
            )

        scraped_at = datetime.now(timezone.utc)
        batch_id = f"vhc-marketing-{scraped_at.strftime('%Y-%m-%d-%H%MZ')}-{run_id[:8]}"
        batch, skipped = build_hot_deals_batch(
            deals_payload_data,
            batch_id=batch_id,
            scraped_at=scraped_at,
            snapshot_mode="full_replace",
            source_platform=self.settings.ove_source_platform,
            min_delta_below_mmr=self.settings.hot_deal_min_delta_below_mmr,
        )

        if skipped:
            self.log.warning(
                "Hot Deals batch: %d VIN(s) skipped at payload-build time "
                "(missing required fields like auction_end_at or price): %s",
                len(skipped), skipped[:10],
            )

        deal_count = len(batch.get("deals", []))
        if deal_count == 0:
            self.log.warning(
                "Hot Deals batch is empty after build (rows=%d, missing_payload=%d, "
                "skipped_at_build=%d) — not pushing",
                len(hot_deals_rows), len(missing_payload), len(skipped),
            )
            return {
                "pushed": 0,
                "skipped": skipped,
                "skipped_count": len(skipped),
                "missing_payload_count": len(missing_payload),
            }

        try:
            self.log.info(
                "Hot Deals VPS push: posting batch_id=%s with %d deals to %s",
                batch_id, deal_count, self.settings.hot_deal_vps_endpoint_path,
            )
            response = self.api_client.push_hot_deals_batch(
                batch,
                endpoint_path=self.settings.hot_deal_vps_endpoint_path,
            )
            self.log.info("Hot Deals VPS push: response=%s", response)
            return {
                "pushed": deal_count,
                "skipped": skipped,
                "skipped_count": len(skipped),
                "missing_payload_count": len(missing_payload),
                "response": response,
            }
        except ApiClientError as exc:
            self.log.error("Hot Deals VPS push failed: %s", exc)
            return {
                "pushed": 0,
                "skipped": skipped,
                "skipped_count": len(skipped),
                "missing_payload_count": len(missing_payload),
                "error": str(exc),
            }
        except Exception as exc:  # pragma: no cover - defensive
            self.log.error("Hot Deals VPS push raised unexpectedly: %s", exc, exc_info=True)
            return {
                "pushed": 0,
                "skipped": skipped,
                "skipped_count": len(skipped),
                "missing_payload_count": len(missing_payload),
                "error": f"{type(exc).__name__}: {exc}",
            }


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

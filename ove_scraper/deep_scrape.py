from __future__ import annotations

import json
import logging
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ove_scraper.api_client import ApiClientError, VCHApiClient
from ove_scraper.browser import (
    BrowserSession,
    BrowserSessionError,
    DeepScrapeResult,
    ListingNotFoundError,
    ManheimAuthRedirectError,
)
from ove_scraper.config import Settings
from ove_scraper.schemas import DetailImage, ListingSnapshot, PendingDetailRequest


REDACTED_TERMS = ("listing seller", "seller name", "current bid", "high bid")
EASTERN_TZ = ZoneInfo("America/New_York")
_TRACKER_LOCK = threading.Lock()


class DeepScrapeWorker:
    def __init__(
        self,
        api_client: VCHApiClient,
        browser: BrowserSession,
        logger: logging.Logger,
        settings: Settings,
    ) -> None:
        self.api_client = api_client
        self.browser = browser
        self.logger = logger
        self.settings = settings
        self._not_found_tracker_path = self.settings.artifact_dir / "_state" / "not_found_tracker.json"
        self._parallel_warning_emitted = False
        # Process-local cooldown keyed by request_id. Whenever a request fails or
        # is terminal-resolved, we record the monotonic timestamp at which it
        # becomes eligible for re-processing locally. This prevents the worker
        # from immediately re-claiming and re-failing the same request_id while
        # the VPS retry backoff is still in effect — the bug behind the prior
        # 3.5-hour /fail hot loop. Independent of the VPS-side `next_retry_at`
        # filter (which is correct but cannot react instantly to a 409 race).
        self._fail_cooldown: dict[str, float] = {}

    def process_pending_once(self) -> list[str]:
        self.logger.info(
            "Requesting OVE detail claims from VPS as worker_id=%s limit=%s lease_seconds=%s",
            self.settings.detail_worker_id,
            max(1, self.settings.deep_scrape_max_workers),
            self.settings.deep_scrape_lease_seconds,
        )
        requests = self._filter_requests_ready(
            self._dedupe_requests(
                self.api_client.claim_pending_detail_requests(
                    worker_id=self.settings.detail_worker_id,
                    limit=max(1, self.settings.deep_scrape_max_workers),
                    lease_seconds=self.settings.deep_scrape_lease_seconds,
                )
            )
        )
        if not requests:
            self.logger.info("No OVE detail claims available for worker_id=%s", self.settings.detail_worker_id)
            return []

        self.logger.info(
            "Claimed %s OVE detail request(s): %s",
            len(requests),
            ", ".join(f"{request.request_id}:{request.vin}" for request in requests),
        )

        max_workers = max(1, self.settings.deep_scrape_max_workers)
        if max_workers > 1 and not self._parallel_warning_emitted:
            self.logger.warning(
                "Deep scrape worker concurrency is forced to 1 because the shared OVE Chrome session "
                "is not safe for parallel workers"
            )
            self._parallel_warning_emitted = True
        return self._process_pending_sequential(requests)

    def _process_pending_sequential(self, requests: list[PendingDetailRequest]) -> list[str]:
        processed: list[str] = []
        for request in requests:
            vin = self._process_request(request, self.browser, self.api_client)
            if vin:
                processed.append(vin)
        return processed

    def _process_request(
        self,
        request: PendingDetailRequest,
        browser: BrowserSession,
        api_client: VCHApiClient,
    ) -> str | None:
        self.logger.info(
            "Starting deep scrape for request_id=%s vin=%s attempts=%s claimed_at=%s lease_expires_at=%s",
            request.request_id,
            request.vin,
            request.attempts,
            request.claimed_at,
            request.lease_expires_at,
        )
        try:
            with self._lease_heartbeat(api_client, request) as heartbeat_state:
                detail = browser.deep_scrape_vin(request.vin)
                if heartbeat_state["lost"]:
                    self.logger.warning(
                        "Abandoning detail request %s for VIN %s because the lease was lost during scrape",
                        request.request_id,
                        request.vin,
                    )
                    return None
            payload = redact_detail(detail, request, self.settings)
            self._write_payload_artifact(request.vin, "detail-payload.json", payload)
            # Defense-in-depth pre-push validation. Refuses to push payloads
            # that look like silent CR-capture failures (auth redirects,
            # zero-image CRs masquerading as success, wrong page captured).
            # Without this guard the prior bug pushed Manheim sign-in pages
            # to the VPS as if they were real condition reports.
            self._validate_cr_payload_or_raise(request, payload)
            self.logger.info(
                "Posting OVE detail payload for request_id=%s vin=%s images=%s has_condition_report=%s",
                request.request_id,
                request.vin,
                len(payload.get("images", [])),
                bool(payload.get("condition_report")),
            )
            api_client.push_ove_detail(request.vin, payload)
            self._clear_not_found_state(request.vin)
            api_client.complete_detail_request(
                request.request_id,
                worker_id=self.settings.detail_worker_id,
                result="success",
            )
            self.logger.info("Completed deep scrape request_id=%s vin=%s", request.request_id, request.vin)
            return request.vin
        except ListingNotFoundError as exc:
            decision = self._record_not_found_attempt(request, str(exc))
            self._write_availability_audit(request.vin, decision, str(exc))
            if not decision["ready_to_finalize"]:
                self.logger.warning(
                    "Deferring unavailable report for %s after miss %s/%s (%s)",
                    request.vin,
                    decision["attempt_count"],
                    self.settings.not_found_confirm_attempts,
                    decision["reason"],
                )
                self._fail_claimed_request(
                    api_client,
                    request,
                    error_category="temporarily_unavailable",
                    error_message=str(exc),
                    retry_after_seconds=self._retry_delay_for_not_found(decision["reason"]),
                )
                return None
            payload = build_not_found_payload(request, self.settings, str(exc))
            try:
                self._write_payload_artifact(request.vin, "detail-payload.json", payload)
                api_client.push_ove_detail(request.vin, payload)
                self._clear_not_found_state(request.vin)
                api_client.complete_detail_request(
                    request.request_id,
                    worker_id=self.settings.detail_worker_id,
                    result="not_found",
                )
                self.logger.warning("Marked VIN %s unavailable in OVE: %s", request.vin, exc)
                return request.vin
            except ApiClientError as api_exc:
                if self._is_terminal_missing_vehicle_error(api_exc):
                    self._terminal_claimed_request(
                        api_client,
                        request,
                        reason="vehicle_missing_on_vps",
                        message=str(api_exc),
                    )
                    self._clear_not_found_state(request.vin)
                    self.logger.warning(
                        "Suppressing stale VPS detail request %s for VIN %s after terminal 404: %s",
                        request.request_id,
                        request.vin,
                        api_exc,
                    )
                    return request.vin
                self.logger.error("Failed to report unavailable VIN %s: %s", request.vin, api_exc)
                self._fail_claimed_request(
                    api_client,
                    request,
                    error_category="api_error",
                    error_message=str(api_exc),
                    retry_after_seconds=self.settings.deep_scrape_retry_delay_seconds,
                )
                return None
        except (ApiClientError, BrowserSessionError, ValueError) as exc:
            # ManheimAuthRedirectError is a BrowserSessionError subclass and
            # is therefore caught here. _classify_failure routes it to
            # auth_expired which is the correct retry semantics per
            # SCRAPER_CONTRACT.md §5.1 (60s backoff after re-login).
            if isinstance(exc, ApiClientError) and self._is_terminal_missing_vehicle_error(exc):
                self._terminal_claimed_request(
                    api_client,
                    request,
                    reason="vehicle_missing_on_vps",
                    message=str(exc),
                )
                self._clear_not_found_state(request.vin)
                self.logger.warning(
                    "Suppressing stale VPS detail request %s for VIN %s after terminal 404: %s",
                    request.request_id,
                    request.vin,
                    exc,
                )
                return request.vin
            if self._is_terminal_validation_error(exc):
                # 422/400 from the VPS — payload is permanently unacceptable.
                # Send to /terminal so we never retry. Closest matching reason
                # in SCRAPER_CONTRACT.md §5.2 is unsupported_listing_type.
                self._terminal_claimed_request(
                    api_client,
                    request,
                    reason="unsupported_listing_type",
                    message=str(exc),
                )
                self.logger.error(
                    "Detail request %s for VIN %s rejected by VPS as permanently invalid; resolved as terminal: %s",
                    request.request_id,
                    request.vin,
                    exc,
                )
                return request.vin
            retry_after_seconds = self.settings.deep_scrape_retry_delay_seconds
            self._fail_claimed_request(
                api_client,
                request,
                error_category=self._classify_failure(exc),
                error_message=str(exc),
                retry_after_seconds=retry_after_seconds,
            )
            self.logger.error("Deep scrape failed for %s: %s", request.vin, exc, exc_info=True)
            self.logger.warning(
                "Released claimed detail request %s for VIN %s back to VPS with retry_after_seconds=%s",
                request.request_id,
                request.vin,
                retry_after_seconds,
            )
            return None

    def _write_payload_artifact(self, vin: str, file_name: str, payload: dict[str, object]) -> Path:
        artifact_dir = self.settings.artifact_dir / vin
        artifact_dir.mkdir(parents=True, exist_ok=True)
        path = artifact_dir / file_name
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _write_availability_audit(self, vin: str, decision: dict[str, object], failure_reason: str) -> Path:
        artifact_dir = self.settings.artifact_dir / vin
        artifact_dir.mkdir(parents=True, exist_ok=True)
        path = artifact_dir / "availability-audit.json"
        payload = {
            "vin": vin,
            "timestamp_eastern": datetime.now(EASTERN_TZ).isoformat(),
            "ready_to_finalize": decision["ready_to_finalize"],
            "attempt_count": decision["attempt_count"],
            "decision_reason": decision["reason"],
            "failure_reason": failure_reason,
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _record_not_found_attempt(self, request: PendingDetailRequest, failure_reason: str) -> dict[str, object]:
        with _TRACKER_LOCK:
            tracker = self._load_not_found_tracker()
            state = tracker.get(request.vin, {})
            if state.get("request_id") != request.request_id:
                attempt_count = 1
            else:
                attempt_count = int(state.get("attempt_count", 0)) + 1
            now = datetime.now(EASTERN_TZ)
            in_ims_window = self._is_ims_refresh_window(now)
            tracker[request.vin] = {
                "request_id": request.request_id,
                "attempt_count": attempt_count,
                "last_failure_at": now.isoformat(),
                "last_failure_reason": failure_reason,
            }
            self._save_not_found_tracker(tracker)

        if in_ims_window:
            return {
                "ready_to_finalize": False,
                "attempt_count": attempt_count,
                "reason": "ims_refresh_window",
            }
        if attempt_count < self.settings.not_found_confirm_attempts:
            return {
                "ready_to_finalize": False,
                "attempt_count": attempt_count,
                "reason": "awaiting_confirmation_retries",
            }
        return {
            "ready_to_finalize": True,
            "attempt_count": attempt_count,
            "reason": "confirmed_not_found",
        }

    def _clear_not_found_state(self, vin: str) -> None:
        with _TRACKER_LOCK:
            tracker = self._load_not_found_tracker()
            if vin not in tracker:
                return
            tracker.pop(vin, None)
            self._save_not_found_tracker(tracker)

    def _dedupe_requests(self, requests: list[PendingDetailRequest]) -> list[PendingDetailRequest]:
        unique: list[PendingDetailRequest] = []
        seen_request_ids: set[str] = set()
        for request in requests:
            if request.request_id in seen_request_ids:
                self.logger.warning(
                    "Skipping duplicate pending detail request in same poll response: %s for VIN %s",
                    request.request_id,
                    request.vin,
                )
                continue
            seen_request_ids.add(request.request_id)
            unique.append(request)
        return unique

    def _filter_requests_ready(self, requests: list[PendingDetailRequest]) -> list[PendingDetailRequest]:
        if not self._fail_cooldown:
            return requests
        now = time.monotonic()
        # Drop expired cooldown entries so the dict can't grow forever in a
        # long-running process. Done on every poll cycle so the cleanup cost is
        # bounded by the number of in-flight requests.
        expired = [request_id for request_id, eligible_at in self._fail_cooldown.items() if eligible_at <= now]
        for request_id in expired:
            self._fail_cooldown.pop(request_id, None)
        ready: list[PendingDetailRequest] = []
        for request in requests:
            eligible_at = self._fail_cooldown.get(request.request_id)
            if eligible_at is not None and eligible_at > now:
                self.logger.info(
                    "Skipping detail request %s for VIN %s; in local fail cooldown for %.0fs more",
                    request.request_id,
                    request.vin,
                    eligible_at - now,
                )
                continue
            ready.append(request)
        return ready

    def _record_fail_cooldown(self, request: PendingDetailRequest, retry_after_seconds: int) -> None:
        # Always honor at least the VPS-suggested backoff, but never less than
        # one minute. The lower bound stops a misconfigured retry_after of 0
        # from defeating the cooldown entirely.
        backoff = max(60, int(retry_after_seconds))
        self._fail_cooldown[request.request_id] = time.monotonic() + backoff

    def _fail_claimed_request(
        self,
        api_client: VCHApiClient,
        request: PendingDetailRequest,
        *,
        error_category: str,
        error_message: str,
        retry_after_seconds: int,
    ) -> None:
        # Record the local cooldown BEFORE the API call. If the call raises or
        # returns a race-y 409, we still want this worker to back off locally
        # for at least the requested duration. Without this the prior bug
        # caused the same request_id to be re-claimed and re-failed every
        # ~6 minutes for hours.
        self._record_fail_cooldown(request, retry_after_seconds)
        try:
            api_client.fail_detail_request(
                request.request_id,
                worker_id=self.settings.detail_worker_id,
                error_category=error_category,
                error_message=error_message,
                retry_after_seconds=retry_after_seconds,
            )
        except ApiClientError as fail_exc:
            if self._is_already_resolved_conflict(fail_exc):
                # The VPS has already moved this request out of CLAIMED — most
                # commonly because the detail POST safety net auto-completed
                # it (see SCRAPER_CONTRACT.md §3.6). This is the expected
                # outcome of a successful detail push, NOT a worker-side
                # error. Treat as already resolved.
                self.logger.info(
                    "Detail request %s for VIN %s already resolved on VPS (likely auto-completed by detail POST); local fail no-op",
                    request.request_id,
                    request.vin,
                )
                return
            self.logger.error(
                "Failed to release claimed detail request %s for VIN %s back to VPS: %s",
                request.request_id,
                request.vin,
                fail_exc,
            )

    def _terminal_claimed_request(
        self,
        api_client: VCHApiClient,
        request: PendingDetailRequest,
        *,
        reason: str,
        message: str,
    ) -> None:
        # Terminal resolutions also get a long local cooldown so a 409 race
        # cannot bring the request back into the worker's poll loop.
        self._record_fail_cooldown(request, 24 * 60 * 60)
        try:
            api_client.terminal_detail_request(
                request.request_id,
                worker_id=self.settings.detail_worker_id,
                reason=reason,
                message=message,
            )
        except ApiClientError as terminal_exc:
            if self._is_already_resolved_conflict(terminal_exc):
                self.logger.info(
                    "Detail request %s for VIN %s already resolved on VPS (likely auto-completed by detail POST); local terminal no-op",
                    request.request_id,
                    request.vin,
                )
                return
            self.logger.error(
                "Failed to terminal-resolve claimed detail request %s for VIN %s: %s",
                request.request_id,
                request.vin,
                terminal_exc,
            )

    def _is_already_resolved_conflict(self, exc: Exception) -> bool:
        # Detect the VPS 409 response that means "this request is no longer
        # in CLAIMED state". Per the contract this is what the VPS returns
        # when the detail POST already auto-completed the request, or another
        # worker re-claimed an expired lease and finished it.
        lowered = str(exc).lower()
        if "status 409" not in lowered:
            return False
        return any(
            marker in lowered
            for marker in (
                "is not currently claimed",
                "status=completed",
                "status=terminal",
                "not in claimed state",
            )
        )

    def _classify_failure(self, exc: Exception) -> str:
        lowered = str(exc).lower()
        if isinstance(exc, ManheimAuthRedirectError):
            return "auth_expired"
        if isinstance(exc, BrowserSessionError):
            if "login page" in lowered or "not authenticated" in lowered or "auth.manheim" in lowered:
                return "auth_expired"
            if "too many requests" in lowered or "rate limit" in lowered or "captcha" in lowered:
                return "rate_limited"
            if "temporarily" in lowered or "not yet" in lowered:
                return "temporarily_unavailable"
            return "browser_error"
        if isinstance(exc, ApiClientError):
            if "status 429" in lowered or "rate limit" in lowered:
                return "rate_limited"
            if "status 5" in lowered:
                return "transient_network"
            return "transient_network"
        if isinstance(exc, ValueError):
            return "page_structure_changed"
        return "transient_network"

    def _is_terminal_missing_vehicle_error(self, exc: Exception) -> bool:
        lowered = str(exc).lower()
        return "status 404" in lowered and "vehicle not found" in lowered

    def _is_terminal_validation_error(self, exc: Exception) -> bool:
        # 422 from the VPS means the payload structurally cannot be accepted
        # (bad VIN format, schema violation, etc). Retrying with the same
        # input cannot succeed, so this must go to /terminal, not /fail.
        # Without this routing the prior bug caused the launcher to crash on
        # repeated unhandled ApiClientErrors when a malformed VIN was queued.
        if not isinstance(exc, ApiClientError):
            return False
        lowered = str(exc).lower()
        return "status 422" in lowered or "status 400" in lowered

    def _validate_cr_payload_or_raise(
        self,
        request: PendingDetailRequest,
        payload: dict[str, object],
    ) -> None:
        """Refuse to push payloads that look like silent CR-capture failures.

        This is the failsafe behind the Manheim auth-redirect detection in
        cdp_browser.py. Even if a new auth failure mode lands on a page that
        the upstream detector doesn't recognize, this guard inspects the
        captured CR metadata one last time before the data hits the VPS, and
        raises a ValueError loud enough to be visible in the failure logs and
        routed via the standard /fail path.

        Validation rules:
          1. If the captured condition_report metadata reports a Manheim auth
             URL or a "Sign In" page title, the capture is corrupt.
          2. If the listing_snapshot metadata's nested condition_report_page
             carries the same auth markers, ditto.
          3. If a condition_report block exists but the merged image list is
             empty AND the report_page's body_text contains the gallery hint
             "of N" suggesting N>1 images were expected, the capture missed
             every image — treat as failure.

        Rule 3 is intentionally conservative. We do NOT yet enforce a
        minimum image count for general CRs because some legitimate listings
        have very few images; only obvious zero-vs-many mismatches fail here.
        Tightening that bound is item P2 in the fix plan, after we have real
        post-fix telemetry.
        """
        cr_block = payload.get("condition_report")
        listing_snapshot = payload.get("listing_snapshot") or {}
        snapshot_metadata = (
            listing_snapshot.get("metadata", {}) if isinstance(listing_snapshot, dict) else {}
        )
        report_page = (
            snapshot_metadata.get("condition_report_page") if isinstance(snapshot_metadata, dict) else None
        )
        if not isinstance(report_page, dict):
            report_page = {}

        # Rule 1+2: auth-URL / sign-in title in captured CR metadata.
        candidate_urls: list[str] = []
        candidate_titles: list[str] = []
        for source in (cr_block, report_page):
            if isinstance(source, dict):
                meta = source.get("metadata") if "metadata" in source else None
                report_page_in_cr = (
                    meta.get("report_page") if isinstance(meta, dict) else None
                )
                if isinstance(report_page_in_cr, dict):
                    candidate_urls.append(str(report_page_in_cr.get("url") or ""))
                    candidate_titles.append(str(report_page_in_cr.get("title") or ""))
                candidate_urls.append(str(source.get("url") or ""))
                candidate_titles.append(str(source.get("title") or ""))

        for url in candidate_urls:
            lowered = url.lower()
            if "auth.manheim.com" in lowered or "/as/authorization" in lowered:
                self._write_payload_artifact(
                    request.vin,
                    "validation-failure-auth-redirect.json",
                    payload,
                )
                raise ValueError(
                    f"Refusing to push CR for VIN {request.vin}: captured page URL is "
                    f"a Manheim auth redirect ({url}); upstream OAuth handshake failed"
                )
        for title in candidate_titles:
            lowered = title.strip().lower()
            if lowered in {"sign in", "log in", "login"}:
                self._write_payload_artifact(
                    request.vin,
                    "validation-failure-signin-title.json",
                    payload,
                )
                raise ValueError(
                    f"Refusing to push CR for VIN {request.vin}: captured page title is "
                    f"{title!r}; CR capture landed on a login page"
                )

        # Rule 3: zero images on a CR page that advertises a multi-image
        # gallery. The "of N" / "1 of N" string is the OVE/Manheim gallery
        # counter widget — if it's present and N>1, we should have captured
        # at least one vehicle image.
        images = payload.get("images") or []
        if isinstance(cr_block, dict) and not images:
            body_text = ""
            if isinstance(report_page, dict):
                body_text = str(report_page.get("body_text") or "")
            advertises_gallery = bool(re.search(r"\b(?:1\s*of|of)\s*([2-9]|\d{2,})\b", body_text, re.IGNORECASE))
            if advertises_gallery:
                self._write_payload_artifact(
                    request.vin,
                    "validation-failure-empty-gallery.json",
                    payload,
                )
                raise ValueError(
                    f"Refusing to push CR for VIN {request.vin}: condition report present but "
                    "image gallery is empty while page text advertises a multi-image gallery"
                )

    def _retry_delay_for_not_found(self, reason: object) -> int:
        if reason == "ims_refresh_window":
            now = datetime.now(EASTERN_TZ)
            next_hour = now.replace(
                hour=self.settings.ims_refresh_end_hour_eastern,
                minute=0,
                second=0,
                microsecond=0,
            )
            if next_hour <= now:
                next_hour = now + timedelta(seconds=self.settings.deep_scrape_retry_delay_seconds)
            return max(60, int((next_hour - now).total_seconds()))
        return max(600, self.settings.deep_scrape_retry_delay_seconds)

    @contextmanager
    def _lease_heartbeat(self, api_client: VCHApiClient, request: PendingDetailRequest):
        stop_event = threading.Event()
        state = {"lost": False}
        thread: threading.Thread | None = None
        if self.settings.deep_scrape_lease_seconds > 0:
            interval_seconds = max(30, int(self.settings.deep_scrape_lease_seconds * 0.4))
            thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(api_client, request, stop_event, state, interval_seconds),
                name=f"ove-heartbeat-{request.request_id}",
                daemon=True,
            )
            thread.start()
        try:
            yield state
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=5)

    def _heartbeat_loop(
        self,
        api_client: VCHApiClient,
        request: PendingDetailRequest,
        stop_event: threading.Event,
        state: dict[str, bool],
        interval_seconds: int,
    ) -> None:
        while not stop_event.wait(interval_seconds):
            try:
                api_client.heartbeat_detail_request(
                    request.request_id,
                    worker_id=self.settings.detail_worker_id,
                    lease_seconds=self.settings.deep_scrape_lease_seconds,
                )
            except ApiClientError as exc:
                self.logger.warning(
                    "Lease heartbeat failed for request %s VIN %s: %s",
                    request.request_id,
                    request.vin,
                    exc,
                )
                if "status 409" in str(exc).lower():
                    state["lost"] = True
                stop_event.set()
                return

    def _load_not_found_tracker(self) -> dict[str, object]:
        if not self._not_found_tracker_path.exists():
            return {}
        try:
            return json.loads(self._not_found_tracker_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_not_found_tracker(self, tracker: dict[str, object]) -> None:
        self._not_found_tracker_path.parent.mkdir(parents=True, exist_ok=True)
        self._not_found_tracker_path.write_text(json.dumps(tracker, indent=2, sort_keys=True), encoding="utf-8")

    def _is_ims_refresh_window(self, now: datetime) -> bool:
        return self.settings.ims_refresh_start_hour_eastern <= now.hour < self.settings.ims_refresh_end_hour_eastern


def redact_detail(detail: DeepScrapeResult, request: PendingDetailRequest, settings: Settings) -> dict[str, object]:
    if contains_redacted_term(detail.seller_comments):
        raise ValueError("Redacted auction data detected in deep scrape payload")
    snapshot = redact_snapshot(detail.listing_snapshot)
    payload = {
        "source_platform": request.source_platform or settings.ove_source_platform,
        "images": build_detail_images(detail.images),
        "condition_report": detail.condition_report.model_dump(mode="json") if detail.condition_report else None,
        "seller_comments": sanitize_text(detail.seller_comments),
        "listing_snapshot": snapshot.model_dump(mode="json") if snapshot else None,
        "sync_metadata": {
            "request_id": request.request_id,
            "requested_at": request.requested_at.isoformat(),
            "request_source": request.request_source,
            "requested_by": request.requested_by,
            "reason": request.reason,
            "metadata": request.metadata,
            "scraper_node_id": settings.scraper_node_id,
            "scraper_version": settings.scraper_version,
        },
    }
    rendered = str(payload).lower()
    if any(term in rendered for term in REDACTED_TERMS):
        raise ValueError("Redacted auction data detected in deep scrape payload")
    return payload


def build_not_found_payload(request: PendingDetailRequest, settings: Settings, failure_reason: str) -> dict[str, object]:
    return {
        "source_platform": request.source_platform or settings.ove_source_platform,
        "images": [],
        "condition_report": {},
        "seller_comments": None,
        "listing_snapshot": {
            "title": None,
            "subtitle": None,
            "badges": [],
            "hero_facts": [],
            "sections": [],
            "icons": [],
            "page_url": None,
            "screenshot_refs": [],
            "raw_html_ref": None,
            "metadata": {
                "scrape_status": "not_found",
                "listing_available": False,
                "failure_category": "vin_not_found",
                "failure_reason": failure_reason,
            },
        },
        "sync_metadata": {
            "request_id": request.request_id,
            "requested_at": request.requested_at.isoformat(),
            "request_source": request.request_source,
            "requested_by": request.requested_by,
            "reason": request.reason,
            "metadata": request.metadata,
            "scraper_node_id": settings.scraper_node_id,
            "scraper_version": settings.scraper_version,
            "scrape_status": "not_found",
            "listing_available": False,
            "failure_category": "vin_not_found",
            "failure_reason": failure_reason,
            "completed_with_error": True,
        },
    }


def redact_snapshot(snapshot: ListingSnapshot | None) -> ListingSnapshot | None:
    if snapshot is None:
        return None

    hero_facts = [fact for fact in snapshot.hero_facts if is_safe_mapping(fact)]
    sections = []
    for section in snapshot.sections:
        title = sanitize_text(section.get("title"))
        if not title:
            continue
        items = [item for item in section.get("items", []) if is_safe_mapping(item)]
        subtitle = sanitize_text(section.get("subtitle"))
        sections.append(
            {
                "id": sanitize_text(section.get("id")),
                "title": title,
                "subtitle": subtitle,
                "layout": sanitize_text(section.get("layout")),
                "items": items,
                "metadata": section.get("metadata", {}),
            }
        )

    return ListingSnapshot(
        title=sanitize_text(snapshot.title),
        subtitle=sanitize_text(snapshot.subtitle),
        page_url=snapshot.page_url,
        badges=[badge for badge in snapshot.badges if is_safe_mapping(badge)],
        hero_facts=hero_facts,
        sections=sections,
        icons=[icon for icon in snapshot.icons if is_safe_mapping(icon)],
        raw_html_ref=snapshot.raw_html_ref,
        screenshot_refs=snapshot.screenshot_refs,
        metadata=snapshot.metadata,
    )


def build_detail_images(urls: list[str]) -> list[dict[str, object]]:
    images: list[dict[str, object]] = []
    for index, url in enumerate(urls):
        image = DetailImage(
            url=url,
            role="hero" if index == 0 else "gallery",
            display_order=index,
            is_primary=index == 0,
        )
        images.append(image.model_dump(mode="json"))
    return images


def is_safe_mapping(value: dict[str, object]) -> bool:
    rendered = str(value)
    return not contains_redacted_term(rendered)


def sanitize_text(value: str | None) -> str | None:
    if value is None:
        return None
    if contains_redacted_term(value):
        return None
    return value.strip() or None


def contains_redacted_term(value: str | None) -> bool:
    if value is None:
        return False
    lowered = value.lower()
    return any(term in lowered for term in REDACTED_TERMS)

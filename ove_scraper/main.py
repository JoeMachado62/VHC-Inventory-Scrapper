from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from ove_scraper.automation_lock import AutomationLockBusyError, OveAutomationLock, lock_name_for_port
from ove_scraper.browser import BrowserSessionError, SavedSearchPageEmpty
from ove_scraper.api_client import VCHApiClient
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings, load_env_file
from ove_scraper.deep_scrape import DeepScrapeWorker, redact_detail
from ove_scraper.keep_awake import KeepAwake
from ove_scraper.logging_utils import configure_logging
from ove_scraper.notifier import AdminNotifier
from ove_scraper.schemas import PendingDetailRequest
from ove_scraper.sync_service import HourlySyncRunner

EASTERN_TZ = ZoneInfo("America/New_York")

# Process-wide shutdown event flipped by SIGINT / SIGTERM / SIGBREAK.
# The main loop checks this between ticks so taskkill / Ctrl+C / Task
# Scheduler "stop" can drain in-flight work cleanly instead of being
# killed mid-scrape and leaving claims dangling on the VPS.
SHUTDOWN_EVENT = threading.Event()

# Auth-failure circuit breaker.  When recover_browser_session raises
# BrowserSessionError repeatedly (auth genuinely gone, not a transient
# CDP glitch), the main loop backs off exponentially instead of
# hammering OVE every few seconds and risking an account ban.
#
#   _AUTH_FAIL_COUNT   backoff (seconds)
#   1                  30
#   2                  60
#   3                  120
#   4                  240
#   5+                 300  (cap — parked until manual intervention)
#
_AUTH_FAIL_COUNT = 0
_AUTH_FAIL_MAX_BACKOFF = 300          # 5 minutes cap
_AUTH_FAIL_PARK_THRESHOLD = 5         # after 5 consecutive failures, park

# Shared mutable heartbeat state. The main loop updates these fields as
# it observes new events (sync_ok, poll_ok, claim taken, etc); the
# background heartbeat ticker reads them every 30s and POSTs them to
# the VPS. Heartbeats CANNOT be coupled to poll/sync ticks because the
# hourly sync holds the OveAutomationLock for many minutes and would
# block heartbeats long enough to trip the VPS health endpoint to
# warning (5 min) and critical (15 min) while real work is in progress.
_HEARTBEAT_STATE: dict[str, Any] = {
    "last_sync_at": None,
    "last_poll_at": None,
    "last_claim_at": None,
    "pending_claims": None,
    "status_note": "starting",
}
_HEARTBEAT_INTERVAL_SECONDS = 30


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _install_signal_handlers(logger) -> None:
    def _handler(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        logger.warning("Received signal %s; flipping shutdown event", name)
        SHUTDOWN_EVENT.set()

    # SIGINT works on every platform (Ctrl+C). SIGTERM is POSIX. SIGBREAK
    # is what Windows Task Scheduler sends when stopping a task — it is
    # the closest thing to SIGTERM on Windows. Install whichever exists.
    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _handler)
        except (ValueError, OSError):
            pass
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _handler)
        except (ValueError, OSError):
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OVE scraper module")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("sync-once")
    subparsers.add_parser("poll-once")
    subparsers.add_parser("run")
    scrape_vin = subparsers.add_parser("scrape-vin")
    scrape_vin.add_argument("vin")
    scrape_vin.add_argument("--output", default="")
    subparsers.add_parser("hot-deal", help="Run Hot Deal vehicle screening pipeline")
    reprocess = subparsers.add_parser(
        "hot-deal-reprocess",
        help=(
            "One-shot recovery: reclassify prior step1_fail rows with scraper-error "
            "reasons back to 'scrape_failed' so the next Hot Deal run re-screens them. "
            "Also re-screens any rows in the DB whose rejection predates the current "
            "screener fixes."
        ),
    )
    reprocess.add_argument(
        "--rescreen",
        action="store_true",
        help=(
            "Also re-run the screener against every current step1_fail VIN's cr_data "
            "using today's screening logic; promote any that now pass back to pending."
        ),
    )
    return parser


def main() -> None:
    global _AUTH_FAIL_COUNT  # must appear before any assignment in this scope
    # Suppress Node.js deprecation warnings from Playwright
    os.environ["NODE_OPTIONS"] = "--no-deprecation"

    load_env_file()
    args = build_parser().parse_args()
    settings = Settings.from_env()
    logger = configure_logging(settings.log_level, settings.log_file_path)
    _install_signal_handlers(logger)
    browser, api_client, sync_runner, deep_scrape_worker, notifier = build_runtime(settings, logger)

    try:
        with KeepAwake(logger):
            if args.command == "sync-once":
                run_sync_once_with_recovery(settings, browser, api_client, logger, notifier=notifier)
                return

            if args.command == "poll-once":
                run_poll_once_with_recovery(settings, browser, api_client, logger, notifier=notifier)
                return

            if args.command == "hot-deal":
                from ove_scraper.hot_deal_db import init_db
                from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner
                db_conn = init_db(settings.hot_deal_db_path)
                try:
                    runner = HotDealPipelineRunner(
                        settings=settings, browser=browser, db_conn=db_conn,
                        log=logger, notifier=notifier,
                        api_client=api_client,
                    )
                    result = runner.run_once()
                    logger.info("Hot Deal pipeline finished: %s", result.get("status", "unknown"))
                finally:
                    db_conn.close()
                return

            if args.command == "hot-deal-reprocess":
                run_hot_deal_reprocess(settings, logger, rescreen=args.rescreen)
                return

            if args.command == "scrape-vin":
                detail = browser.deep_scrape_vin(args.vin)
                synthetic_request = PendingDetailRequest.model_validate(
                    {
                        "request_id": "manual",
                        "vin": args.vin,
                        "source_platform": settings.ove_source_platform,
                        "status": "MANUAL",
                        "priority": 100,
                        "attempts": 0,
                        "requested_at": "2026-03-08T00:00:00+00:00",
                        "request_source": "manual",
                        "requested_by": "codex",
                        "reason": "manual scrape-vin",
                        "metadata": {},
                    }
                )
                payload = redact_detail(detail, synthetic_request, settings)
                output = Path(args.output) if args.output else settings.artifact_dir / args.vin / "payload.json"
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
                logger.info("Wrote VIN payload to %s", output)
                return

            next_sync_at = 0.0
            next_poll_at = 0.0
            next_keepalive_at = 0.0
            # Hot Deal daily pipeline tick. We check once a minute whether
            # today's scheduled slot has been reached AND today hasn't
            # already run (or has failed and is within the retry budget).
            # The actual decision lives in should_run_hot_deal_now() which
            # reads artifacts/_state/hot_deal_daily_state.json — the state
            # file is the single source of truth for "did today run yet",
            # so a reboot in the middle of the day still produces one
            # run per Eastern calendar day.
            next_hot_deal_check_at = 0.0
            # Heartbeat ticker is INDEPENDENT of poll/sync. The hourly
            # sync can hold the OveAutomationLock for up to 30 minutes,
            # during which the polling tick is skipped by the busy-lock
            # path — so a heartbeat coupled to polling would let the VPS
            # /health endpoint trip warning (5min) and critical (15min)
            # while the scraper is doing real work. Heartbeats are pure
            # HTTP, never touch the browser, never need the lock; we
            # tick them every 30s on their own clock.
            next_heartbeat_at = 0.0
            heartbeat_thread = threading.Thread(
                target=_heartbeat_ticker,
                args=(settings, logger),
                name="ove-heartbeat-ticker",
                daemon=True,
            )
            heartbeat_thread.start()

            while not SHUTDOWN_EVENT.is_set():
                try:
                    now = time.monotonic()

                    if now >= next_sync_at:
                        if is_within_sync_window(settings):
                            sync_result = run_sync_once_with_recovery(
                                settings,
                                browser,
                                api_client,
                                logger,
                                sync_runner=sync_runner,
                                notifier=notifier,
                            )
                            _AUTH_FAIL_COUNT = 0  # reset circuit breaker on success
                            if sync_result is not None and getattr(sync_result, "execution_status", None) == "Success":
                                _HEARTBEAT_STATE["last_sync_at"] = _utc_now_iso()
                                _HEARTBEAT_STATE["status_note"] = "snapshot_ok"
                            wait_seconds = seconds_until_next_scheduled_sync(settings)
                            next_sync_at = now + wait_seconds
                            logger.info(
                                "Next OVE saved-searches sync scheduled in %.0f seconds",
                                wait_seconds,
                            )
                        else:
                            pause_seconds = seconds_until_next_sync_window(settings)
                            logger.info(
                                "Hourly sync paused outside OVE sync window; resuming in %.0f seconds",
                                pause_seconds,
                            )
                            next_sync_at = now + pause_seconds

                    if SHUTDOWN_EVENT.is_set():
                        break

                    if now >= next_poll_at:
                        poll_result = run_poll_once_with_recovery(
                            settings,
                            browser,
                            api_client,
                            logger,
                            deep_scrape_worker=deep_scrape_worker,
                            notifier=notifier,
                        )
                        _AUTH_FAIL_COUNT = 0  # reset circuit breaker on success
                        _HEARTBEAT_STATE["last_poll_at"] = _utc_now_iso()
                        if isinstance(poll_result, list) and poll_result:
                            _HEARTBEAT_STATE["last_claim_at"] = _HEARTBEAT_STATE["last_poll_at"]
                            _HEARTBEAT_STATE["pending_claims"] = len(poll_result)
                        else:
                            _HEARTBEAT_STATE["pending_claims"] = 0
                        _HEARTBEAT_STATE["status_note"] = "poll_ok"
                        next_poll_at = now + settings.deep_scrape_poll_interval_seconds

                    if SHUTDOWN_EVENT.is_set():
                        break

                    if now >= next_keepalive_at:
                        run_browser_operation(
                            settings,
                            browser,
                            logger,
                            browser.touch_session,
                            "browser keepalive",
                            notifier=notifier,
                        )
                        next_keepalive_at = now + settings.browser_keepalive_interval_seconds

                    if SHUTDOWN_EVENT.is_set():
                        break

                    if now >= next_hot_deal_check_at:
                        if settings.hot_deal_enabled:
                            decision = should_run_hot_deal_now(settings, logger)
                            if decision["should_run"]:
                                logger.info(
                                    "Hot Deal auto-run: firing (%s)", decision["reason"],
                                )
                                run_hot_deal_with_recovery(
                                    settings, browser, logger,
                                    notifier=notifier,
                                    api_client=api_client,
                                )
                            else:
                                logger.debug(
                                    "Hot Deal auto-run: skipping (%s)", decision["reason"],
                                )
                        # Re-check every 60s whether the slot has arrived,
                        # or whether a retry-delay has elapsed. Cheap: just
                        # reads a JSON file and a clock.
                        next_hot_deal_check_at = now + 60.0

                    # Wait on the shutdown event instead of a plain sleep so
                    # SIGINT/SIGTERM/SIGBREAK wake the loop within 1s.
                    SHUTDOWN_EVENT.wait(timeout=1.0)
                except BrowserSessionError as exc:
                    _AUTH_FAIL_COUNT += 1
                    backoff = min(30 * (2 ** (_AUTH_FAIL_COUNT - 1)), _AUTH_FAIL_MAX_BACKOFF)
                    _HEARTBEAT_STATE["status_note"] = f"auth_fail_{_AUTH_FAIL_COUNT}"

                    if _AUTH_FAIL_COUNT >= _AUTH_FAIL_PARK_THRESHOLD:
                        logger.error(
                            "Auth failed %d consecutive times (parked). "
                            "Manual login required — scraper will exit. Backoff was %ds.",
                            _AUTH_FAIL_COUNT, backoff,
                        )
                        if notifier is not None:
                            notifier.notify_browser_auth_lost(
                                reason=f"PARKED after {_AUTH_FAIL_COUNT} consecutive auth failures: {exc}",
                                context={
                                    "chrome_debug_host": settings.chrome_debug_host,
                                    "chrome_debug_port": settings.chrome_debug_port,
                                    "node_id": settings.scraper_node_id,
                                    "consecutive_failures": _AUTH_FAIL_COUNT,
                                },
                                logger=logger,
                            )
                        SHUTDOWN_EVENT.set()
                        break

                    logger.warning(
                        "Auth failure #%d; backing off %ds before retry: %s",
                        _AUTH_FAIL_COUNT, backoff, exc,
                    )
                    try:
                        api_client.close()
                    finally:
                        if hasattr(browser, "close"):
                            browser.close()
                    if SHUTDOWN_EVENT.wait(timeout=backoff):
                        break
                    browser, api_client, sync_runner, deep_scrape_worker, notifier = build_runtime(settings, logger)

                except Exception as exc:
                    logger.exception("OVE main loop crashed (non-auth); rebuilding runtime: %s", exc)
                    try:
                        api_client.close()
                    finally:
                        if hasattr(browser, "close"):
                            browser.close()
                    if SHUTDOWN_EVENT.wait(timeout=5.0):
                        break
                    browser, api_client, sync_runner, deep_scrape_worker, notifier = build_runtime(settings, logger)
            logger.info("Shutdown event observed; exiting main loop cleanly")
    finally:
        # Make sure SHUTDOWN_EVENT is set so the background heartbeat
        # ticker exits its sleep promptly. Already set if we got here
        # via signal, but defensive against the crash-and-rethrow path.
        SHUTDOWN_EVENT.set()
        try:
            released = deep_scrape_worker.release_in_flight_claims()
            if released:
                logger.warning("Released %s in-flight detail claims on shutdown", released)
        except Exception as exc:
            logger.warning("Failed to release in-flight claims on shutdown: %s", exc)
        try:
            _HEARTBEAT_STATE["status_note"] = "shutting_down"
            send_heartbeat(
                api_client,
                settings,
                logger,
                last_sync_at=_HEARTBEAT_STATE.get("last_sync_at"),
                last_poll_at=_HEARTBEAT_STATE.get("last_poll_at"),
                last_claim_at=_HEARTBEAT_STATE.get("last_claim_at"),
                pending_claims=_HEARTBEAT_STATE.get("pending_claims"),
                status_note="shutting_down",
            )
        except Exception:
            pass
        api_client.close()
        if hasattr(browser, "close"):
            browser.close()


def build_runtime(settings: Settings, logger):
    """Construct the runtime objects. When `settings.chrome_debug_port_sync`
    is set (Path 2 / two-Chrome architecture, 2026-04-26), the saved-search
    sync runs on a SECOND Chrome instance with its own browser session,
    its own settings copy (so `chrome_debug_port` points at the secondary
    port), and its own automation-lock mutex (so it never contends with
    the primary Chrome that hosts hot-deal + deep-scrape work).
    """
    primary_browser = PlaywrightCdpBrowserSession(settings)
    api_client = VCHApiClient(settings.vch_api_base_url, settings.vch_service_token)
    notifier = AdminNotifier(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_username=settings.smtp_username,
        smtp_password=settings.smtp_password,
        smtp_use_tls=settings.smtp_use_tls,
        from_email=settings.alert_from_email,
        admin_alert_email=settings.admin_alert_email,
        cooldown_seconds=settings.admin_alert_cooldown_seconds,
    )

    if settings.chrome_debug_port_sync and settings.chrome_debug_port_sync != settings.chrome_debug_port:
        # Path 2: clone Settings with the secondary port so the sync's
        # PlaywrightCdpBrowserSession connects to Login B's Chrome. Every
        # other field — VCH API URL, sync schedule, saved-search names,
        # Eastern-time window — is identical, so the sync runner's
        # business logic is unchanged.
        from dataclasses import replace as _dataclass_replace
        sync_settings = _dataclass_replace(
            settings, chrome_debug_port=settings.chrome_debug_port_sync,
        )
        sync_browser = PlaywrightCdpBrowserSession(sync_settings)
        logger.info(
            "Two-Chrome mode: sync runner -> port %d (Login B), "
            "hot-deal/deep-scrape -> port %d (Login A)",
            settings.chrome_debug_port_sync,
            settings.chrome_debug_port,
        )
        sync_runner = HourlySyncRunner(sync_settings, sync_browser, api_client, logger, notifier=notifier)
    else:
        sync_browser = primary_browser
        sync_runner = HourlySyncRunner(settings, primary_browser, api_client, logger, notifier=notifier)

    deep_scrape_worker = DeepScrapeWorker(api_client, primary_browser, logger, settings, notifier=notifier)
    logger.info("Configured deep-scrape worker pool size: %s", settings.deep_scrape_max_workers)
    # Tuple is intentionally backwards-compatible — same arity as before.
    # `sync_browser` is reachable via sync_runner.browser; callers that
    # need to drive sync recovery use the runner, not the raw browser.
    return primary_browser, api_client, sync_runner, deep_scrape_worker, notifier


def run_sync_once_with_recovery(
    settings: Settings,
    browser: PlaywrightCdpBrowserSession,
    api_client: VCHApiClient,
    logger,
    sync_runner: HourlySyncRunner | None = None,
    notifier: AdminNotifier | None = None,
):
    # Path 2: when a separate sync_runner is supplied (the typical case
    # after build_runtime), it carries its own browser pointed at the
    # secondary Chrome. Use that browser + a port-derived lock name so
    # the mutex doesn't collide with the primary Chrome's lock.
    if sync_runner is not None:
        sync_browser = sync_runner.browser
        sync_settings = sync_runner.settings
    else:
        sync_runner = HourlySyncRunner(settings, browser, api_client, logger, notifier=notifier)
        sync_browser = browser
        sync_settings = settings
    return run_browser_operation(
        sync_settings,
        sync_browser,
        logger,
        sync_runner.run_once,
        "hourly sync",
        notifier=notifier,
        lock_name=lock_name_for_port(sync_settings.chrome_debug_port),
    )


def send_heartbeat(
    api_client: VCHApiClient,
    settings: Settings,
    logger,
    *,
    last_sync_at: str | None = None,
    last_poll_at: str | None = None,
    last_claim_at: str | None = None,
    pending_claims: int | None = None,
    status_note: str | None = None,
) -> None:
    """Best-effort heartbeat. Never raises — see VCHApiClient.send_scraper_heartbeat."""
    try:
        result = api_client.send_scraper_heartbeat(
            worker_id=settings.detail_worker_id,
            profile=settings.scraper_profile_slug,
            scraper_version=settings.scraper_version,
            node_id=settings.scraper_node_id,
            last_sync_at=last_sync_at,
            last_poll_at=last_poll_at,
            last_claim_at=last_claim_at,
            pending_claims=pending_claims,
            status_note=status_note,
        )
        if result is None:
            logger.debug("Heartbeat to VPS returned None (transient or non-200)")
    except Exception as exc:
        logger.debug("Heartbeat call raised unexpectedly: %s", exc)


def _heartbeat_ticker(settings: Settings, logger) -> None:
    """Background daemon thread that POSTs a heartbeat every
    _HEARTBEAT_INTERVAL_SECONDS based on the latest _HEARTBEAT_STATE.

    IMPORTANT: this thread creates its OWN lightweight httpx client for
    heartbeats instead of sharing the main loop's VCHApiClient. This is
    critical because the main loop's crash-recovery path tears down the
    old api_client and builds a new one — if the ticker held a reference
    to the old client, every heartbeat after a crash-recovery would fail
    on a closed connection. The standalone client is cheap (one POST
    every 30s) and immune to the main-loop rebuild cycle.
    """
    import httpx as _httpx

    logger.info("Heartbeat ticker started (interval=%ss)", _HEARTBEAT_INTERVAL_SECONDS)
    base_url = settings.vch_api_base_url.rstrip("/")
    headers = {"X-Service-Token": settings.vch_service_token, "Content-Type": "application/json"}
    endpoint = f"{base_url}/inventory/ove/scraper-heartbeat"

    def _tick(note_override: str | None = None) -> None:
        body: dict[str, Any] = {
            "worker_id": settings.detail_worker_id,
            "profile": settings.scraper_profile_slug,
            "scraper_version": settings.scraper_version,
            "node_id": settings.scraper_node_id,
        }
        for key in ("last_sync_at", "last_poll_at", "last_claim_at", "pending_claims"):
            val = _HEARTBEAT_STATE.get(key)
            if val is not None:
                body[key] = val
        body["status_note"] = note_override or _HEARTBEAT_STATE.get("status_note") or "ticker"
        try:
            resp = _httpx.post(endpoint, json=body, headers=headers, timeout=10.0)
            if resp.status_code >= 400:
                logger.debug("Heartbeat tick returned %s", resp.status_code)
        except Exception as exc:
            logger.debug("Heartbeat tick failed: %s", exc)

    _tick(note_override="ticker_boot")
    while not SHUTDOWN_EVENT.wait(timeout=_HEARTBEAT_INTERVAL_SECONDS):
        _tick()
    logger.info("Heartbeat ticker exiting on shutdown event")


def run_poll_once_with_recovery(
    settings: Settings,
    browser: PlaywrightCdpBrowserSession,
    api_client: VCHApiClient,
    logger,
    deep_scrape_worker: DeepScrapeWorker | None = None,
    notifier: AdminNotifier | None = None,
):
    worker = deep_scrape_worker or DeepScrapeWorker(api_client, browser, logger, settings)
    return run_browser_operation(settings, browser, logger, worker.process_pending_once, "detail poll", notifier=notifier)


def run_browser_operation(
    settings: Settings,
    browser,
    logger,
    operation,
    operation_name: str,
    notifier: AdminNotifier | None = None,
    lock_name: str | None = None,
):
    """Run a browser-driven operation under the OveAutomationLock.

    `lock_name` lets the caller scope the mutex per-Chrome (Path 2
    / two-Chrome architecture). When None, the historical default
    (`Local\\OVE_Browser_Automation`) is used so single-Chrome callers
    are byte-identical to prior behavior.
    """
    if lock_name is None:
        lock_name = lock_name_for_port(settings.chrome_debug_port)
    try:
        with OveAutomationLock(name=lock_name, timeout_seconds=_browser_operation_lock_timeout_seconds(operation_name)):
            ensure_browser_session(settings, browser, logger, notifier=notifier)
            try:
                return operation()
            except SavedSearchPageEmpty as exc:
                # "No Saved Searches" means OVE accepted the session cookie
                # but the token is stale — the backend returns zero searches
                # instead of redirecting to login.  Reloading the same
                # profile just reloads the same bad cookies.  The fix is to
                # clear cookies so Chrome starts a fresh login on relaunch.
                logger.warning(
                    "%s detected stale OVE session (empty saved searches): "
                    "clearing cookies and relaunching browser",
                    operation_name,
                )
                _clear_chrome_cookies(settings, browser, logger)
                recover_browser_session(settings, browser, logger, notifier=notifier)
                return operation()
            except BrowserSessionError as exc:
                logger.warning("%s lost browser session: %s", operation_name, exc)
                recover_browser_session(settings, browser, logger, notifier=notifier)
                return operation()
    except AutomationLockBusyError as exc:
        logger.warning("%s skipped because another OVE automation task is active: %s", operation_name, exc)
        return None


def ensure_browser_session(settings: Settings, browser, logger, notifier: AdminNotifier | None = None) -> None:
    try:
        browser.ensure_session()
    except BrowserSessionError as exc:
        logger.warning("OVE browser session unavailable: %s", exc)
        recover_browser_session(settings, browser, logger, notifier=notifier)


def recover_browser_session(settings: Settings, browser, logger, notifier: AdminNotifier | None = None) -> None:
    browser.close()
    _kill_stale_chrome(settings, logger)
    launch_browser_script(logger)
    wait_for_cdp(settings, logger, timeout_seconds=45)
    try:
        browser.ensure_session()
    except BrowserSessionError as exc:
        if notifier is not None:
            notifier.notify_browser_auth_lost(
                reason=str(exc),
                context={
                    "chrome_debug_host": settings.chrome_debug_host,
                    "chrome_debug_port": settings.chrome_debug_port,
                    "node_id": settings.scraper_node_id,
                },
                logger=logger,
            )
        raise


def _kill_stale_chrome(settings: Settings, logger) -> None:
    """Kill any Chrome process using the CDP profile that is no longer
    listening on the debug port. Belt-and-suspenders with the TCP check
    in start_ove_browser.ps1 — this Python-side kill runs BEFORE the
    PS1 script so the script's 'existing process' check sees a clean
    state. Failures here are non-fatal; the PS1 script's own check is
    the fallback.
    """
    try:
        result = subprocess.run(
            [
                "powershell",
                "-Command",
                (
                    "Get-CimInstance Win32_Process | "
                    "Where-Object { $_.Name -eq 'chrome.exe' -and "
                    "$_.CommandLine -like '*--remote-debugging-port=9222*' -and "
                    "$_.CommandLine -like '*chrome-cdp-profile*' } | "
                    "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
                ),
            ],
            capture_output=True,
            timeout=15,
        )
        logger.info("Killed stale Chrome CDP processes (exit=%s)", result.returncode)
        time.sleep(2)
    except Exception as exc:
        logger.warning("Failed to kill stale Chrome processes: %s", exc)


_CHROME_CDP_PROFILE = Path("C:/chrome-cdp-profile")


def _clear_chrome_cookies(settings: Settings, browser, logger) -> None:
    """Kill Chrome and delete its cookie files so the next launch starts
    a fresh OVE login.  This is the programmatic equivalent of the manual
    fix: clearing browser cookies then re-logging in.

    Called when OVE shows 'No Saved Searches' — the session cookie looks
    valid to OVE's page shell (no login redirect) but the backend token
    is stale, so it silently returns zero searches.
    """
    browser.close()
    _kill_stale_chrome(settings, logger)

    cookie_files = [
        _CHROME_CDP_PROFILE / "Default" / "Network" / "Cookies",
        _CHROME_CDP_PROFILE / "Default" / "Network" / "Cookies-journal",
    ]
    for path in cookie_files:
        try:
            if path.exists():
                path.unlink()
                logger.info("Deleted stale cookie file: %s", path)
        except Exception as exc:
            logger.warning("Failed to delete cookie file %s: %s", path, exc)


def launch_browser_script(logger) -> None:
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "start_ove_browser.ps1"
    if not script_path.exists():
        raise BrowserSessionError(f"Browser launcher script not found: {script_path}")

    logger.info("Launching OVE browser via %s", script_path)
    subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
        ],
        check=True,
    )


def wait_for_cdp(settings: Settings, logger, timeout_seconds: int = 30) -> None:
    endpoint = f"http://{settings.chrome_debug_host}:{settings.chrome_debug_port}/json/version"
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        try:
            with urlopen(endpoint, timeout=3) as response:
                if response.status == 200:
                    return
        except URLError:
            time.sleep(1)
            continue

    logger.error("Chrome CDP endpoint did not come online: %s", endpoint)
    raise BrowserSessionError(f"Chrome CDP endpoint unavailable at {endpoint}")


def _browser_operation_lock_timeout_seconds(operation_name: str) -> int:
    lowered = operation_name.strip().lower()
    if "sync" in lowered:
        return 1800
    if "poll" in lowered:
        return 300
    if "keepalive" in lowered:
        return 60
    return 900


def is_within_sync_window(settings: Settings, now: datetime | None = None) -> bool:
    current = now or datetime.now(EASTERN_TZ)
    return settings.sync_window_start_hour_eastern <= current.hour < settings.sync_window_end_hour_eastern


def seconds_until_next_sync_window(settings: Settings, now: datetime | None = None) -> float:
    current = now or datetime.now(EASTERN_TZ)
    start_today = current.replace(
        hour=settings.sync_window_start_hour_eastern,
        minute=0,
        second=0,
        microsecond=0,
    )
    if current < start_today:
        target = start_today
    else:
        target = start_today + timedelta(days=1)
    return max(1.0, (target - current).total_seconds())


def seconds_until_next_scheduled_sync(
    settings: Settings, now: datetime | None = None
) -> float:
    """Return seconds from `now` until the next wall-clock slot in
    `settings.sync_schedule_eastern`. If no slot remains today, roll to the
    first slot tomorrow. Clamped to >= 1.0 so the main loop never busy-loops.
    """
    current = now or datetime.now(EASTERN_TZ)
    slots = settings.sync_schedule_eastern
    if not slots:
        # Defensive: empty schedule should never happen, but if it does,
        # fall back to the legacy interval so the loop still makes progress.
        return float(settings.sync_interval_seconds)
    today_slots = [
        current.replace(hour=h, minute=m, second=0, microsecond=0)
        for h, m in slots
    ]
    future_today = [slot for slot in today_slots if slot > current]
    if future_today:
        target = future_today[0]
    else:
        first_h, first_m = slots[0]
        target = (current + timedelta(days=1)).replace(
            hour=first_h, minute=first_m, second=0, microsecond=0
        )
    return max(1.0, (target - current).total_seconds())


def run_hot_deal_reprocess(settings: Settings, logger, *, rescreen: bool) -> None:
    """One-shot recovery pass for Hot Deal DB.

    Two kinds of reclassification:

    1. Rows with status='step1_fail' whose rejection_reason is a known
       scraper-side error (CR-click failure, listing-not-found) get
       moved to status='scrape_failed'. The next normal Hot Deal run
       will reset those back to pending and re-screen them.

    2. With --rescreen, every remaining step1_fail row whose cr_data
       was captured is re-run through the current screener. Rows that
       now pass are moved back to 'pending' so the next run re-screens
       them end-to-end (CR + AutoCheck + web search). Those that still
       fail stay in step1_fail with the new reason. This is how the
       2026-04-23 run's 50 false-positive VINs get recovered without
       burning another day on them.
    """
    from ove_scraper.hot_deal_db import (
        init_db,
        reclassify_scraper_failures_as_scrape_failed,
    )

    db_conn = init_db(settings.hot_deal_db_path)
    try:
        moved = reclassify_scraper_failures_as_scrape_failed(db_conn)
        logger.info(
            "Hot Deal reprocess: %d scraper-failed VIN(s) moved to scrape_failed for retry",
            moved,
        )

        if not rescreen:
            logger.info(
                "Hot Deal reprocess done (scraper-error reclassification only). "
                "Next daily run will re-screen them."
            )
            return

        # --rescreen: re-run screener against stored cr_data for each
        # step1_fail row. Because the screener only needs
        # ConditionReport + listing_json (not a live browser), this is
        # pure computation against DB rows.
        import json as _json
        from ove_scraper.hot_deal_screener import screen_condition_report
        from ove_scraper.schemas import ConditionReport

        rows = db_conn.execute(
            "SELECT vin, cr_data, rejection_reason FROM hot_deal_vins "
            "WHERE status='step1_fail'"
        ).fetchall()
        promoted = 0
        still_failing = 0
        no_data = 0
        for row in rows:
            cr_json_str = row["cr_data"]
            if not cr_json_str:
                no_data += 1
                continue
            try:
                stored = _json.loads(cr_json_str)
            except _json.JSONDecodeError:
                no_data += 1
                continue
            # cr_data in the DB only stores {"passed": ..., "reason": ...}.
            # The full CR dump isn't persisted (see hot_deal_pipeline.py:224),
            # so we can't re-run the full screener here — but we CAN tell
            # whether the old reason matches one of the known false-positive
            # patterns and move the VIN back to pending for a fresh scrape.
            old_reason = (row["rejection_reason"] or "").strip()
            # Prefix-match known screener false-positive classes that
            # have since been fixed. Prefixes (not exact strings) because
            # the 2026-04-24 screener change started appending the
            # matching phrase / (system: condition) detail onto the
            # reason string for post-mortem clarity. Intentionally
            # narrow — "Branded title" / "Structural damage" / "TMU" /
            # "Windshield damage" are never auto-promoted back; a
            # human decides if those were wrong.
            known_false_positive_prefixes = (
                "Engine/drivetrain issue detected",
                "Mechanical finding: engine/drivetrain concern",
            )
            if any(old_reason.startswith(p) for p in known_false_positive_prefixes):
                db_conn.execute(
                    "UPDATE hot_deal_vins SET status='pending', "
                    "rejection_step=NULL, rejection_reason=NULL, cr_data=NULL "
                    "WHERE vin=?",
                    (row["vin"],),
                )
                promoted += 1
            else:
                still_failing += 1
        db_conn.commit()
        logger.info(
            "Hot Deal reprocess (--rescreen): %d VIN(s) promoted pending->re-screen, "
            "%d stay step1_fail, %d had no cr_data to re-evaluate",
            promoted, still_failing, no_data,
        )
        logger.info(
            "Next daily run will re-screen %d VIN(s). Tomorrow's 07:00 ET "
            "auto-run will pick these up automatically.",
            moved + promoted,
        )
    finally:
        db_conn.close()


def _hot_deal_state_path(settings: Settings) -> Path:
    return settings.artifact_dir / "_state" / "hot_deal_daily_state.json"


def _load_hot_deal_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_hot_deal_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _is_within_ims_refresh_window(settings: Settings, now: datetime) -> bool:
    return settings.ims_refresh_start_hour_eastern <= now.hour < settings.ims_refresh_end_hour_eastern


def should_run_hot_deal_now(
    settings: Settings,
    logger,
    now: datetime | None = None,
) -> dict:
    """Decide whether the main loop should kick off today's Hot Deal run.

    Reads the persisted state file and answers "yes + why" or "no + why".
    Exposes the reason so the log trail on skipped checks is legible and
    so ops can diagnose why a day's run didn't fire. Single source of
    truth for the "one run per Eastern calendar day" invariant.
    """
    current = now or datetime.now(EASTERN_TZ)
    today_str = current.date().isoformat()

    slots = settings.hot_deal_daily_schedule_eastern
    if not slots:
        return {"should_run": False, "reason": "no_schedule_configured"}

    # "First slot today that has already passed" — all slots are expressed
    # in Eastern time and the pipeline runs only once per day, so the
    # earliest-past slot is the trigger boundary. A slot of (7, 0) means
    # "eligible to run any time at or after 7:00 AM Eastern today".
    today_slots_as_dt = [
        current.replace(hour=h, minute=m, second=0, microsecond=0)
        for h, m in slots
    ]
    past_slots = [slot for slot in today_slots_as_dt if slot <= current]
    if not past_slots:
        return {"should_run": False, "reason": "before_first_slot_today"}

    if _is_within_ims_refresh_window(settings, current):
        # Saved-search exports are backed by Manheim IMS which goes into a
        # refresh window 4-5 PM ET and returns transient "not found" results.
        # Defer until the window closes (memory: manheim_ims_refresh_window).
        return {"should_run": False, "reason": "ims_refresh_window"}

    state = _load_hot_deal_state(_hot_deal_state_path(settings))
    last_date = state.get("last_run_date_eastern")
    last_status = state.get("last_run_status")
    attempts_today = int(state.get("attempts_today", 0) or 0)

    if last_date != today_str:
        # Either never ran, or last ran on a previous day. New day = fresh
        # attempt budget regardless of yesterday's outcome.
        return {"should_run": True, "reason": "new_day"}

    if last_status == "completed":
        return {"should_run": False, "reason": "already_completed_today"}

    if last_status == "started":
        last_run_at = _parse_iso_datetime(state.get("last_run_at"))
        if last_run_at is None:
            logger.warning("Hot Deal state has status=started but no parseable last_run_at; treating as stale")
            return {"should_run": True, "reason": "stale_started_unparseable"}
        age_seconds = (current - last_run_at).total_seconds()
        if age_seconds >= settings.hot_deal_stale_start_seconds:
            return {
                "should_run": True,
                "reason": f"stale_started_{int(age_seconds)}s_old",
            }
        return {
            "should_run": False,
            "reason": f"run_in_progress_{int(age_seconds)}s_elapsed",
        }

    # last_status == "failed" (or anything else we didn't expect)
    if attempts_today >= settings.hot_deal_max_daily_attempts:
        return {
            "should_run": False,
            "reason": f"daily_attempt_cap_reached_{attempts_today}",
        }
    last_run_at = _parse_iso_datetime(state.get("last_run_at"))
    if last_run_at is not None:
        elapsed = (current - last_run_at).total_seconds()
        if elapsed < settings.hot_deal_retry_delay_seconds:
            return {
                "should_run": False,
                "reason": f"retry_cooldown_{int(settings.hot_deal_retry_delay_seconds - elapsed)}s_remaining",
            }
    return {
        "should_run": True,
        "reason": f"retry_attempt_{attempts_today + 1}_of_{settings.hot_deal_max_daily_attempts}",
    }


def _parse_iso_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN_TZ)
    return parsed.astimezone(EASTERN_TZ)


def run_hot_deal_with_recovery(
    settings: Settings,
    browser,
    logger,
    notifier: AdminNotifier | None = None,
    api_client: VCHApiClient | None = None,
) -> None:
    """Execute one Hot Deal pipeline run, persist state, and surface
    failures. Mirrors run_sync_once_with_recovery / run_poll_once_with_recovery
    in that BrowserSessionError is intentionally NOT caught — it propagates
    to the main-loop auth handler so the session can be rebuilt with the
    normal circuit-breaker backoff."""
    from ove_scraper.hot_deal_db import init_db
    from ove_scraper.hot_deal_pipeline import HotDealPipelineRunner

    state_path = _hot_deal_state_path(settings)
    state = _load_hot_deal_state(state_path)
    now_eastern = datetime.now(EASTERN_TZ)
    today_str = now_eastern.date().isoformat()

    # Reset attempts counter on a new day. The should-run check has
    # already gated us, but writing the started marker here is the
    # authoritative "we attempted" record.
    attempts_today = int(state.get("attempts_today", 0) or 0)
    if state.get("last_run_date_eastern") != today_str:
        attempts_today = 0
    attempts_today += 1

    _save_hot_deal_state(state_path, {
        "last_run_date_eastern": today_str,
        "last_run_status": "started",
        "last_run_at": now_eastern.isoformat(),
        "attempts_today": attempts_today,
        "last_failure_reason": state.get("last_failure_reason") if attempts_today > 1 else None,
    })

    logger.info(
        "Hot Deal pipeline: starting attempt %s/%s for %s",
        attempts_today, settings.hot_deal_max_daily_attempts, today_str,
    )

    try:
        db_conn = init_db(settings.hot_deal_db_path)
        try:
            runner = HotDealPipelineRunner(
                settings=settings, browser=browser, db_conn=db_conn,
                log=logger, notifier=notifier,
                api_client=api_client,
            )
            result = runner.run_once()
        finally:
            db_conn.close()
    except BrowserSessionError:
        # Re-raise to the main loop's auth handler. Leave state as
        # "started" — the stale-start detection will reclassify it as
        # retryable once the runtime rebuilds and we're back in the loop.
        logger.warning("Hot Deal pipeline hit BrowserSessionError; deferring to main-loop auth recovery")
        raise
    except Exception as exc:
        finish = datetime.now(EASTERN_TZ)
        reason = f"{type(exc).__name__}: {exc}"
        _save_hot_deal_state(state_path, {
            "last_run_date_eastern": today_str,
            "last_run_status": "failed",
            "last_run_at": finish.isoformat(),
            "attempts_today": attempts_today,
            "last_failure_reason": reason,
        })
        logger.error("Hot Deal pipeline crashed on attempt %s: %s", attempts_today, exc, exc_info=True)
        if attempts_today >= settings.hot_deal_max_daily_attempts and notifier is not None:
            try:
                notifier.notify_hot_deal_pipeline_failed(
                    attempts=attempts_today,
                    last_error=reason,
                    logger=logger,
                )
            except Exception as notify_exc:
                logger.warning("Failed to send Hot Deal failure notification: %s", notify_exc)
        return

    finish = datetime.now(EASTERN_TZ)
    status = (result or {}).get("status") if isinstance(result, dict) else None
    if status == "completed":
        _save_hot_deal_state(state_path, {
            "last_run_date_eastern": today_str,
            "last_run_status": "completed",
            "last_run_at": finish.isoformat(),
            "attempts_today": attempts_today,
            "last_failure_reason": None,
        })
        logger.info(
            "Hot Deal pipeline: completed for %s (attempt %s, total_vins=%s, hot_deals=%s)",
            today_str, attempts_today,
            (result or {}).get("total_vins", 0),
            (result or {}).get("hot_deals", 0),
        )
    else:
        # Pipeline reached finish_run() but status != "completed" (e.g.
        # "failed" — export failure inside run_once caught and logged).
        reason = f"pipeline returned status={status!r}"
        _save_hot_deal_state(state_path, {
            "last_run_date_eastern": today_str,
            "last_run_status": "failed",
            "last_run_at": finish.isoformat(),
            "attempts_today": attempts_today,
            "last_failure_reason": reason,
        })
        logger.error(
            "Hot Deal pipeline did not complete cleanly on attempt %s: %s",
            attempts_today, reason,
        )
        if attempts_today >= settings.hot_deal_max_daily_attempts and notifier is not None:
            try:
                notifier.notify_hot_deal_pipeline_failed(
                    attempts=attempts_today,
                    last_error=reason,
                    logger=logger,
                )
            except Exception as notify_exc:
                logger.warning("Failed to send Hot Deal failure notification: %s", notify_exc)


if __name__ == "__main__":
    main()

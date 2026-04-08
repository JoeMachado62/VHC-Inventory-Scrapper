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

from ove_scraper.automation_lock import AutomationLockBusyError, OveAutomationLock
from ove_scraper.browser import BrowserSessionError
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
    return parser


def main() -> None:
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
                args=(api_client, settings, logger),
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
                            if sync_result is not None and getattr(sync_result, "execution_status", None) == "Success":
                                _HEARTBEAT_STATE["last_sync_at"] = _utc_now_iso()
                                _HEARTBEAT_STATE["status_note"] = "snapshot_ok"
                            next_sync_at = now + settings.sync_interval_seconds
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

                    # Wait on the shutdown event instead of a plain sleep so
                    # SIGINT/SIGTERM/SIGBREAK wake the loop within 1s.
                    SHUTDOWN_EVENT.wait(timeout=1.0)
                except Exception as exc:
                    logger.exception("OVE main loop crashed; rebuilding runtime: %s", exc)
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
    browser = PlaywrightCdpBrowserSession(settings)
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
    sync_runner = HourlySyncRunner(settings, browser, api_client, logger, notifier=notifier)
    deep_scrape_worker = DeepScrapeWorker(api_client, browser, logger, settings)
    logger.info("Configured deep-scrape worker pool size: %s", settings.deep_scrape_max_workers)
    return browser, api_client, sync_runner, deep_scrape_worker, notifier


def run_sync_once_with_recovery(
    settings: Settings,
    browser: PlaywrightCdpBrowserSession,
    api_client: VCHApiClient,
    logger,
    sync_runner: HourlySyncRunner | None = None,
    notifier: AdminNotifier | None = None,
):
    runner = sync_runner or HourlySyncRunner(settings, browser, api_client, logger, notifier=notifier)
    return run_browser_operation(settings, browser, logger, runner.run_once, "hourly sync", notifier=notifier)


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


def _heartbeat_ticker(api_client: VCHApiClient, settings: Settings, logger) -> None:
    """Background daemon thread that POSTs a heartbeat every
    _HEARTBEAT_INTERVAL_SECONDS based on the latest _HEARTBEAT_STATE.

    This is decoupled from the main loop's poll/sync ticks because the
    hourly sync grabs the OveAutomationLock for many minutes, and the
    polling tick is gated through the same lock — so a heartbeat coupled
    to polling would not fire during a long sync run, tripping the VPS
    health endpoint to warning (5min) then critical (15min) while real
    work is in progress.

    Heartbeats are pure HTTP, never touch the browser, never need the
    lock. The thread is a daemon so it dies automatically with the
    process; the SHUTDOWN_EVENT wake unblocks it for clean exit.
    """
    logger.info("Heartbeat ticker started (interval=%ss)", _HEARTBEAT_INTERVAL_SECONDS)
    # Send one heartbeat immediately so the VPS sees the new boot quickly
    # instead of waiting for the first interval to expire.
    send_heartbeat(
        api_client,
        settings,
        logger,
        last_sync_at=_HEARTBEAT_STATE.get("last_sync_at"),
        last_poll_at=_HEARTBEAT_STATE.get("last_poll_at"),
        last_claim_at=_HEARTBEAT_STATE.get("last_claim_at"),
        pending_claims=_HEARTBEAT_STATE.get("pending_claims"),
        status_note=_HEARTBEAT_STATE.get("status_note") or "ticker_boot",
    )
    while not SHUTDOWN_EVENT.wait(timeout=_HEARTBEAT_INTERVAL_SECONDS):
        send_heartbeat(
            api_client,
            settings,
            logger,
            last_sync_at=_HEARTBEAT_STATE.get("last_sync_at"),
            last_poll_at=_HEARTBEAT_STATE.get("last_poll_at"),
            last_claim_at=_HEARTBEAT_STATE.get("last_claim_at"),
            pending_claims=_HEARTBEAT_STATE.get("pending_claims"),
            status_note=_HEARTBEAT_STATE.get("status_note"),
        )
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


def run_browser_operation(settings: Settings, browser, logger, operation, operation_name: str, notifier: AdminNotifier | None = None):
    try:
        with OveAutomationLock(timeout_seconds=_browser_operation_lock_timeout_seconds(operation_name)):
            ensure_browser_session(settings, browser, logger, notifier=notifier)
            try:
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
    launch_browser_script(logger)
    wait_for_cdp(settings, logger)
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


if __name__ == "__main__":
    main()

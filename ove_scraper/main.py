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

from dataclasses import dataclass
from ove_scraper import auth_lockout
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

# Exit code emitted when the scraper is starting up while a disk-backed
# auth lockout is in effect. Distinct from 0 (clean shutdown) and 1
# (crash) so the launcher PowerShell script can detect it and refuse
# to restart Python until the lockout clears. This is the linchpin of
# the cross-process lockout: without it, the launcher's `while ($true)`
# loop would respawn Python and burn another login click attempt.
EXIT_CODE_AUTH_LOCKOUT_ACTIVE = 99


# ---------------------------------------------------------------------------
# Chrome instance descriptors — Path 2 / two-Chrome architecture (2026-04-28)
# ---------------------------------------------------------------------------
#
# Each Chrome instance the scraper drives has THREE coupled identifiers:
#   - the CDP debug port (9222 primary, 9223 secondary sync)
#   - the user-data-dir on disk (different profile state per instance)
#   - the launcher PS1 script that spawns it with the right flags
#
# Before the two-Chrome architecture there was implicitly only one Chrome,
# so all three identifiers were hardcoded into the recovery code paths
# (_kill_stale_chrome, launch_browser_script, _clear_chrome_cookies).
#
# When the sync workflow moved to a second Chrome (Login B on port 9223
# with the C:\Users\joema\AppData\Local\ove_sync profile), the recovery
# code kept killing/relaunching the PRIMARY Chrome on every sync failure
# — leaving Login B's broken session untouched while repeatedly thrashing
# Login A's session for an issue that had nothing to do with it. The
# observable symptom: Login A's Chrome window count grew during a Login
# B auth-failure cycle, while Login B stayed stuck.
#
# ChromeInstance bundles the three identifiers so every recovery callsite
# can route to the correct Chrome by Settings.chrome_debug_port.

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@dataclass(frozen=True, slots=True)
class ChromeInstance:
    port: int
    profile_path: Path
    launcher_script: Path  # absolute path to the .ps1 launcher


PRIMARY_CHROME = ChromeInstance(
    port=9222,
    profile_path=Path("C:/chrome-cdp-profile"),
    launcher_script=_SCRIPTS_DIR / "start_ove_browser.ps1",
)

SYNC_CHROME = ChromeInstance(
    port=9223,
    profile_path=Path("C:/Users/joema/AppData/Local/ove_sync"),
    launcher_script=_SCRIPTS_DIR / "start_ove_browser_sync.ps1",
)


def chrome_for_port(port: int) -> ChromeInstance:
    """Return the ChromeInstance descriptor for a given debug port.

    Defaults to PRIMARY_CHROME for unknown ports — including the legacy
    single-Chrome case where chrome_debug_port_sync is 0/unset and all
    code paths see the primary's port.
    """
    if port == SYNC_CHROME.port:
        return SYNC_CHROME
    return PRIMARY_CHROME

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

# Transient per-operation error counter (Fix 3, 2026-04-30). The main
# loop's `except Exception` handler used to rebuild the entire runtime
# on ANY non-BrowserSessionError exception. That was too broad — a
# transient Page.goto error from one VIN's deep-scrape (e.g.
# "Target page, context or browser has been closed") cascaded to a
# full runtime rebuild → re-attach to OVE → got auth-redirected → the
# auth-recovery loop tore through Chrome's session state and Manheim
# SMS-challenged the account (the 2026-04-30 22:12 incident).
#
# The new behavior: classify the exception with
# _is_transient_per_operation_error, and on transient ones just log
# and continue without rebuilding. The counter below is the safety
# escape hatch — if transient errors keep firing in a row, eventually
# escalate to a real rebuild.
_TRANSIENT_ERROR_COUNT = 0
_TRANSIENT_ERROR_REBUILD_THRESHOLD = 10   # rebuild only after this many in a row

# Per-port consecutive saved-search-timeout streak (Fix, 2026-05-01).
# The transient classifier matches "Page.goto: Timeout" — which was the
# right call for one-off OVE slowness, but it caused a real production
# bug on Login B (2026-05-01): the SYNC kept hitting goto-timeout on
# https://www.ove.com/saved_searches#/, the classifier said "transient,
# retry", 10 retries triggered a runtime rebuild, the rebuild left
# Chrome's dead session intact so the next attempt produced the same
# timeout, and the system entered an infinite loop with no real recovery.
#
# A goto that times out N times in a row at the saved-search URL is a
# session-dead signal, not transient. We track the streak per port (so
# A's health doesn't influence B's escalation decision) and at the
# threshold we route the failure into the real recovery path (Chrome
# kill+relaunch, lockout-gated) instead of the transient retry loop.
#
# Threshold of 3 is deliberate: 1 = single OVE hiccup, 2 = could be
# coincidence, 3 = session dead. Total wait before escalation: ~3min
# (3 × 60s timeout + retry gaps).
_SAVED_SEARCH_TIMEOUT_STREAK: dict[int, int] = {}
_SAVED_SEARCH_TIMEOUT_ESCALATE_AT = 3
_LUX_PREFLIGHT_LAST_AT: dict[int, float] = {}

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
    subparsers.add_parser(
        "unlock",
        help=(
            "Clear the disk-backed auth lockout state in "
            "artifacts/_state/ (per-port files + global manual-unlock "
            "flag). Use after Manheim has lifted an account lock and "
            "you have manually re-authed Chrome. Without this, every "
            "Python process refuses auth attempts."
        ),
    )
    subparsers.add_parser(
        "lockout-status",
        help="Print the current auth-lockout state and exit.",
    )
    scrape_vin = subparsers.add_parser("scrape-vin")
    scrape_vin.add_argument("vin")
    scrape_vin.add_argument("--output", default="")
    subparsers.add_parser("hot-deal", help="Run Hot Deal vehicle screening pipeline")
    reprocess = subparsers.add_parser(
        "hot-deal-reprocess",
        help=(
            "One-shot recovery: reclassify prior step1_fail rows with scraper-error "
            "reasons back to 'scrape_failed' so the next Hot Deal run re-screens them. "
            "With --rescreen, also queues current Hot Deal rows for a full fresh "
            "screen before the next VPS push."
        ),
    )
    reprocess.add_argument(
        "--rescreen",
        action="store_true",
        help=(
            "Queue current hot_deal rows for full fresh screening and re-run the "
            "screener against every current step1_fail VIN's stored cr_data; promote "
            "any fixed false positives back to pending."
        ),
    )
    return parser


def main() -> None:
    global _AUTH_FAIL_COUNT, _TRANSIENT_ERROR_COUNT  # must appear before any assignment in this scope
    # Suppress Node.js deprecation warnings from Playwright
    os.environ["NODE_OPTIONS"] = "--no-deprecation"

    load_env_file()
    args = build_parser().parse_args()
    settings = Settings.from_env()
    logger = configure_logging(settings.log_level, settings.log_file_path)
    _install_signal_handlers(logger)

    # Admin commands that operate on the lockout state directly. These
    # must run BEFORE build_runtime so they're usable even when the
    # browser session can't be constructed (e.g. Chrome isn't running).
    if args.command == "unlock":
        auth_lockout.unlock(settings.artifact_dir)
        print(auth_lockout.describe_state(settings.artifact_dir, port=None))
        for _port in (PRIMARY_CHROME.port, SYNC_CHROME.port):
            print(f"PORT {_port}: {auth_lockout.describe_state(settings.artifact_dir, port=_port)}")
        print(
            "REMINDER: confirm Manheim has lifted any account lock on their side "
            "before restarting the scraper, or you'll trigger another lockout."
        )
        return

    if args.command == "lockout-status":
        # Three lines: global flag, then each known port (Login A, Login B).
        # Per-port split landed 2026-05-01 — pre-split, lockout was a single
        # shared file; the global line preserves the operator-override flag,
        # and the per-port lines surface where the actual ledger / cooldown
        # lives now.
        print(auth_lockout.describe_state(settings.artifact_dir, port=None))
        print(f"PORT {PRIMARY_CHROME.port} (Login A): "
              f"{auth_lockout.describe_state(settings.artifact_dir, port=PRIMARY_CHROME.port)}")
        print(f"PORT {SYNC_CHROME.port} (Login B):  "
              f"{auth_lockout.describe_state(settings.artifact_dir, port=SYNC_CHROME.port)}")
        return

    # Cross-process auth lockout gate (2026-04-28 hardening). Every
    # operational command (run, sync-once, poll-once, scrape-vin,
    # hot-deal*) must check the disk-backed lockout state at startup
    # and refuse to run if blocked. Exiting with EXIT_CODE_AUTH_LOCKOUT_ACTIVE
    # signals the launcher PowerShell to NOT respawn Python — which
    # is the only way to break the launcher's `while ($true)` loop
    # from inside Python without killing the whole supervision tree.
    #
    # We do this BEFORE build_runtime() so we don't waste time
    # connecting to Chrome only to refuse work.
    operational_commands = {"run", "sync-once", "poll-once", "scrape-vin", "hot-deal", "hot-deal-reprocess"}
    if args.command in operational_commands:
        # Per-port lockout split (2026-05-01): the launcher manages both
        # Chrome instances, so refuse to start if EITHER port is blocked.
        # Recovery code paths inside the loop check their own port
        # independently, but at startup we have no way to know which
        # instance the operator just touched, so the safe move is to
        # park the whole process until the operator clears the relevant
        # lockout. The global manual-unlock flag is folded into every
        # per-port get_state, so checking each port covers the global
        # case too.
        for _port in (PRIMARY_CHROME.port, SYNC_CHROME.port):
            lockout_state = auth_lockout.get_state(settings.artifact_dir, port=_port)
            if not lockout_state.blocked:
                continue
            logger.error(
                "STARTUP BLOCKED by auth lockout (port %d): %s",
                _port, lockout_state.reason,
            )
            logger.error(
                "Lockout state directory: %s",
                settings.artifact_dir / "_state",
            )
            logger.error(
                "To clear manually after Manheim has lifted any account lock: "
                "python -m ove_scraper.main unlock"
            )
            print(f"BLOCKED (port {_port}): {lockout_state.reason}")
            raise SystemExit(EXIT_CODE_AUTH_LOCKOUT_ACTIVE)

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
                            _TRANSIENT_ERROR_COUNT = 0  # reset transient counter on success
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
                        _TRANSIENT_ERROR_COUNT = 0  # reset transient counter on success
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
                        # Login A keepalive (port 9222 in two-Chrome mode,
                        # the only Chrome in single-Chrome mode).
                        run_browser_operation(
                            settings,
                            browser,
                            logger,
                            browser.touch_session,
                            "browser keepalive",
                            notifier=notifier,
                        )
                        # Fix C (2026-04-28): symmetric keepalive for
                        # Login B. Pre-fix, Login B's session was only
                        # touched during the hourly sync, so an hour of
                        # idle time was enough for OVE to expire it —
                        # which guaranteed the next sync hit a stale
                        # session and triggered an auth-failure cascade.
                        # Now Login B gets the same 5-minute touch as
                        # Login A. Guard against the single-Chrome case
                        # (sync_runner.browser IS browser) so we don't
                        # double-touch.
                        if (
                            sync_runner is not None
                            and sync_runner.browser is not browser
                        ):
                            run_browser_operation(
                                sync_runner.settings,
                                sync_runner.browser,
                                logger,
                                sync_runner.browser.touch_session,
                                "browser keepalive (sync)",
                                notifier=notifier,
                                lock_name=lock_name_for_port(
                                    sync_runner.settings.chrome_debug_port
                                ),
                            )
                        # Fix B (2026-04-28): orphan-tab sweeper. Defense
                        # in depth against any leak path we haven't
                        # plugged: enumerate every page on each Chrome
                        # and close the ones whose URL matches the auth
                        # / login / signin patterns — i.e. tabs the
                        # scraper spawned during a CR click but failed
                        # to close. Runs after the touch_session calls
                        # so any legitimate auth-redirect tabs that
                        # touch_session itself opened (single-shot
                        # sign-in click flow) have been resolved.
                        sweep_targets = [browser]
                        if (
                            sync_runner is not None
                            and sync_runner.browser is not browser
                        ):
                            sweep_targets.append(sync_runner.browser)
                        for _b in sweep_targets:
                            try:
                                if hasattr(_b, "sweep_orphan_tabs"):
                                    _b.sweep_orphan_tabs()
                            except Exception as sweep_exc:
                                logger.warning(
                                    "Orphan-tab sweep raised (non-fatal): %s",
                                    sweep_exc,
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
                    # Fix 3 (2026-04-30): the prior implementation
                    # rebuilt the entire runtime on ANY non-
                    # BrowserSessionError exception. That was too
                    # broad: a single transient Page.goto error from
                    # one VIN's deep-scrape (e.g. "Target page,
                    # context or browser has been closed") cascaded
                    # to a runtime rebuild → re-attach to OVE → got
                    # auth-redirected → started the recovery loop
                    # that ended with Manheim SMS-challenging the
                    # account (the 22:12 incident).
                    #
                    # Now we classify the exception. Transient
                    # per-operation errors get logged + continue
                    # without rebuilding. The escape hatch is the
                    # _TRANSIENT_ERROR_REBUILD_THRESHOLD counter:
                    # if these keep firing in a row, eventually we
                    # DO rebuild (something might genuinely be
                    # broken).
                    if _is_transient_per_operation_error(exc):
                        _TRANSIENT_ERROR_COUNT += 1
                        if _TRANSIENT_ERROR_COUNT < _TRANSIENT_ERROR_REBUILD_THRESHOLD:
                            logger.warning(
                                "Transient per-operation error #%d/%d (continuing without rebuild): %s",
                                _TRANSIENT_ERROR_COUNT,
                                _TRANSIENT_ERROR_REBUILD_THRESHOLD,
                                exc,
                            )
                            # Brief pause so we don't pin CPU if the
                            # error repeats instantly. SHUTDOWN_EVENT
                            # check lets Ctrl+C/SIGTERM still wake us.
                            if SHUTDOWN_EVENT.wait(timeout=5.0):
                                break
                            continue
                        # Threshold tripped — fall through to
                        # rebuild. Reset the counter so we don't
                        # rebuild on every iteration after this.
                        logger.error(
                            "Transient errors hit %d in a row; escalating to runtime rebuild",
                            _TRANSIENT_ERROR_COUNT,
                        )
                        _TRANSIENT_ERROR_COUNT = 0

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
    # Wire the notifier into BOTH browser sessions so the recovery path
    # can fire alerts about states only it can observe — specifically
    # the credentials-not-saved state detected by the single-shot login
    # click (Fix D, 2026-04-28). Without this the alert can only fire
    # if the caller explicitly threads a notifier through every recovery
    # call site, which the prior code did not do consistently.
    for _b in (primary_browser, sync_browser):
        if _b is not None and hasattr(_b, "set_notifier"):
            _b.set_notifier(notifier)
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
    # Long-lived httpx.Client instead of module-level httpx.post() per call.
    # Module-level httpx.post() creates a fresh Client per invocation, and
    # httpx >= 0.28 on Python 3.12 Windows has a known interaction with
    # anyio's backend detection: each fresh Client briefly probes for an
    # async backend, which in some configurations installs transient
    # asyncio policy state that bleeds across threads. Over time, this
    # poisons the main thread's asyncio detection so Playwright's sync
    # API refuses to run with "Sync API inside the asyncio loop". A
    # single reused Client (this thread's only client) avoids the per-
    # tick probe entirely. See 2026-04-28 incident report.
    import httpx as _httpx

    logger.info("Heartbeat ticker started (interval=%ss)", _HEARTBEAT_INTERVAL_SECONDS)
    base_url = settings.vch_api_base_url.rstrip("/")
    headers = {"X-Service-Token": settings.vch_service_token, "Content-Type": "application/json"}
    endpoint = f"{base_url}/inventory/ove/scraper-heartbeat"

    # Single Client for the lifetime of the ticker thread. Closed in the
    # finally so a clean shutdown drops the connection pool gracefully.
    client = _httpx.Client(timeout=10.0)

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
            resp = client.post(endpoint, json=body, headers=headers)
            if resp.status_code >= 400:
                logger.debug("Heartbeat tick returned %s", resp.status_code)
        except Exception as exc:
            logger.debug("Heartbeat tick failed: %s", exc)

    try:
        _tick(note_override="ticker_boot")
        while not SHUTDOWN_EVENT.wait(timeout=_HEARTBEAT_INTERVAL_SECONDS):
            _tick()
    finally:
        try:
            client.close()
        except Exception:
            pass
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


def _is_saved_search_session_probe(operation_name: str) -> bool:
    """True for operations that probe the saved-search page to validate
    OVE session liveness — the goto target where Login B's dead-session
    timeouts surface. Matches both the hourly sync and the keepalive."""
    name = operation_name.lower()
    return "sync" in name or "saved-search" in name or "keepalive" in name


def _is_saved_search_goto_timeout(exc: BaseException) -> bool:
    """True if `exc` is the specific failure mode that the per-port
    streak counter is designed to break out of: a Playwright goto
    timeout against the OVE saved-searches URL."""
    msg = str(exc)
    if "Page.goto: Timeout" not in msg:
        return False
    return "saved_searches" in msg or "saved-searches" in msg


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
    port = settings.chrome_debug_port
    is_session_probe = _is_saved_search_session_probe(operation_name)
    try:
        run_lux_auth_preflight(settings, operation_name, logger)
        with OveAutomationLock(name=lock_name, timeout_seconds=_browser_operation_lock_timeout_seconds(operation_name)):
            ensure_browser_session(settings, browser, logger, notifier=notifier)
            try:
                result = operation()
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
                _SAVED_SEARCH_TIMEOUT_STREAK[port] = 0
                return operation()
            except BrowserSessionError as exc:
                logger.warning("%s lost browser session: %s", operation_name, exc)
                recover_browser_session(settings, browser, logger, notifier=notifier)
                _SAVED_SEARCH_TIMEOUT_STREAK[port] = 0
                return operation()
            except Exception as exc:
                # Per-port streak escalation (2026-05-01). The transient
                # classifier in the main loop will treat this as a
                # transient retry, but if we've already seen the same
                # failure mode multiple times in a row on this port we
                # need to break out of the retry loop and run real
                # recovery (Chrome kill+relaunch, lockout-gated).
                if is_session_probe and _is_saved_search_goto_timeout(exc):
                    streak = _SAVED_SEARCH_TIMEOUT_STREAK.get(port, 0) + 1
                    _SAVED_SEARCH_TIMEOUT_STREAK[port] = streak
                    if streak >= _SAVED_SEARCH_TIMEOUT_ESCALATE_AT:
                        logger.error(
                            "SAVED_SEARCH_TIMEOUT_STREAK port=%d streak=%d "
                            "→ escalating to recover_browser_session",
                            port, streak,
                        )
                        _SAVED_SEARCH_TIMEOUT_STREAK[port] = 0
                        try:
                            recover_browser_session(settings, browser, logger, notifier=notifier)
                        except BrowserSessionError as recover_exc:
                            # Recovery itself failed (e.g. lockout active).
                            # Re-raise so the main loop's circuit breaker
                            # gets its turn — this is the right outcome:
                            # we tried to recover, we couldn't, escalate.
                            raise
                        # Recovery succeeded — retry the original op once.
                        return operation()
                    logger.warning(
                        "Saved-search goto timeout (port=%d streak=%d/%d); "
                        "letting transient handler retry",
                        port, streak, _SAVED_SEARCH_TIMEOUT_ESCALATE_AT,
                    )
                raise
            else:
                # Successful operation resets the streak so a future
                # blip starts counting from zero.
                if is_session_probe:
                    _SAVED_SEARCH_TIMEOUT_STREAK[port] = 0
                return result
            finally:
                try:
                    if hasattr(browser, "compact_tabs"):
                        browser.compact_tabs()
                    elif hasattr(browser, "sweep_orphan_tabs"):
                        browser.sweep_orphan_tabs()
                except Exception as sweep_exc:
                    logger.warning(
                        "Post-operation tab cleanup raised during %s (non-fatal): %s",
                        operation_name,
                        sweep_exc,
                    )
    except AutomationLockBusyError as exc:
        logger.warning("%s skipped because another OVE automation task is active: %s", operation_name, exc)
        return None


def run_lux_auth_preflight(settings: Settings, operation_name: str, logger) -> None:
    """Best-effort Lux auth/tab-hygiene preflight before OVE browser work.

    The deterministic Playwright code remains the owner of scraper work. Lux is
    used only for the browser-auth handoff and tab cleanup problem space. This
    function is intentionally best-effort: a Lux outage must not prevent the
    existing Playwright recovery path from getting its turn.
    """
    if os.getenv("OVE_LUX_AUTH_PREFLIGHT", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    interval_seconds = _get_lux_preflight_interval_seconds(operation_name)
    now = time.monotonic()
    last = _LUX_PREFLIGHT_LAST_AT.get(settings.chrome_debug_port)
    if last is not None and now - last < interval_seconds:
        return
    script = _SCRIPTS_DIR / "lux_auth_handoff.py"
    if not script.exists():
        logger.debug("Lux auth preflight skipped; script missing at %s", script)
        return
    track = "sync" if settings.chrome_debug_port == SYNC_CHROME.port else "hot-deal"
    timeout_seconds = _get_lux_preflight_timeout_seconds(operation_name)
    command = [
        "python",
        str(script),
        "--track",
        track,
        "--port",
        str(settings.chrome_debug_port),
        "--artifact-dir",
        str(settings.artifact_dir),
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)
    try:
        logger.info(
            "Lux auth preflight starting for %s on port %d (track=%s, timeout=%ss)",
            operation_name,
            settings.chrome_debug_port,
            track,
            timeout_seconds,
        )
        result = subprocess.run(
            command,
            cwd=str(Path(__file__).resolve().parent.parent),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        _LUX_PREFLIGHT_LAST_AT[settings.chrome_debug_port] = time.monotonic()
    except subprocess.TimeoutExpired as exc:
        logger.warning(
            "Lux auth preflight timed out for %s on port %d after %ss; continuing with Playwright path. stdout=%s stderr=%s",
            operation_name,
            settings.chrome_debug_port,
            timeout_seconds,
            (exc.stdout or "")[-1000:],
            (exc.stderr or "")[-1000:],
        )
        return
    except Exception as exc:
        logger.warning("Lux auth preflight failed to start for %s: %s", operation_name, exc)
        return
    if result.returncode == 0:
        logger.info(
            "Lux auth preflight OK for %s on port %d. stdout_tail=%s",
            operation_name,
            settings.chrome_debug_port,
            (result.stdout or "")[-1000:],
        )
        return
    logger.warning(
        "Lux auth preflight returned %s for %s on port %d; continuing with Playwright path. stdout_tail=%s stderr_tail=%s",
        result.returncode,
        operation_name,
        settings.chrome_debug_port,
        (result.stdout or "")[-1000:],
        (result.stderr or "")[-1000:],
    )


def _get_lux_preflight_timeout_seconds(operation_name: str) -> int:
    raw = os.getenv("OVE_LUX_AUTH_PREFLIGHT_TIMEOUT_SECONDS")
    if raw:
        try:
            return max(30, int(raw))
        except ValueError:
            pass
    name = operation_name.lower()
    if "hot" in name:
        return 300
    if "sync" in name:
        return 300
    return 180


def _get_lux_preflight_interval_seconds(operation_name: str) -> int:
    raw = os.getenv("OVE_LUX_AUTH_PREFLIGHT_INTERVAL_SECONDS")
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            pass
    name = operation_name.lower()
    if "hot" in name:
        return 0
    if "sync" in name:
        return 300
    return 600


def _is_process_state_error(exc: BaseException) -> bool:
    """Return True if `exc` describes a Python-process-state issue rather
    than a real OVE browser problem.

    The big example (2026-04-28): Playwright's sync API raises
    "It looks like you are using Playwright Sync API inside the asyncio
    loop" once SOMETHING in the process has started an asyncio event
    loop (typically a background thread spawned by an HTTP client
    library after the hot-deal pipeline runs). The error keeps firing
    on every subsequent Playwright sync call because the loop persists
    in the process; killing/relaunching Chrome can't fix it. Treating
    this as "auth lost" produced the false-positive email storm and
    the visible Chrome-tab accumulation from the recovery cycle.

    Second observed variant (2026-04-29 post-power-outage): the same
    underlying asyncio-state pollution can surface as a different
    Playwright error message — "got Future <Future pending> attached
    to a different loop" — when a coroutine future is awaited on a
    loop that no longer matches the running thread's recorded loop.
    Same root cause, same correct response: let the launcher restart
    Python with clean asyncio state. Adding the "attached to a
    different loop" string here ensures these errors take the same
    safe-exit path instead of being misclassified as auth failure
    (the misclassification on 2026-04-29 contributed to the Login B
    Chrome relaunch storm that preceded the Manheim account lockout).

    For all matched errors, the right move is to let the launcher
    restart the Python process — NOT to thrash Chrome.
    """
    msg = str(exc)
    return (
        "Playwright Sync API inside the asyncio loop" in msg
        or "Sync API inside the asyncio loop" in msg
        or "attached to a different loop" in msg
    )


def _is_transient_per_operation_error(exc: BaseException) -> bool:
    """Return True if `exc` describes a recoverable per-operation error
    that should NOT trigger a full runtime rebuild.

    Background (Fix 3, 2026-04-30): a transient Page.goto error from
    one deep-scrape attempt at 22:12 caused the main loop's broad
    `except Exception` clause to rebuild the entire runtime. The
    rebuild re-attached to OVE, got auth-redirected, and started a
    Chrome kill+relaunch loop that ended with Manheim SMS-challenging
    the account. The triggering exception was just one transient
    Playwright error — handling it locally would have avoided the
    cascade entirely.

    The Playwright errors below are common Chrome/network blips. They
    don't indicate Chrome itself is broken or auth is lost; they
    mean one operation needs to fail and be retried via the standard
    /fail flow. The right response is log + continue, NOT a runtime
    rebuild.

    Process-state errors (asyncio loop pollution) are deliberately
    NOT covered here — those are still classified by
    _is_process_state_error and routed to a clean Python restart.
    """
    msg = str(exc)
    # Common Chrome/Playwright transient signals.
    transient_markers = (
        "Target page, context or browser has been closed",
        "Target page, context or browser",
        "Frame was detached",
        "Navigation interrupted",
        "Page.goto: Timeout",
        "net::ERR_",
        "Protocol error",
        "Browser has been closed",
        "Connection closed while reading",
        "WebSocket is not open",
    )
    return any(marker in msg for marker in transient_markers)


def ensure_browser_session(settings: Settings, browser, logger, notifier: AdminNotifier | None = None) -> None:
    try:
        browser.ensure_session()
    except BrowserSessionError as exc:
        logger.warning("OVE browser session unavailable: %s", exc)
        if _is_process_state_error(exc):
            # Process is contaminated — Chrome is fine, Python is not.
            # Re-raise so the main loop's auth-failure circuit breaker
            # ticks down and the launcher restarts Python with a fresh
            # asyncio state. Do NOT touch Chrome.
            logger.error(
                "Process-state failure (asyncio/Playwright conflict); not "
                "touching Chrome — exiting so launcher can restart Python."
            )
            SHUTDOWN_EVENT.set()
            raise
        recover_browser_session(settings, browser, logger, notifier=notifier)


def recover_browser_session(settings: Settings, browser, logger, notifier: AdminNotifier | None = None) -> None:
    """Recover the SPECIFIC Chrome session this `settings` describes.

    Routes by `settings.chrome_debug_port` so a sync-side recovery
    (Login B on 9223) doesn't kill/relaunch the primary Chrome (Login A
    on 9222) it has nothing to do with — the bug fixed 2026-04-28.

    Skips the Chrome kill/relaunch dance when the underlying error is a
    Python-process-state issue (e.g. polluted asyncio loop). In that
    case Chrome is healthy and recovery would just thrash tabs.
    """
    # Auth-lockout gate (2026-04-28 hardening): if the disk-backed
    # lockout is active, refuse to relaunch Chrome. Relaunching opens
    # a fresh Chrome window pointed at OVE; if Chrome's password
    # manager auto-fills+submits, that's another login attempt against
    # an already-locked account, which extends the lockout. Raising
    # BrowserSessionError sends control back to the main loop which
    # backs off and eventually exits via the auth-fail park threshold
    # — clean, no Chrome thrashing.
    # Per-port lockout: a B-side rate-limit no longer blocks A's recovery
    # (and vice versa). The global manual-unlock flag is still observed
    # because get_state folds it into every per-port read.
    lockout_state = auth_lockout.get_state(
        settings.artifact_dir, port=settings.chrome_debug_port,
    )
    if lockout_state.blocked:
        logger.error(
            "recover_browser_session REFUSED by auth lockout (port %d): %s",
            settings.chrome_debug_port, lockout_state.reason,
        )
        raise BrowserSessionError(
            f"Browser recovery skipped — auth lockout active: {lockout_state.reason}"
        )

    # Per-port relaunch rate gate (2026-05-04 fix). The lockout gate
    # above only kicks in AFTER an account-lock event has been recorded.
    # The 2026-05-04 incident showed that 6 Chrome relaunches on port
    # 9223 in 9 minutes (~90s spacing) triggered Manheim's anti-abuse
    # heuristic and locked the account BEFORE any single failure was
    # severe enough to record an account-lock event. The streak counter
    # in run_browser_operation resets to 0 after each relaunch, so a
    # single ~10-min bad window can fire 3+ relaunches before the first
    # 30-min lockout cooldown ever starts. This gate enforces a 5-min
    # floor between successive relaunches per port, regardless of what
    # else is happening, so a panic-relaunch loop becomes structurally
    # impossible.
    is_rate_limited, last_age_s = auth_lockout.is_relaunch_rate_limited(
        settings.artifact_dir, port=settings.chrome_debug_port,
    )
    if is_rate_limited:
        logger.error(
            "RELAUNCH_RATE_LIMITED port=%d last_relaunch_age_s=%d (5-min floor)",
            settings.chrome_debug_port, last_age_s,
        )
        raise BrowserSessionError(
            f"Browser recovery skipped — Chrome on port "
            f"{settings.chrome_debug_port} was relaunched {last_age_s}s ago; "
            "5-min floor in effect to prevent Manheim anti-abuse trigger."
        )

    # Same guard at the recover entry — _clear_chrome_cookies path
    # leads here too and we don't want to kill/relaunch on a
    # process-state error from there either.
    chrome = chrome_for_port(settings.chrome_debug_port)
    browser.close()
    _kill_stale_chrome(chrome, logger)
    launch_browser_script(chrome, logger)
    # Record the relaunch BEFORE waiting for CDP so the rate gate
    # protects subsequent attempts even if wait_for_cdp times out.
    try:
        auth_lockout.record_chrome_relaunch(
            settings.artifact_dir, port=settings.chrome_debug_port,
        )
    except Exception as exc:
        # Ledger write failure is non-fatal — the rate gate just won't
        # be effective for this relaunch. Loud log so operators notice
        # if disk is full / permissions broken.
        logger.error(
            "Failed to record Chrome relaunch in ledger (rate gate degraded "
            "for port %d): %s",
            settings.chrome_debug_port, exc,
        )
    wait_for_cdp(settings, logger, timeout_seconds=45)
    try:
        browser.ensure_session()
    except BrowserSessionError as exc:
        if _is_process_state_error(exc):
            # Don't fire the auth-lost alert for this — Chrome is fine,
            # Python is the problem. Set shutdown so launcher restarts.
            logger.error(
                "Recovery hit process-state failure (asyncio/Playwright "
                "conflict); not alerting on auth and exiting for clean "
                "Python restart."
            )
            SHUTDOWN_EVENT.set()
            raise
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


def _kill_stale_chrome(chrome: ChromeInstance, logger) -> None:
    """Kill any Chrome process using THIS instance's debug port + profile.

    Belt-and-suspenders with the TCP check in the launcher PS1 — this
    Python-side kill runs BEFORE the PS1 script so the script's
    'existing process' check sees a clean state. Failures here are
    non-fatal; the PS1 script's own check is the fallback.

    Caller passes a ChromeInstance so this filter targets the right
    Chrome. Killing the wrong Chrome (e.g. killing the primary when the
    sync browser is the one in trouble) was the root cause of the
    multi-window thrashing observed 2026-04-28.
    """
    # Backslashes need doubling for PowerShell's wildcard match. The
    # profile path is rendered as a Windows path here so the
    # CommandLine match against Chrome's --user-data-dir argument lines
    # up character-for-character.
    profile_str = str(chrome.profile_path).replace("/", "\\")
    try:
        result = subprocess.run(
            [
                "powershell",
                "-Command",
                (
                    "Get-CimInstance Win32_Process | "
                    "Where-Object { $_.Name -eq 'chrome.exe' -and "
                    f"$_.CommandLine -like '*--remote-debugging-port={chrome.port}*' -and "
                    f"$_.CommandLine -like '*{profile_str}*' " + "} | "
                    "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
                ),
            ],
            capture_output=True,
            timeout=15,
        )
        logger.info(
            "Killed stale Chrome CDP processes (port=%d, exit=%s)",
            chrome.port, result.returncode,
        )
        time.sleep(2)
    except Exception as exc:
        logger.warning(
            "Failed to kill stale Chrome processes for port %d: %s",
            chrome.port, exc,
        )


# Cookie domains that ARE deleted by the surgical clear. These are the
# OVE session/app cookies — wiping them forces a fresh OVE session
# while leaving Manheim's device-trust cookie intact, so re-auth via
# the saved-credentials path doesn't trigger a fresh 2FA challenge.
# Pattern uses SQL LIKE: 'ove.com' (exact) OR '%.ove.com' (subdomains).
_OVE_COOKIE_DOMAIN_PATTERNS: tuple[str, ...] = (
    "ove.com",      # exact host_key
    "%.ove.com",    # any *.ove.com subdomain (e.g. www.ove.com, .ove.com)
)


def _clear_chrome_cookies(settings: Settings, browser, logger) -> None:
    """Kill Chrome and SURGICALLY delete only OVE cookies, preserving
    Manheim/Cox/Okta device-trust cookies so re-auth doesn't trigger 2FA.

    2026-05-04 redesign. Pre-fix this function deleted the entire
    Cookies SQLite database, which wiped the Manheim device-trust
    cookie alongside the stale OVE session cookie. Result: every
    recovery cycle forced a fresh 2FA text to the operator's phone,
    blocking automated re-auth.

    Post-fix (this function) opens the Cookies SQLite directly and
    runs `DELETE FROM cookies WHERE host_key MATCHES ove.com pattern`,
    leaving everything else intact. The Manheim device-trust cookie
    persists, so on next visit:
      - OVE has no session cookie → redirects to Manheim auth
      - Manheim sees device-trust cookie → no 2FA required
      - Chrome auto-fills credentials → Playwright clicks Sign In
      - Redirect back to OVE with fresh session

    Falls back to full Cookies file delete if SQLite operations fail —
    we never want to leave the system in a state where stale OVE
    cookies persist (that was the original bug this whole path was
    designed to fix). The fallback restores the pre-2026-05-04
    behavior; you'll get a 2FA text but recovery still works.

    Called when OVE shows 'No Saved Searches' — the session cookie
    looks valid to OVE's page shell (no login redirect) but the
    backend token is stale, so it silently returns zero searches.
    Routes by settings.chrome_debug_port.
    """
    import sqlite3

    chrome = chrome_for_port(settings.chrome_debug_port)
    browser.close()
    _kill_stale_chrome(chrome, logger)

    cookies_path = chrome.profile_path / "Default" / "Network" / "Cookies"
    journal_path = chrome.profile_path / "Default" / "Network" / "Cookies-journal"

    if not cookies_path.exists():
        logger.info(
            "Cookies file does not exist at %s; nothing to clear", cookies_path,
        )
        return

    # Surgical SQLite delete. Try this first; fall back to full file
    # delete on any failure (better to lose device-trust than to leave
    # stale OVE cookies in place — the latter was the original
    # production bug).
    try:
        conn = sqlite3.connect(str(cookies_path), timeout=10.0)
        try:
            where_clauses = " OR ".join(
                "host_key = ?" if "%" not in p else "host_key LIKE ?"
                for p in _OVE_COOKIE_DOMAIN_PATTERNS
            )
            params = tuple(_OVE_COOKIE_DOMAIN_PATTERNS)

            # Count what we're about to delete + what we're keeping so
            # the operator can see in the log that the surgical clear
            # actually targeted only OVE cookies.
            total_before = conn.execute(
                "SELECT COUNT(*) FROM cookies"
            ).fetchone()[0]
            to_delete = conn.execute(
                f"SELECT COUNT(*) FROM cookies WHERE {where_clauses}",
                params,
            ).fetchone()[0]

            conn.execute(f"DELETE FROM cookies WHERE {where_clauses}", params)
            conn.commit()

            logger.info(
                "Surgical cookie clear (port %d): deleted %d OVE cookies, "
                "preserved %d non-OVE cookies (Manheim device-trust intact)",
                settings.chrome_debug_port,
                to_delete,
                total_before - to_delete,
            )
            return
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "Surgical cookie clear failed (%s); falling back to full file "
            "delete (operator will get a fresh 2FA text on next login)",
            exc,
        )

    # Fallback: full delete (pre-2026-05-04 behavior). Loud so operators
    # can see when the surgical path didn't work.
    for path in (cookies_path, journal_path):
        try:
            if path.exists():
                path.unlink()
                logger.info("Deleted stale cookie file: %s", path)
        except Exception as exc:
            logger.warning("Failed to delete cookie file %s: %s", path, exc)


def launch_browser_script(chrome: ChromeInstance, logger) -> None:
    """Invoke THIS Chrome instance's launcher PS1.

    Caller passes a ChromeInstance so primary/secondary recovery routes
    to the correct launcher. Pre-2026-04-28 this was hardcoded to
    start_ove_browser.ps1 and Sync-Chrome failures triggered Primary-
    Chrome relaunches (the multi-window leak).
    """
    if not chrome.launcher_script.exists():
        raise BrowserSessionError(
            f"Browser launcher script not found: {chrome.launcher_script}"
        )

    logger.info("Launching OVE browser via %s", chrome.launcher_script)
    subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(chrome.launcher_script),
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

    2. With --rescreen, every current status='hot_deal' row is moved
       back to pending and its prior screen payloads are cleared, so
       the next pipeline run performs a full fresh CR + AutoCheck +
       web-search screen before the VPS batch push.

    3. With --rescreen, every remaining step1_fail row whose cr_data
       was captured is re-run through the current screener. Rows that
       now pass are moved back to 'pending' so the next run re-screens
       them end-to-end (CR + AutoCheck + web search). Those that still
       fail stay in step1_fail with the new reason. This is how the
       2026-04-23 run's 50 false-positive VINs get recovered without
       burning another day on them.
    """
    from ove_scraper.hot_deal_db import (
        init_db,
        reclassify_autocheck_capture_failures_as_scrape_failed,
        reclassify_scraper_failures_as_scrape_failed,
        reset_hot_deals_to_pending_for_rescreen,
    )

    db_conn = init_db(settings.hot_deal_db_path)
    try:
        moved = reclassify_scraper_failures_as_scrape_failed(db_conn)
        autocheck_moved = reclassify_autocheck_capture_failures_as_scrape_failed(db_conn)
        logger.info(
            "Hot Deal reprocess: %d scraper-failed VIN(s) moved to scrape_failed for retry",
            moved,
        )
        if autocheck_moved:
            logger.info(
                "Hot Deal reprocess: %d AutoCheck capture-failure VIN(s) moved to scrape_failed for retry",
                autocheck_moved,
            )

        if not rescreen:
            logger.info(
                "Hot Deal reprocess done (scraper-error reclassification only). "
                "Next daily run will re-screen them."
            )
            return

        current_hot_deals_reset = reset_hot_deals_to_pending_for_rescreen(db_conn)
        logger.info(
            "Hot Deal reprocess (--rescreen): %d current Hot Deal VIN(s) queued "
            "for a full fresh screen before the next VPS push",
            current_hot_deals_reset,
        )

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
            moved + current_hot_deals_reset + promoted,
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
    except SavedSearchPageEmpty as exc:
        # Stale-auth recovery (2026-05-03 fix): "No Saved Searches" means
        # OVE accepted the session cookie but the backend token is dead.
        # In-page reload retries cannot fix this — they reload the same
        # bad cookies. The mirror handler in run_browser_operation
        # ([main.py: see except SavedSearchPageEmpty around line 903])
        # already exists for sync/poll; the hot-deal pipeline previously
        # swallowed the exception type and never reached this branch,
        # so all 3 daily attempts hit the same dead cookies and the
        # pipeline failed with no real recovery (the 2026-05-03
        # incident). Clear cookies + relaunch Chrome BEFORE marking
        # state failed so the next scheduler tick (~30 min later) gets
        # a fresh authenticated session.
        finish = datetime.now(EASTERN_TZ)
        reason = (
            f"SavedSearchPageEmpty (stale auth on Login A): {exc}. "
            "Cookies cleared + Chrome relaunched; next retry will use fresh session."
        )
        logger.warning(
            "Hot Deal pipeline detected stale OVE session (No Saved Searches): "
            "clearing cookies and relaunching browser before next retry"
        )
        try:
            _clear_chrome_cookies(settings, browser, logger)
            recover_browser_session(settings, browser, logger, notifier=notifier)
        except BrowserSessionError as recover_exc:
            # Recovery itself failed (likely lockout active). Mark the
            # state failed with the recovery error so the operator sees
            # both signals. Do NOT re-raise — the main loop's auth
            # handler would force a runtime rebuild that doesn't help
            # if the lockout is what's blocking us.
            logger.error(
                "Hot Deal stale-auth recovery FAILED on attempt %s: %s",
                attempts_today, recover_exc,
            )
            reason = f"SavedSearchPageEmpty + recovery failed: {recover_exc}"
        _save_hot_deal_state(state_path, {
            "last_run_date_eastern": today_str,
            "last_run_status": "failed",
            "last_run_at": finish.isoformat(),
            "attempts_today": attempts_today,
            "last_failure_reason": reason,
        })
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

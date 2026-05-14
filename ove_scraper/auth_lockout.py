"""Disk-backed cross-process auth lockout state.

This module is the single source of truth for "should the scraper be
allowed to interact with Manheim auth right now?" — and crucially, it
SURVIVES Python process restarts. The pre-existing in-memory class flag
on PlaywrightCdpBrowserSession (single-shot login click per process)
was bypassed every time the launcher loop respawned Python, which is
how the 2026-04-28 Manheim account lockout happened in the first place.

Every code path that could click Sign In, navigate to a login URL, or
relaunch Chrome MUST first call `is_blocked()` and abort if blocked.

Two lockout layers:

  1. CLICK RATE LIMITING — the click ledger keeps the timestamp of every
     login click attempt across processes. If too many clicks happen in
     a short window, further clicks are blocked even before Manheim
     itself locks the account. This is the proactive guardrail.

  2. MANHEIM ACCOUNT LOCK — when Manheim's "account locked" page is
     detected, a hard cooldown is written and respected by every
     subsequent process. Defaults to 6 hours; can be cleared manually
     with `python -m ove_scraper.main unlock`.

Per-account state (2026-05-01 split): each Chrome instance the scraper
drives is logged into a different Manheim account. Pre-split, all state
lived in a single `auth_lockout.json` shared by both ports — meaning a
B-side rate-limit breach blocked A's recovery, and a B-side Manheim
lockout could trip the global `manual_unlock_required` flag and freeze
A's session too. The split keeps per-port files for the click ledger
and account-lock cooldown, but keeps `manual_unlock_required` in a
GLOBAL file so an operator-typed `unlock` still means "unlock everything"
(no surprise where unlocking one account leaves the other blocked).

State file layout — per port (JSON, in `_state/auth_lockout_<port>.json`):

  {
    "click_history_utc": ["2026-04-28T22:00:00+00:00", ...],  # newest last, capped at 50
    "manheim_locked_until_utc": "2026-04-29T04:00:00+00:00" | null,
    "manheim_lock_reason": "Account is locked. Try again later." | null,
    "manheim_lock_count_24h": 1,
    "manheim_lock_first_at_utc": "2026-04-28T22:00:00+00:00",
    "rate_limit_until_utc": "2026-04-29T04:00:00+00:00" | null
  }

Global state file (JSON, in `_state/auth_lockout_global.json`):

  {
    "manual_unlock_required": false
  }

Concurrency: writes are atomic via tempfile-rename. Multiple readers
are safe; multiple writers race but the worst case is a slightly stale
ledger entry, which is fine — the goal is "approximate rate limit",
not "exact ledger".
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


# ---- Tunable thresholds. Conservative on purpose; the cost of being
# blocked when we shouldn't be is "scraper waits 30 min"; the cost of
# being unblocked when we shouldn't be is "Manheim locks the account
# for 24h". Bias toward over-blocking.

# Maximum number of click events to remember.
_CLICK_HISTORY_MAX = 50

# Maximum number of Chrome relaunches to remember per port.
_RELAUNCH_HISTORY_MAX = 20

# Minimum spacing between Chrome relaunches per port. Prevents the
# 2026-05-04 incident class: 6 Chrome relaunches in 9 minutes triggered
# Manheim's anti-abuse heuristic and locked Login B's account. The
# existing per-port lockout cooldown only kicks in AFTER an account-lock
# event has been recorded; this gate kicks in BEFORE, so even a panic
# relaunch loop cannot fire faster than once per 5 minutes per port.
_RELAUNCH_RATE_LIMIT_SECONDS = 5 * 60

# Rate-limit thresholds: (window_seconds, max_clicks_in_window, cooldown_seconds_after_breach).
# Evaluated in order; the FIRST matching breach sets the cooldown.
_RATE_LIMIT_RULES: tuple[tuple[int, int, int], ...] = (
    (10 * 60, 3, 30 * 60),       # >=3 clicks in 10 min  -> 30 min cooldown
    (60 * 60, 5, 2 * 60 * 60),   # >=5 clicks in 60 min  -> 2 hour cooldown
    (4 * 60 * 60, 8, 12 * 60 * 60),  # >=8 clicks in 4 hr  -> 12 hour cooldown
)

# How long a Manheim account-lock blocks us by default. Manheim's
# actual policy is undocumented; 6 hours is a safe baseline.
_MANHEIM_LOCK_DEFAULT_SECONDS = 6 * 60 * 60

# Escalation: after this many account locks within 24h, require manual unlock.
_MANHEIM_LOCK_MAX_24H_BEFORE_MANUAL = 2

# Known Chrome debug ports. Used by `unlock(port=None)` to clear EVERY
# per-port file in one operator action so "unlock" still means "unlock
# everything", and by `lockout-status` to enumerate. Kept here rather
# than imported from main.py to avoid a circular dep.
KNOWN_PORTS: tuple[int, ...] = (9222, 9223)


@dataclass(frozen=True)
class LockoutState:
    blocked: bool
    reason: str | None
    blocked_until_utc: datetime | None
    requires_manual_unlock: bool


def _state_dir(artifact_dir: Path) -> Path:
    return artifact_dir / "_state"


def _state_path(artifact_dir: Path, port: int | None) -> Path:
    """Return the on-disk JSON path for a given scope.

    `port=None` -> the GLOBAL file that holds operator-level flags
    (currently only `manual_unlock_required`). Any per-port file holds
    the click ledger + cooldowns for that Chrome instance only.
    """
    if port is None:
        return _state_dir(artifact_dir) / "auth_lockout_global.json"
    return _state_dir(artifact_dir) / f"auth_lockout_{port}.json"


def _legacy_state_path(artifact_dir: Path) -> Path:
    return _state_dir(artifact_dir) / "auth_lockout.json"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _migrate_legacy_if_needed(artifact_dir: Path) -> None:
    """One-time migration of the pre-split shared `auth_lockout.json`.

    If the legacy file exists and the new global file does NOT, we copy
    `manual_unlock_required` to the global file and the rest of the
    state into the PRIMARY port's per-port file (port 9222 — the legacy
    file pre-dated the two-Chrome split, so it could only ever have
    described the primary). Then we rename the legacy file out of the
    way so subsequent calls don't re-migrate.

    Idempotent: if the global file already exists OR the legacy file is
    absent, this is a no-op. Failures are swallowed (the migration is
    a courtesy, not load-bearing).
    """
    legacy = _legacy_state_path(artifact_dir)
    global_path = _state_path(artifact_dir, port=None)
    if global_path.exists() or not legacy.exists():
        return
    try:
        legacy_state = json.loads(legacy.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("Legacy lockout migration: cannot read %s (%s); skipping", legacy, exc)
        return

    global_state = {
        "manual_unlock_required": bool(legacy_state.get("manual_unlock_required") or False),
    }
    primary_state = {
        k: v for k, v in legacy_state.items()
        if k != "manual_unlock_required"
    }
    try:
        _save_atomic(global_path, global_state)
        _save_atomic(_state_path(artifact_dir, port=KNOWN_PORTS[0]), primary_state)
        # Move the legacy file aside so we don't re-run this on every read.
        rename_target = legacy.with_name(f"{legacy.name}.migrated_{int(time.time())}")
        os.replace(legacy, rename_target)
        LOGGER.warning(
            "Legacy auth_lockout.json migrated to per-port files. "
            "Original moved to %s",
            rename_target,
        )
    except Exception as exc:
        LOGGER.warning("Legacy lockout migration: write failed (%s); leaving legacy file in place", exc)


def _load(artifact_dir: Path, port: int | None) -> dict[str, Any]:
    _migrate_legacy_if_needed(artifact_dir)
    path = _state_path(artifact_dir, port)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        # Corrupted file is treated as "no state" rather than an error.
        # Fail-loud preference doesn't apply here: the lockout file is
        # a safety net, and refusing to start because the safety net is
        # corrupt would create a worse failure mode than just rebuilding
        # it from scratch on the next write.
        LOGGER.warning(
            "Auth lockout state file %s is unreadable (%s); treating as empty.",
            path, exc,
        )
        return {}


def _save_atomic(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file in the same directory, then rename — atomic on
    # both POSIX and Windows (NTFS) so a partial write can never leave a
    # half-baked JSON the next process would read as "no lockout".
    fd, tmp_path = tempfile.mkstemp(prefix=".auth_lockout_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def get_state(artifact_dir: Path, port: int) -> LockoutState:
    """Read the lockout state for `port` and decide whether the caller
    should be allowed to proceed. Pure read; no side effects beyond the
    one-time legacy migration in `_load`.

    The global manual-unlock flag is checked FIRST so an operator can
    park BOTH accounts with one signal, regardless of per-port state.
    """
    global_state = _load(artifact_dir, port=None)
    if global_state.get("manual_unlock_required"):
        return LockoutState(
            blocked=True,
            reason="Manual unlock required (too many Manheim lockouts in 24h). Run: python -m ove_scraper.main unlock",
            blocked_until_utc=None,
            requires_manual_unlock=True,
        )

    state = _load(artifact_dir, port=port)
    now = _now_utc()

    locked_until = _parse_dt(state.get("manheim_locked_until_utc"))
    if locked_until and now < locked_until:
        return LockoutState(
            blocked=True,
            reason=(
                f"Manheim account-lock cooldown until {_format_dt(locked_until)} "
                f"({state.get('manheim_lock_reason') or 'no reason recorded'})"
            ),
            blocked_until_utc=locked_until,
            requires_manual_unlock=False,
        )

    rate_limit_until = _parse_dt(state.get("rate_limit_until_utc"))
    if rate_limit_until and now < rate_limit_until:
        return LockoutState(
            blocked=True,
            reason=(
                f"Login-click rate limit cooldown until {_format_dt(rate_limit_until)}"
            ),
            blocked_until_utc=rate_limit_until,
            requires_manual_unlock=False,
        )

    return LockoutState(
        blocked=False,
        reason=None,
        blocked_until_utc=None,
        requires_manual_unlock=False,
    )


def is_blocked(artifact_dir: Path, port: int) -> bool:
    return get_state(artifact_dir, port).blocked


def record_login_click(artifact_dir: Path, port: int) -> LockoutState:
    """Record a login-click attempt for `port`, evaluate rate limits, and
    return the resulting state. Call this BEFORE the click — if the
    returned state is blocked, do not proceed with the click.

    The function is idempotent against duplicate calls within the same
    second (helpful for ensure_session + touch_session firing back-to-
    back at startup); it always records the timestamp, but the rate
    limit only triggers off counts within windows.
    """
    state = _load(artifact_dir, port=port)
    now = _now_utc()

    history = list(state.get("click_history_utc") or [])
    history.append(_format_dt(now))
    if len(history) > _CLICK_HISTORY_MAX:
        history = history[-_CLICK_HISTORY_MAX:]
    state["click_history_utc"] = history

    # Evaluate rate-limit rules. First breach wins.
    parsed_history = [_parse_dt(s) for s in history]
    parsed_history = [p for p in parsed_history if p is not None]
    breached_cooldown_seconds: int | None = None
    for window_seconds, threshold, cooldown_seconds in _RATE_LIMIT_RULES:
        cutoff = now - timedelta(seconds=window_seconds)
        count = sum(1 for ts in parsed_history if ts >= cutoff)
        if count >= threshold:
            breached_cooldown_seconds = cooldown_seconds
            LOGGER.error(
                "Auth lockout (port %d): rate-limit breach (%d clicks in %ds, threshold %d); "
                "applying %ds cooldown", port, count, window_seconds, threshold, cooldown_seconds,
            )
            break

    if breached_cooldown_seconds is not None:
        state["rate_limit_until_utc"] = _format_dt(now + timedelta(seconds=breached_cooldown_seconds))

    _save_atomic(_state_path(artifact_dir, port=port), state)
    return get_state(artifact_dir, port)


def record_chrome_relaunch(artifact_dir: Path, port: int) -> None:
    """Record that Chrome on `port` was just relaunched.

    Mirrors the click-ledger pattern. Call this AFTER `launch_browser_script`
    succeeds in `recover_browser_session`. Combined with
    `is_relaunch_rate_limited`, gives us a per-port floor between
    successive relaunches that prevents the panic-relaunch failure mode
    documented in the 2026-05-04 Manheim account-lock incident.
    """
    state = _load(artifact_dir, port=port)
    history = list(state.get("relaunch_history_utc") or [])
    history.append(_format_dt(_now_utc()))
    if len(history) > _RELAUNCH_HISTORY_MAX:
        history = history[-_RELAUNCH_HISTORY_MAX:]
    state["relaunch_history_utc"] = history
    _save_atomic(_state_path(artifact_dir, port=port), state)


def is_relaunch_rate_limited(artifact_dir: Path, port: int) -> tuple[bool, int]:
    """Return (is_rate_limited, last_relaunch_age_s) for `port`.

    is_rate_limited=True if any relaunch was recorded within
    `_RELAUNCH_RATE_LIMIT_SECONDS` (default 5 min) for this port.
    last_relaunch_age_s is the age of the most-recent relaunch in
    seconds, or -1 if no history exists. The age is returned even when
    not rate-limited so callers can include it in the structured log
    line for diagnostics.
    """
    state = _load(artifact_dir, port=port)
    history = list(state.get("relaunch_history_utc") or [])
    if not history:
        return False, -1
    latest = _parse_dt(history[-1])
    if latest is None:
        return False, -1
    age_seconds = int((_now_utc() - latest).total_seconds())
    if age_seconds < 0:
        # Clock skew or future timestamp; treat as not rate-limited but
        # report a 0 age so the operator notices the weird signal.
        return False, 0
    return age_seconds < _RELAUNCH_RATE_LIMIT_SECONDS, age_seconds


def record_manheim_account_locked(
    artifact_dir: Path,
    port: int,
    *,
    reason: str,
    cooldown_seconds: int = _MANHEIM_LOCK_DEFAULT_SECONDS,
) -> LockoutState:
    """Record that Manheim's account-locked page was detected for `port`.
    Sets a long cooldown (default 6h) on this port and escalates to the
    GLOBAL manual-unlock-required flag after repeated lockouts within 24h
    on this port (the global flag intentionally parks BOTH accounts so
    the operator has to confirm before either resumes)."""
    state = _load(artifact_dir, port=port)
    now = _now_utc()

    locked_until = now + timedelta(seconds=cooldown_seconds)
    state["manheim_locked_until_utc"] = _format_dt(locked_until)
    state["manheim_lock_reason"] = reason

    first_at = _parse_dt(state.get("manheim_lock_first_at_utc"))
    count_24h = int(state.get("manheim_lock_count_24h") or 0)
    if first_at is None or (now - first_at) > timedelta(hours=24):
        first_at = now
        count_24h = 1
    else:
        count_24h += 1

    state["manheim_lock_first_at_utc"] = _format_dt(first_at)
    state["manheim_lock_count_24h"] = count_24h

    _save_atomic(_state_path(artifact_dir, port=port), state)

    if count_24h >= _MANHEIM_LOCK_MAX_24H_BEFORE_MANUAL:
        global_state = _load(artifact_dir, port=None)
        global_state["manual_unlock_required"] = True
        _save_atomic(_state_path(artifact_dir, port=None), global_state)
        LOGGER.error(
            "Auth lockout: %d Manheim account-locks within 24h on port %d "
            "— GLOBAL manual unlock required (parks both accounts).",
            count_24h, port,
        )

    LOGGER.error(
        "Auth lockout (port %d): Manheim account locked. cooldown_until=%s reason=%r",
        port, _format_dt(locked_until), reason,
    )

    return get_state(artifact_dir, port)


def record_success(artifact_dir: Path, port: int) -> None:
    """Optional: record a successful auth interaction. Resets the
    24h-lockout counter for this port (NOT the click history; that
    decays naturally via the ledger window). Call this from healthy
    code paths so intermittent failures don't accumulate forever."""
    state = _load(artifact_dir, port=port)
    if state.get("manheim_lock_count_24h"):
        state["manheim_lock_count_24h"] = 0
        state["manheim_lock_first_at_utc"] = None
        _save_atomic(_state_path(artifact_dir, port=port), state)


def unlock(artifact_dir: Path, port: int | None = None) -> None:
    """Manual operator override: clear lockout state.

    `port=None` (default) -> clear EVERY per-port file plus the global
    manual-unlock flag. This is what the `unlock` CLI does — the
    operator means "resume everything".

    `port=<int>` -> clear ONE per-port file; leave the global flag
    untouched. Only useful if the operator wants to surgically resume
    one account while keeping the global park in effect.

    After unlocking, the operator MUST verify Manheim has lifted any
    account lock on their side before restarting the scraper, or the
    scraper will trigger another lock immediately.
    """
    targets: list[tuple[int | None, dict[str, Any]]] = []
    if port is None:
        global_state = _load(artifact_dir, port=None)
        global_state["manual_unlock_required"] = False
        targets.append((None, global_state))
        for known_port in KNOWN_PORTS:
            targets.append((known_port, _empty_per_port_state()))
    else:
        targets.append((port, _empty_per_port_state()))

    for tgt_port, tgt_state in targets:
        _save_atomic(_state_path(artifact_dir, port=tgt_port), tgt_state)

    LOGGER.warning(
        "Auth lockout: state cleared by manual unlock (scope=%s).",
        "all" if port is None else f"port {port}",
    )


def _empty_per_port_state() -> dict[str, Any]:
    return {
        "click_history_utc": [],
        "manheim_locked_until_utc": None,
        "manheim_lock_reason": None,
        "manheim_lock_count_24h": 0,
        "manheim_lock_first_at_utc": None,
        "rate_limit_until_utc": None,
        "relaunch_history_utc": [],
    }


def describe_state(artifact_dir: Path, port: int | None) -> str:
    """Human-readable one-line summary for log messages and CLI output.

    `port=None` describes the global file (operator-flag only).
    `port=<int>` describes a per-port file (ledger + cooldowns) and
    folds in the global manual-unlock flag so the line tells the truth
    about whether the port is effectively blocked.
    """
    if port is None:
        global_state = _load(artifact_dir, port=None)
        if global_state.get("manual_unlock_required"):
            return "GLOBAL: BLOCKED (manual_unlock_required=true)"
        return "GLOBAL: OK"

    state = _load(artifact_dir, port=port)
    lockout = get_state(artifact_dir, port)
    parts: list[str] = []
    if lockout.blocked:
        parts.append(f"BLOCKED: {lockout.reason}")
    else:
        parts.append("OK: not blocked")
    history = list(state.get("click_history_utc") or [])
    parts.append(f"clicks_logged={len(history)}")
    parts.append(f"manheim_lock_count_24h={state.get('manheim_lock_count_24h') or 0}")
    return " | ".join(parts)

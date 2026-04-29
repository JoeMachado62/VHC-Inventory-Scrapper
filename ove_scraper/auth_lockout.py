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

State file layout (JSON, in `state/auth_lockout.json`):

  {
    "click_history_utc": ["2026-04-28T22:00:00+00:00", ...],  # newest last, capped at 50
    "manheim_locked_until_utc": "2026-04-29T04:00:00+00:00" | null,
    "manheim_lock_reason": "Account is locked. Try again later." | null,
    "manheim_lock_count_24h": 1,
    "manheim_lock_first_at_utc": "2026-04-28T22:00:00+00:00",
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


@dataclass(frozen=True)
class LockoutState:
    blocked: bool
    reason: str | None
    blocked_until_utc: datetime | None
    requires_manual_unlock: bool


def _state_path(artifact_dir: Path) -> Path:
    return artifact_dir / "_state" / "auth_lockout.json"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _load(artifact_dir: Path) -> dict[str, Any]:
    path = _state_path(artifact_dir)
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


def _save_atomic(artifact_dir: Path, state: dict[str, Any]) -> None:
    path = _state_path(artifact_dir)
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


def get_state(artifact_dir: Path) -> LockoutState:
    """Read the lockout state and decide whether the caller should be
    allowed to proceed. Pure read; no side effects."""
    state = _load(artifact_dir)
    now = _now_utc()

    if state.get("manual_unlock_required"):
        return LockoutState(
            blocked=True,
            reason="Manual unlock required (too many Manheim lockouts in 24h). Run: python -m ove_scraper.main unlock",
            blocked_until_utc=None,
            requires_manual_unlock=True,
        )

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


def is_blocked(artifact_dir: Path) -> bool:
    return get_state(artifact_dir).blocked


def record_login_click(artifact_dir: Path) -> LockoutState:
    """Record a login-click attempt, evaluate rate limits, and return
    the resulting state. Call this BEFORE the click — if the returned
    state is blocked, do not proceed with the click.

    The function is idempotent against duplicate calls within the same
    second (helpful for ensure_session + touch_session firing back-to-
    back at startup); it always records the timestamp, but the rate
    limit only triggers off counts within windows.
    """
    state = _load(artifact_dir)
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
                "Auth lockout: rate-limit breach (%d clicks in %ds, threshold %d); "
                "applying %ds cooldown", count, window_seconds, threshold, cooldown_seconds,
            )
            break

    if breached_cooldown_seconds is not None:
        state["rate_limit_until_utc"] = _format_dt(now + timedelta(seconds=breached_cooldown_seconds))

    _save_atomic(artifact_dir, state)
    return get_state(artifact_dir)


def record_manheim_account_locked(
    artifact_dir: Path,
    *,
    reason: str,
    cooldown_seconds: int = _MANHEIM_LOCK_DEFAULT_SECONDS,
) -> LockoutState:
    """Record that Manheim's account-locked page was detected. Sets a
    long cooldown (default 6h) and escalates to manual-unlock-required
    after repeated lockouts within 24h."""
    state = _load(artifact_dir)
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

    if count_24h >= _MANHEIM_LOCK_MAX_24H_BEFORE_MANUAL:
        state["manual_unlock_required"] = True
        LOGGER.error(
            "Auth lockout: %d Manheim account-locks within 24h — manual unlock required.",
            count_24h,
        )

    LOGGER.error(
        "Auth lockout: Manheim account locked. cooldown_until=%s reason=%r",
        _format_dt(locked_until), reason,
    )

    _save_atomic(artifact_dir, state)
    return get_state(artifact_dir)


def record_success(artifact_dir: Path) -> None:
    """Optional: record a successful auth interaction. Resets the
    24h-lockout counter (NOT the click history; that decays naturally
    via the ledger window). Call this from healthy code paths so
    intermittent failures don't accumulate forever."""
    state = _load(artifact_dir)
    if state.get("manheim_lock_count_24h"):
        state["manheim_lock_count_24h"] = 0
        state["manheim_lock_first_at_utc"] = None
        _save_atomic(artifact_dir, state)


def unlock(artifact_dir: Path) -> None:
    """Manual operator override: clear all lockout state. Used by the
    `unlock` CLI subcommand. After unlocking, the operator MUST verify
    Manheim has lifted any account lock on their side before
    restarting the scraper, or the scraper will trigger another lock
    immediately."""
    state = _load(artifact_dir)
    state["click_history_utc"] = []
    state["manheim_locked_until_utc"] = None
    state["manheim_lock_reason"] = None
    state["manheim_lock_count_24h"] = 0
    state["manheim_lock_first_at_utc"] = None
    state["rate_limit_until_utc"] = None
    state["manual_unlock_required"] = False
    _save_atomic(artifact_dir, state)
    LOGGER.warning("Auth lockout: state cleared by manual unlock.")


def describe_state(artifact_dir: Path) -> str:
    """Human-readable one-line summary for log messages and CLI output."""
    state = _load(artifact_dir)
    lockout = get_state(artifact_dir)
    parts: list[str] = []
    if lockout.blocked:
        parts.append(f"BLOCKED: {lockout.reason}")
    else:
        parts.append("OK: not blocked")
    history = list(state.get("click_history_utc") or [])
    parts.append(f"clicks_logged={len(history)}")
    parts.append(f"manheim_lock_count_24h={state.get('manheim_lock_count_24h') or 0}")
    return " | ".join(parts)

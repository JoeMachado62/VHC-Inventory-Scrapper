"""Tests for the per-port auth_lockout state split (2026-05-01).

Pre-split: a single `auth_lockout.json` held everything, meaning a
B-side rate-limit blocked A's recovery and B's Manheim lock could
trip the global manual-unlock flag and freeze A too. Post-split:
per-port files for ledger + cooldowns, global file for the operator-
override flag.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ove_scraper import auth_lockout


def _read_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


def test_per_port_rate_limit_does_not_cross_to_other_port(tmp_path):
    """Hammer port 9222's click ledger past the 3-clicks-in-10-min
    threshold and confirm port 9223 stays unblocked. This is the
    architectural property the split was created to provide."""
    artifact_dir = tmp_path / "artifacts"
    for _ in range(3):
        auth_lockout.record_login_click(artifact_dir, port=9222)

    a_state = auth_lockout.get_state(artifact_dir, port=9222)
    b_state = auth_lockout.get_state(artifact_dir, port=9223)

    assert a_state.blocked is True, "port 9222 should be rate-limited after 3 clicks"
    assert "rate limit" in (a_state.reason or "").lower()
    assert b_state.blocked is False, "port 9223 must NOT inherit port 9222's rate limit"


def test_global_manual_unlock_blocks_both_ports(tmp_path):
    """The operator-override flag still parks BOTH accounts so a manual
    unlock command means 'resume everything', preserving the original
    safety property of the single-file design."""
    artifact_dir = tmp_path / "artifacts"
    state_dir = artifact_dir / "_state"
    _write_state(state_dir / "auth_lockout_global.json", {"manual_unlock_required": True})

    for port in (9222, 9223):
        state = auth_lockout.get_state(artifact_dir, port=port)
        assert state.blocked is True, f"port {port} should be blocked by global manual_unlock"
        assert state.requires_manual_unlock is True


def test_unlock_clears_global_and_all_known_ports(tmp_path):
    """`auth_lockout.unlock(dir)` with no port argument is the CLI
    default and must clear the global flag plus every per-port file."""
    artifact_dir = tmp_path / "artifacts"
    # Set up: rate-limit on 9223, account-lock on 9222, global park flag
    for _ in range(3):
        auth_lockout.record_login_click(artifact_dir, port=9223)
    auth_lockout.record_manheim_account_locked(
        artifact_dir, port=9222, reason="test"
    )
    state_dir = artifact_dir / "_state"
    _write_state(state_dir / "auth_lockout_global.json", {"manual_unlock_required": True})

    auth_lockout.unlock(artifact_dir)

    for port in (9222, 9223):
        assert auth_lockout.is_blocked(artifact_dir, port=port) is False, (
            f"port {port} should be unblocked after global unlock"
        )
    global_state = json.loads(
        (state_dir / "auth_lockout_global.json").read_text(encoding="utf-8")
    )
    assert global_state.get("manual_unlock_required") is False


def test_unlock_with_port_only_clears_that_port(tmp_path):
    """Targeted unlock (operator wants to resume one account while
    leaving the global park in effect) only touches that port's file."""
    artifact_dir = tmp_path / "artifacts"
    state_dir = artifact_dir / "_state"
    _write_state(state_dir / "auth_lockout_global.json", {"manual_unlock_required": True})
    for _ in range(3):
        auth_lockout.record_login_click(artifact_dir, port=9223)

    auth_lockout.unlock(artifact_dir, port=9223)

    # Per-port rate-limit cleared, but global manual-unlock still blocks.
    state = auth_lockout.get_state(artifact_dir, port=9223)
    assert state.blocked is True
    assert state.requires_manual_unlock is True


def test_legacy_state_file_is_migrated_on_first_read(tmp_path):
    """Operators upgrading from pre-split must not lose state. Drop a
    legacy auth_lockout.json with manual_unlock_required=true and
    verify get_state returns blocked AND the global file now exists
    AND the legacy file was renamed out of the way."""
    artifact_dir = tmp_path / "artifacts"
    state_dir = artifact_dir / "_state"
    legacy = state_dir / "auth_lockout.json"
    _write_state(legacy, {
        "manual_unlock_required": True,
        "click_history_utc": ["2026-04-30T12:00:00+00:00"],
        "manheim_lock_count_24h": 1,
    })

    state = auth_lockout.get_state(artifact_dir, port=9222)
    assert state.blocked is True
    assert state.requires_manual_unlock is True

    global_path = state_dir / "auth_lockout_global.json"
    primary_path = state_dir / "auth_lockout_9222.json"
    assert global_path.exists(), "global file should be created during migration"
    assert primary_path.exists(), "per-port primary file should hold pre-split ledger"
    assert not legacy.exists(), "legacy file should be renamed out of the way"

    migrated = list(state_dir.glob("auth_lockout.json.migrated_*"))
    assert len(migrated) == 1, "exactly one migration backup should exist"


def test_migration_is_idempotent(tmp_path):
    """If the global file already exists, the legacy file (if any) must
    NOT clobber it. We only migrate when global is missing."""
    artifact_dir = tmp_path / "artifacts"
    state_dir = artifact_dir / "_state"
    _write_state(state_dir / "auth_lockout_global.json", {"manual_unlock_required": False})
    _write_state(state_dir / "auth_lockout.json", {"manual_unlock_required": True})

    # Reading should NOT trigger migration because global already exists.
    state = auth_lockout.get_state(artifact_dir, port=9222)
    assert state.blocked is False  # global says unblocked, legacy ignored

    # Legacy file was left in place untouched.
    assert (state_dir / "auth_lockout.json").exists()


def test_record_manheim_account_lock_escalates_to_global_after_threshold(tmp_path):
    """Two account-locks within 24h on a single port escalate to the
    GLOBAL manual-unlock flag, parking BOTH accounts. The escalation
    is intentional — repeated lockouts indicate a deeper auth problem
    that needs operator review before either account resumes."""
    artifact_dir = tmp_path / "artifacts"
    auth_lockout.record_manheim_account_locked(artifact_dir, port=9223, reason="first")
    auth_lockout.record_manheim_account_locked(artifact_dir, port=9223, reason="second")

    # Now port 9222 (untouched directly) should also be blocked because
    # the global flag was set.
    a_state = auth_lockout.get_state(artifact_dir, port=9222)
    assert a_state.blocked is True
    assert a_state.requires_manual_unlock is True


def test_describe_state_renders_per_port(tmp_path):
    """The CLI relies on describe_state for human-readable output. The
    global describe should mention the global flag; per-port describes
    should fold in the global flag's effect on that port."""
    artifact_dir = tmp_path / "artifacts"
    state_dir = artifact_dir / "_state"
    _write_state(state_dir / "auth_lockout_global.json", {"manual_unlock_required": True})

    global_desc = auth_lockout.describe_state(artifact_dir, port=None)
    port_desc = auth_lockout.describe_state(artifact_dir, port=9222)

    assert "GLOBAL" in global_desc and "BLOCKED" in global_desc
    assert "BLOCKED" in port_desc
    assert "manual" in port_desc.lower() or "unlock" in port_desc.lower()

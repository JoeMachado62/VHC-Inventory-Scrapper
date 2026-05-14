"""Tests for the surgical Chrome cookie clear (2026-05-04 fix).

Pre-fix: `_clear_chrome_cookies` deleted the entire Chrome Cookies
SQLite database, which wiped Manheim's device-trust cookie alongside
the stale OVE session cookie. Result: every recovery cycle forced a
fresh 2FA text to the operator's phone, blocking automated re-auth.

Post-fix: opens the Cookies SQLite directly and runs `DELETE FROM
cookies WHERE host_key matches OVE pattern`, leaving Manheim/Cox/Okta
cookies intact. Falls back to full file delete if SQLite operations
fail — never leave the system in a state where stale OVE cookies
persist.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _make_chrome_profile_with_cookies(profile_root: Path, *, cookies: list[tuple[str, str]]) -> Path:
    """Create a fake Chrome profile directory containing a Cookies
    SQLite seeded with the given (host_key, name) tuples. Returns the
    path to the Cookies file. Schema mirrors Chrome 90+ so the
    surgical query has exactly the columns it expects."""
    cookies_path = profile_root / "Default" / "Network" / "Cookies"
    cookies_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(cookies_path))
    try:
        # Minimal schema — just enough columns to satisfy the surgical query.
        conn.execute(
            """
            CREATE TABLE cookies(
                creation_utc INTEGER NOT NULL,
                host_key TEXT NOT NULL,
                top_frame_site_key TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL,
                value TEXT NOT NULL,
                encrypted_value BLOB DEFAULT '',
                path TEXT NOT NULL,
                expires_utc INTEGER NOT NULL,
                is_secure INTEGER NOT NULL,
                is_httponly INTEGER NOT NULL,
                last_access_utc INTEGER NOT NULL,
                has_expires INTEGER NOT NULL DEFAULT 1,
                is_persistent INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 1,
                samesite INTEGER NOT NULL DEFAULT -1,
                source_scheme INTEGER NOT NULL DEFAULT 0,
                source_port INTEGER NOT NULL DEFAULT -1,
                last_update_utc INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        for host_key, name in cookies:
            conn.execute(
                "INSERT INTO cookies (creation_utc, host_key, name, value, path, "
                "expires_utc, is_secure, is_httponly, last_access_utc) "
                "VALUES (1, ?, ?, 'val', '/', 9999999999, 1, 1, 1)",
                (host_key, name),
            )
        conn.commit()
    finally:
        conn.close()
    return cookies_path


def _make_settings(tmp_path: Path, *, port: int, profile_root: Path):
    """Build a Settings + ChromeInstance pair so _clear_chrome_cookies
    routes to our test profile. Patches chrome_for_port to return our
    test instance."""
    from ove_scraper import main as main_module
    from ove_scraper.config import Settings

    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=port,
    )
    test_chrome = main_module.ChromeInstance(
        port=port,
        profile_path=profile_root,
        launcher_script=tmp_path / "fake_launcher.ps1",
    )
    return settings, test_chrome


def test_surgical_clear_removes_ove_cookies_only(tmp_path, monkeypatch):
    from ove_scraper import main as main_module

    profile = tmp_path / "chrome-profile"
    cookies_path = _make_chrome_profile_with_cookies(profile, cookies=[
        # OVE cookies — should be deleted
        ("ove.com", "ove_session"),
        (".ove.com", "ove_csrf"),
        ("www.ove.com", "ove_user_pref"),
        # Manheim cookies — must be preserved (device-trust lives here)
        (".manheim.com", "device_trust"),
        ("auth.manheim.com", "auth_session"),
        ("signin.manheim.com", "signin_pref"),
        # Cox / SSO chain — must be preserved
        ("authorize.coxautoinc.com", "cox_idp"),
        # Negative-pattern guards — these must NOT match the OVE pattern
        ("cove.com", "should_keep"),
        ("manheim.com", "should_keep"),
        ("move.com", "should_keep"),
    ])

    settings, test_chrome = _make_settings(tmp_path, port=9222, profile_root=profile)
    monkeypatch.setattr(main_module, "chrome_for_port", lambda _p: test_chrome)
    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)

    browser = MagicMock()
    main_module._clear_chrome_cookies(settings, browser, MagicMock())

    # Reopen and verify
    conn = sqlite3.connect(str(cookies_path))
    try:
        remaining = conn.execute(
            "SELECT host_key, name FROM cookies ORDER BY host_key, name"
        ).fetchall()
    finally:
        conn.close()

    remaining_hosts = {host for host, _ in remaining}
    assert "ove.com" not in remaining_hosts, "OVE cookies must be wiped"
    assert ".ove.com" not in remaining_hosts
    assert "www.ove.com" not in remaining_hosts
    assert ".manheim.com" in remaining_hosts, "device-trust MUST survive"
    assert "auth.manheim.com" in remaining_hosts
    assert "authorize.coxautoinc.com" in remaining_hosts
    assert "cove.com" in remaining_hosts, "false-prefix match (cove != ove)"
    assert "move.com" in remaining_hosts, "false-prefix match (move != ove)"
    assert "manheim.com" in remaining_hosts


def test_surgical_clear_succeeds_when_no_cookies_match(tmp_path, monkeypatch):
    """A profile with only non-OVE cookies should leave everything alone
    (delete count = 0) and not error."""
    from ove_scraper import main as main_module

    profile = tmp_path / "chrome-profile"
    _make_chrome_profile_with_cookies(profile, cookies=[
        (".manheim.com", "device_trust"),
        ("auth.manheim.com", "auth_session"),
    ])
    settings, test_chrome = _make_settings(tmp_path, port=9222, profile_root=profile)
    monkeypatch.setattr(main_module, "chrome_for_port", lambda _p: test_chrome)
    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)

    main_module._clear_chrome_cookies(settings, MagicMock(), MagicMock())

    # Manheim cookies should still be there.
    cookies_path = profile / "Default" / "Network" / "Cookies"
    conn = sqlite3.connect(str(cookies_path))
    try:
        rows = conn.execute("SELECT host_key FROM cookies").fetchall()
    finally:
        conn.close()
    assert len(rows) == 2


def test_surgical_clear_handles_missing_cookies_file(tmp_path, monkeypatch):
    """If the Cookies file doesn't exist (fresh profile), function
    must return cleanly without raising."""
    from ove_scraper import main as main_module

    profile = tmp_path / "chrome-profile"
    profile.mkdir()  # exists but no Default/Network/Cookies inside
    settings, test_chrome = _make_settings(tmp_path, port=9222, profile_root=profile)
    monkeypatch.setattr(main_module, "chrome_for_port", lambda _p: test_chrome)
    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)

    # Must not raise.
    main_module._clear_chrome_cookies(settings, MagicMock(), MagicMock())


def test_surgical_clear_falls_back_to_full_delete_on_sqlite_failure(tmp_path, monkeypatch):
    """If the Cookies SQLite is corrupted (or schema mismatch), surgical
    clear must fall back to deleting the file entirely so we don't
    leave stale OVE cookies in place. That was the original failure
    mode the whole _clear_chrome_cookies path was designed to fix."""
    from ove_scraper import main as main_module

    profile = tmp_path / "chrome-profile"
    cookies_dir = profile / "Default" / "Network"
    cookies_dir.mkdir(parents=True)
    cookies_path = cookies_dir / "Cookies"
    # Write garbage so SQLite open will fail on the actual query.
    cookies_path.write_bytes(b"not a sqlite database")

    settings, test_chrome = _make_settings(tmp_path, port=9222, profile_root=profile)
    monkeypatch.setattr(main_module, "chrome_for_port", lambda _p: test_chrome)
    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)

    main_module._clear_chrome_cookies(settings, MagicMock(), MagicMock())

    # Fallback should have deleted the (corrupt) Cookies file.
    assert not cookies_path.exists(), (
        "fallback must delete the corrupt Cookies file — leaving it in "
        "place would mean stale OVE cookies persist forever, which is "
        "the exact bug the whole path exists to fix"
    )


def test_subdomain_pattern_does_not_match_unrelated_domains(tmp_path, monkeypatch):
    """Belt-and-suspenders test for the SQL LIKE pattern. We use
    `host_key = 'ove.com' OR host_key LIKE '%.ove.com'`. This must NOT
    match e.g. 'cove.com', 'love.com', 'oveapps.example.com',
    'fake-ove.com'."""
    from ove_scraper import main as main_module

    profile = tmp_path / "chrome-profile"
    cookies_path = _make_chrome_profile_with_cookies(profile, cookies=[
        ("ove.com", "delete_me"),
        (".ove.com", "delete_me"),
        ("api.ove.com", "delete_me"),
        # All of these must SURVIVE
        ("cove.com", "keep"),
        ("love.com", "keep"),
        ("oveapps.example.com", "keep"),
        ("fake-ove.com", "keep"),
        ("ove.com.au", "keep"),  # different TLD
        ("not-ove.com", "keep"),
    ])
    settings, test_chrome = _make_settings(tmp_path, port=9222, profile_root=profile)
    monkeypatch.setattr(main_module, "chrome_for_port", lambda _p: test_chrome)
    monkeypatch.setattr(main_module, "_kill_stale_chrome", lambda *a, **k: None)

    main_module._clear_chrome_cookies(settings, MagicMock(), MagicMock())

    conn = sqlite3.connect(str(cookies_path))
    try:
        remaining_hosts = {row[0] for row in conn.execute("SELECT host_key FROM cookies")}
    finally:
        conn.close()

    # Deleted
    assert "ove.com" not in remaining_hosts
    assert ".ove.com" not in remaining_hosts
    assert "api.ove.com" not in remaining_hosts
    # Preserved (the false-positive guards)
    for host in ("cove.com", "love.com", "oveapps.example.com", "fake-ove.com",
                 "ove.com.au", "not-ove.com"):
        assert host in remaining_hosts, f"{host} must survive — not an OVE domain"

"""Tests for the automation lock name derivation.

The Path 2 / two-Chrome architecture (2026-04-26) introduced a per-port
lock name so the saved-search sync (running on a secondary Chrome with
Login B) gets its own Windows mutex and never serializes against the
primary Chrome that hosts hot-deal + deep-scrape work.
"""
from __future__ import annotations

from ove_scraper.automation_lock import DEFAULT_LOCK_NAME, lock_name_for_port


def test_default_port_uses_unsuffixed_name():
    # Backwards-compat: the historical single-Chrome setup uses port
    # 9222 with the unsuffixed name. An upgrade with no secondary
    # Chrome configured must produce the same mutex name as before.
    assert lock_name_for_port(9222) == DEFAULT_LOCK_NAME


def test_zero_port_uses_default_name():
    # Settings.chrome_debug_port_sync defaults to 0 ("not configured");
    # callers may pass that value directly. It must not produce a
    # weird "_0" suffix.
    assert lock_name_for_port(0) == DEFAULT_LOCK_NAME


def test_negative_port_uses_default_name():
    # Defensive: any non-positive sentinel falls back to the default.
    assert lock_name_for_port(-1) == DEFAULT_LOCK_NAME


def test_secondary_port_uses_port_suffixed_name():
    assert lock_name_for_port(9223) == r"Local\OVE_Browser_Automation_9223"


def test_different_secondary_ports_produce_different_names():
    # The whole point of the helper: two Chromes get distinct mutexes.
    assert lock_name_for_port(9223) != lock_name_for_port(9224)
    assert lock_name_for_port(9224) != lock_name_for_port(9222)


# ---------------------------------------------------------------------------
# ChromeInstance descriptor — locks in the per-Chrome routing fix
# (2026-04-28) so the recovery code can never again be hardcoded to a
# single port/profile/launcher.
# ---------------------------------------------------------------------------

def test_chrome_for_port_returns_primary_for_9222():
    from ove_scraper.main import chrome_for_port, PRIMARY_CHROME
    assert chrome_for_port(9222) is PRIMARY_CHROME


def test_chrome_for_port_returns_sync_for_9223():
    from ove_scraper.main import chrome_for_port, SYNC_CHROME
    assert chrome_for_port(9223) is SYNC_CHROME


def test_chrome_for_port_falls_back_to_primary_for_unknown():
    # Defensive: in single-Chrome mode chrome_debug_port_sync is 0 and
    # callers should still get a workable descriptor (the primary).
    from ove_scraper.main import chrome_for_port, PRIMARY_CHROME
    assert chrome_for_port(0) is PRIMARY_CHROME
    assert chrome_for_port(9999) is PRIMARY_CHROME


def test_chrome_instance_descriptors_have_distinct_profiles():
    # A bug where both descriptors pointed at the same profile_path
    # would silently deauth one Chrome whenever the other recovered.
    from ove_scraper.main import PRIMARY_CHROME, SYNC_CHROME
    assert PRIMARY_CHROME.port != SYNC_CHROME.port
    assert PRIMARY_CHROME.profile_path != SYNC_CHROME.profile_path
    assert PRIMARY_CHROME.launcher_script != SYNC_CHROME.launcher_script

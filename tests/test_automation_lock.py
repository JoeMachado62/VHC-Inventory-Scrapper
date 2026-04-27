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

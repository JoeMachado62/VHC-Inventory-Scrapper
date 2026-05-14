"""Tests for the active keepalive (2026-05-06).

The active keepalive is the response to the HTTP 401-on-graphql class
of failure that produced partial CSVs and missing VINs on 2026-05-06.

Pre-fix: persistent-tab keepalive just navigated to the saved-searches
list and waited 8s. That wasn't generating GraphQL traffic, so OVE's
data-layer Bearer access tokens expired silently.

Post-fix: each tick rotates through the user's saved searches, navigates
into ONE search, clicks pagination N times with realistic linger, and
verifies cards rendered. Generates real GraphQL traffic that exercises
the token-refresh chain — same as a real user actively shopping.
"""
from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ove_scraper.browser import BrowserSessionError, SavedSearchPageEmpty
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings


def _make_session(tmp_path: Path, *, port: int = 9223, linger_ms: int = 10,
                  pagination_clicks: int = 2) -> PlaywrightCdpBrowserSession:
    """Linger defaults to 10ms in tests so we don't actually sleep 60s
    per page. Production default is 60_000ms."""
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=port,
        keepalive_active=True,
        keepalive_pagination_clicks=pagination_clicks,
        keepalive_page_linger_ms=linger_ms,
    )
    return PlaywrightCdpBrowserSession(settings)


class _ActiveFakePage:
    """Stand-in supporting the active-keepalive flow.

    Tracks how many times each operation was invoked so tests can
    assert on click sequences. Subclasses can override to simulate
    failures."""

    def __init__(
        self,
        *,
        url: str = "https://www.ove.com/saved_searches#/",
        cards_render_ms: int = 50,
        cards_render_calls_until_fail: int | None = None,
    ) -> None:
        self.url = url
        self._closed = False
        self._cards_render_ms = cards_render_ms
        # If set to N, the Nth `wait_for_selector` call raises (sim
        # failure on subsequent paginated pages, etc.). None = always
        # succeed.
        self._cards_render_calls_until_fail = cards_render_calls_until_fail
        self.goto_call_count = 0
        self.wait_for_selector_call_count = 0
        self.wait_for_timeout_calls: list[int] = []
        self.click_calls: list[str] = []
        self.locator_queries: list[str] = []
        self.close_call_count = 0

    def goto(self, *_args, **_kwargs):
        self.goto_call_count += 1

    def wait_for_selector(self, _selector: str, *, timeout: int | None = None,
                          state: str | None = None):
        # `state` kwarg is passed by _wait_for_vehicle_results_cards
        # ("visible") and tolerated for compatibility.
        self.wait_for_selector_call_count += 1
        if (
            self._cards_render_calls_until_fail is not None
            and self.wait_for_selector_call_count > self._cards_render_calls_until_fail
        ):
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            raise PlaywrightTimeoutError("simulated cards did not render")

    def evaluate(self, _script: str):
        return False

    def wait_for_timeout(self, ms: int) -> None:
        self.wait_for_timeout_calls.append(ms)

    def close(self) -> None:
        self.close_call_count += 1
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed

    def locator(self, selector: str):
        self.locator_queries.append(selector)
        return _ActiveFakeLocator(self, selector)

    def get_by_text(self, *_args, **_kwargs):
        return _ActiveFakeLocator(self, "text-fallback")

    def get_by_role(self, *_args, **_kwargs):
        return _ActiveFakeLocator(self, "role-fallback")


class _ActiveFakeLocator:
    """Fake Locator: claims to find pagination buttons so click()
    succeeds. Tests that want to simulate "no next page" override
    via subclass / monkey-patch."""

    def __init__(self, page: _ActiveFakePage, selector: str) -> None:
        self._page = page
        self._selector = selector

    @property
    def first(self):
        return self

    def count(self) -> int:
        return 1

    def is_visible(self, **_kw) -> bool:
        return True

    def is_enabled(self, **_kw) -> bool:
        return True

    def click(self, **_kw) -> None:
        self._page.click_calls.append(self._selector)


class _FakeBrowser:
    contexts: list = []


def _wire(session, monkeypatch, page, *, search_names=("East-Hub-2015-2021",
                                                       "East-Hub-2022",
                                                       "East-Hub-2023")):
    """Common stubs: connect, get_or_create_keepalive_page, open_saved_search,
    list_saved_searches, is_login_page."""
    monkeypatch.setattr(session, "_connect_browser", lambda: _FakeBrowser())
    monkeypatch.setattr(session, "_get_or_create_keepalive_page", lambda _b: page)
    # _open_saved_search clicks into a search; we just record the call.
    opened: list[str] = []
    def _fake_open(_p, name): opened.append(name)
    monkeypatch.setattr(session, "_open_saved_search", _fake_open)
    monkeypatch.setattr(session, "list_saved_searches", lambda: search_names)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)
    return opened


def _find_log(caplog, needle: str) -> str | None:
    for record in caplog.records:
        if needle in record.getMessage():
            return record.getMessage()
    return None


def test_active_keepalive_visits_one_search_per_tick_and_rotates(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, pagination_clicks=2, linger_ms=5)
    page = _ActiveFakePage()
    opened = _wire(
        session, monkeypatch, page,
        search_names=("Search-A", "Search-B", "Search-C"),
    )

    with caplog.at_level(logging.INFO, logger="ove_scraper.cdp_browser"):
        session.touch_session()
        session.touch_session()
        session.touch_session()
        session.touch_session()  # 4th tick wraps back to Search-A

    assert opened == ["Search-A", "Search-B", "Search-C", "Search-A"], (
        "active keepalive must round-robin through saved searches"
    )


def test_active_keepalive_clicks_pagination_n_times(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, pagination_clicks=3, linger_ms=5)
    page = _ActiveFakePage()
    _wire(session, monkeypatch, page)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    # 3 pagination clicks expected
    assert len(page.click_calls) == 3, (
        f"expected 3 pagination clicks (configured), got {len(page.click_calls)}"
    )

    # KEEPALIVE_TICK log should record pagination_clicks=3
    msg = _find_log(caplog, "KEEPALIVE_TICK")
    assert msg is not None
    assert "pagination_clicks=3" in msg
    assert "tab_strategy=active" in msg
    assert "outcome=ok" in msg


def test_active_keepalive_lingers_per_page(tmp_path, monkeypatch):
    """Linger is invoked AFTER each render: once for page 1, then once
    after each pagination click. With 2 pagination_clicks, that's 3
    total wait_for_timeout calls."""
    session = _make_session(tmp_path, pagination_clicks=2, linger_ms=12345)
    page = _ActiveFakePage()
    _wire(session, monkeypatch, page)

    session.touch_session()

    # Three lingers: initial page + after click 1 + after click 2
    linger_calls = [ms for ms in page.wait_for_timeout_calls if ms == 12345]
    assert len(linger_calls) == 3, (
        f"expected 3 page-linger calls of 12345ms, got {linger_calls}"
    )


def test_active_keepalive_records_search_in_telemetry(tmp_path, monkeypatch, caplog):
    session = _make_session(tmp_path, pagination_clicks=1, linger_ms=5)
    page = _ActiveFakePage()
    _wire(session, monkeypatch, page, search_names=("East-Hub-2025-2026",))

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    msg = _find_log(caplog, "KEEPALIVE_TICK")
    assert msg is not None
    assert "search_visited=East-Hub-2025-2026" in msg


def test_active_keepalive_caches_saved_search_names_after_first_lookup(tmp_path, monkeypatch):
    """list_saved_searches is expensive — should be called at most once
    (on first tick), then cached."""
    session = _make_session(tmp_path, pagination_clicks=1, linger_ms=5)
    page = _ActiveFakePage()
    list_call_count = {"n": 0}
    def _spy():
        list_call_count["n"] += 1
        return ("A", "B", "C")
    monkeypatch.setattr(session, "_connect_browser", lambda: _FakeBrowser())
    monkeypatch.setattr(session, "_get_or_create_keepalive_page", lambda _b: page)
    monkeypatch.setattr(session, "_open_saved_search", lambda _p, _n: None)
    monkeypatch.setattr(session, "list_saved_searches", _spy)
    monkeypatch.setattr(session, "_is_login_page", lambda _p: False)

    session.touch_session()
    session.touch_session()
    session.touch_session()

    assert list_call_count["n"] == 1, (
        "saved-search names should be cached after first lookup; "
        f"list_saved_searches was called {list_call_count['n']} times"
    )


def test_active_keepalive_recovers_when_cards_dont_render(tmp_path, monkeypatch, caplog):
    """Per user requirement: all saved searches have thousands of
    listings, so empty cards = real session problem. Active keepalive
    must propagate SavedSearchPageEmpty to trigger surgical cookie
    clear + recovery."""
    session = _make_session(tmp_path, pagination_clicks=2, linger_ms=5)
    page = _ActiveFakePage(cards_render_calls_until_fail=0)  # FIRST render fails
    _wire(session, monkeypatch, page)

    # _wait_for_saved_search_cards converts the wait_for_selector
    # PlaywrightTimeoutError to BrowserSessionError("...did not render")
    # (or SavedSearchPageEmpty if the page text matches the empty-state
    # copy). The active keepalive must NOT swallow these.
    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        with pytest.raises((SavedSearchPageEmpty, BrowserSessionError)):
            session.touch_session()

    msg = _find_log(caplog, "KEEPALIVE_TICK")
    assert msg is not None
    assert "outcome=cards_did_not_render" in msg or "outcome=cards_empty" in msg

    # Persistent page reference cleared so next tick gets a fresh tab.
    assert session._keepalive_page is None


def test_active_keepalive_pagination_failure_is_non_fatal(tmp_path, monkeypatch, caplog):
    """If pagination on page 2 fails (e.g. cards don't render after
    clicking next), the tick still SUCCEEDS. The initial render proved
    the session works; pagination is bonus exercise."""
    session = _make_session(tmp_path, pagination_clicks=3, linger_ms=5)
    # First wait_for_selector (initial render) succeeds.
    # Second wait_for_selector (after pagination click 1) fails.
    page = _ActiveFakePage(cards_render_calls_until_fail=1)
    _wire(session, monkeypatch, page)

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()  # MUST NOT raise

    msg = _find_log(caplog, "KEEPALIVE_TICK")
    assert msg is not None
    assert "outcome=ok" in msg, "tick must succeed even if later pagination fails"


def test_active_keepalive_no_saved_searches_falls_back_gracefully(tmp_path, monkeypatch, caplog):
    """If list_saved_searches returns empty (rare — would mean OVE has
    NO saved searches at all), the keepalive should log and skip the
    tick rather than raising."""
    session = _make_session(tmp_path, pagination_clicks=2, linger_ms=5)
    page = _ActiveFakePage()
    _wire(session, monkeypatch, page, search_names=())

    with caplog.at_level(logging.WARNING, logger="ove_scraper.cdp_browser"):
        session.touch_session()

    # No saved searches → no exception, and KEEPALIVE_TICK still logs.
    assert _find_log(caplog, "no saved searches to rotate") is not None


def test_active_keepalive_disabled_falls_back_to_persistent_tab(tmp_path, monkeypatch):
    """When KEEPALIVE_ACTIVE=false but KEEPALIVE_PERSISTENT_TAB=true,
    the dispatcher should call the persistent-tab keepalive (legacy)."""
    settings = Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "scraper.log",
        chrome_debug_port=9223,
        keepalive_active=False,
        keepalive_persistent_tab=True,
    )
    session = PlaywrightCdpBrowserSession(settings)

    # Track which inner method gets called.
    active_calls = {"n": 0}
    persistent_calls = {"n": 0}
    worker_calls = {"n": 0}
    monkeypatch.setattr(
        session, "_touch_session_active",
        lambda: active_calls.update(n=active_calls["n"] + 1),
    )
    monkeypatch.setattr(
        session, "_touch_session_persistent_tab",
        lambda: persistent_calls.update(n=persistent_calls["n"] + 1),
    )
    monkeypatch.setattr(
        session, "_touch_session_worker_tab",
        lambda: worker_calls.update(n=worker_calls["n"] + 1),
    )

    session.touch_session()

    assert persistent_calls["n"] == 1
    assert active_calls["n"] == 0
    assert worker_calls["n"] == 0

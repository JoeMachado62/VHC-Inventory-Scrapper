from __future__ import annotations

import json

import pytest

from ove_scraper.browser import BrowserSessionError, ManheimAuthRedirectError
from ove_scraper.cdp_browser import (
    PlaywrightCdpBrowserSession,
    _extract_image_urls_from_html,
    _extract_stockwave_image_urls,
)
from ove_scraper.config import Settings


class FakePage:
    def __init__(self, url: str, indicators: dict[str, object] | None = None) -> None:
        self.url = url
        self._indicators = indicators or {}

    def evaluate(self, _script: str):
        return self._indicators


class FakeLocator:
    def __init__(self) -> None:
        self.actions: list[str] = []

    def click(self, timeout: int | None = None) -> None:
        self.actions.append("click")

    def fill(self, value: str) -> None:
        self.actions.append(f"fill:{value}")

    def press(self, value: str) -> None:
        self.actions.append(f"press:{value}")


class FakeSearchPage(FakePage):
    def __init__(self, url: str, indicators: dict[str, object] | None = None) -> None:
        super().__init__(url, indicators)
        self.waits: list[tuple[str, int | None]] = []

    def wait_for_load_state(self, state: str, timeout: int | None = None) -> None:
        self.waits.append((state, timeout))

    def wait_for_timeout(self, timeout: int) -> None:
        self.waits.append(("timeout", timeout))


def make_settings(tmp_path) -> Settings:
    return Settings(
        vch_api_base_url="https://example.com/v1",
        vch_service_token="token",
        artifact_dir=tmp_path / "artifacts",
        export_dir=tmp_path / "exports",
        log_file_path=tmp_path / "sync.log",
    )


def test_is_login_page_detects_login_form_without_login_url(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakePage(
        "https://ove.example.com/buy#/",
        {
            "hasPasswordField": True,
            "combinedText": "sign in username password forgot password",
        },
    )

    assert session._is_login_page(page) is True


def test_submit_vin_search_checks_auth_before_filling_vin(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeSearchPage(
        "https://ove.example.com/buy#/",
        {
            "hasPasswordField": True,
            "combinedText": "sign in username password",
        },
    )
    locator = FakeLocator()
    session._find_vin_search_input = lambda current_page: locator  # type: ignore[method-assign]

    try:
        session._submit_vin_search(page, "1HGCM82633A004352")
    except BrowserSessionError as exc:
        assert "not authenticated" in str(exc)
    else:
        raise AssertionError("expected BrowserSessionError")

    assert locator.actions == []


def test_extract_stockwave_image_urls_returns_full_size_unique_gallery_images() -> None:
    raw = json.dumps(
        {
            "mainImage": {
                "largeUrl": "https://images.cdn.manheim.com/example-vehicle-1.jpg",
                "smallUrl": "https://images.cdn.manheim.com/example-vehicle-1.jpg?size=w86h64",
            },
            "images": [
                {
                    "largeUrl": "https://images.cdn.manheim.com/example-vehicle-2.jpg",
                    "smallUrl": "https://images.cdn.manheim.com/example-vehicle-2.jpg?size=w86h64",
                },
                {
                    "largeUrl": "https://images.cdn.manheim.com/example-vehicle-3.jpg",
                },
            ],
        }
    )

    assert _extract_stockwave_image_urls(raw) == [
        "https://images.cdn.manheim.com/example-vehicle-1.jpg",
        "https://images.cdn.manheim.com/example-vehicle-2.jpg",
        "https://images.cdn.manheim.com/example-vehicle-3.jpg",
    ]


class FakeCrUnavailablePage:
    """Minimal Page stand-in whose innerText contains Manheim's
    stale-session 'condition reports are not available right now'
    error copy."""

    def __init__(self, *, url: str, body_text: str, title: str = "") -> None:
        self.url = url
        self._body_text = body_text
        self._title = title
        self._closed = False

    def evaluate(self, _script: str):
        return self._body_text

    def title(self) -> str:
        return self._title

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True


def test_is_cr_unavailable_page_detects_manheim_stale_session_copy(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeCrUnavailablePage(
        url="https://insightcr.manheim.com/cr/x",
        body_text="Sorry, condition reports are not available right now. Please try again later.",
    )

    assert session._is_cr_unavailable_page(page) is True


def test_is_cr_unavailable_page_does_not_misfire_on_data_not_available(tmp_path) -> None:
    """The OVE detail page legitimately contains 'Data Not Available' in
    the vehicle-history section — this must NOT be treated as a CR
    unavailability signal, or every scrape would falsely route to
    auth_expired."""
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeCrUnavailablePage(
        url="https://www.ove.com/buy#/details/X/OVE/conditionInformation",
        body_text="Vehicle History  Data Not Available  AutoCheck Score 92",
    )

    assert session._is_cr_unavailable_page(page) is False


def test_inspect_cr_popups_raises_on_auth_popup(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    auth_popup = FakeCrUnavailablePage(
        url="https://auth.manheim.com/as/authorization.oauth2?x=y",
        body_text="username password sign in",
        title="Sign In",
    )

    with pytest.raises(ManheimAuthRedirectError):
        session._inspect_cr_popups_for_auth(
            [auth_popup],
            tmp_path / "artifacts",
            "https://insightcr.manheim.com/cr/x",
        )


def test_inspect_cr_popups_raises_on_unavailable_popup(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    stale_popup = FakeCrUnavailablePage(
        url="https://insightcr.manheim.com/cr/x",
        body_text="Sorry, condition reports are not available right now.",
    )

    with pytest.raises(ManheimAuthRedirectError):
        session._inspect_cr_popups_for_auth(
            [stale_popup],
            tmp_path / "artifacts",
            "https://insightcr.manheim.com/cr/x",
        )


class FakeLoginLocator:
    def __init__(self, *, visible: bool = True, present: bool = True) -> None:
        self._visible = visible
        self._present = present
        self.clicks = 0

    @property
    def first(self):
        return self

    def count(self) -> int:
        return 1 if self._present else 0

    def is_visible(self, timeout: int | None = None) -> bool:
        return self._visible

    def click(self, timeout: int | None = None) -> None:
        self.clicks += 1


class FakeLoginPage:
    """Page stand-in for testing the single-shot auto-login click. The
    indicators dict drives _is_login_page; the password_filled flag
    drives the JS pre-fill check; url_after_click simulates the URL
    flipping + login form disappearing after a successful click."""

    _INDICATORS_LOGIN = {
        "hasPasswordField": True,
        "combinedText": "sign in username password",
    }
    _INDICATORS_NOT_LOGIN = {
        "hasPasswordField": False,
        "combinedText": "welcome to ove buy cars",
    }

    def __init__(
        self,
        *,
        url: str,
        password_filled: bool = True,
        url_after_click: str | None = None,
        button_findable: bool = True,
    ) -> None:
        self.url = url
        self._password_filled = password_filled
        self._url_after_click = url_after_click
        self._on_login = True
        self._submit = FakeLoginLocator(present=button_findable)
        self.timeouts: list[int] = []

    def evaluate(self, script: str):
        # The pw-populated probe uniquely uses pw.value.length; the
        # _is_login_page probe uses document.title / combinedText.
        if "pw.value.length" in script:
            return self._password_filled
        return self._INDICATORS_LOGIN if self._on_login else self._INDICATORS_NOT_LOGIN

    def locator(self, selector: str):
        return self._submit

    def get_by_role(self, role, name=None):
        return self._submit

    def wait_for_timeout(self, ms: int) -> None:
        self.timeouts.append(ms)
        # After the click + first wait poll, flip to non-login state
        # so _is_login_page returns False on the next check.
        if self._submit.clicks > 0 and self._url_after_click is not None:
            self.url = self._url_after_click
            self._on_login = False


def _reset_auto_login_flag() -> None:
    from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
    PlaywrightCdpBrowserSession._auto_login_last_attempt_at = None


def test_single_shot_auto_login_clicks_and_returns_true_when_url_changes(tmp_path) -> None:
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click="https://www.ove.com/buy#/",
    )

    assert session._try_single_shot_login_click(page) is True
    assert page._submit.clicks == 1
    # Timestamp is set after the click attempt, gating any subsequent
    # call within the cooldown window.
    assert PlaywrightCdpBrowserSession._auto_login_last_attempt_at is not None


def test_single_shot_auto_login_does_not_click_if_password_not_prefilled(tmp_path) -> None:
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=False,
    )

    assert session._try_single_shot_login_click(page) is False
    assert page._submit.clicks == 0
    # The timestamp IS set — we used our one chance per cooldown and
    # found the precondition unmet. Operator must log in manually.
    assert PlaywrightCdpBrowserSession._auto_login_last_attempt_at is not None


def test_single_shot_auto_login_is_single_shot_within_cooldown(tmp_path) -> None:
    """Regression guard against the Manheim account lock incident.
    Within the cooldown window, the scraper only ever clicks Sign In
    ONCE — even if multiple subsystems hit the recovery helper."""
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    first_page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click="https://www.ove.com/buy#/",
    )
    second_page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click="https://www.ove.com/buy#/",
    )

    first_result = session._try_single_shot_login_click(first_page)
    second_result = session._try_single_shot_login_click(second_page)

    assert first_result is True
    assert first_page._submit.clicks == 1
    # Second call is blocked by the cooldown — no click attempted.
    assert second_result is False
    assert second_page._submit.clicks == 0


def test_auto_login_allows_new_click_after_cooldown_elapsed(tmp_path) -> None:
    """Fix 2 (2026-04-30): the timestamp-based cooldown replaces the
    boolean flag so long-running processes can recover from rare
    auth events. After the cooldown window elapses, a fresh click
    is allowed (still rate-limited by the cross-process click
    ledger)."""
    from datetime import datetime, timedelta, timezone
    from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession

    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))

    first_page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click="https://www.ove.com/buy#/",
    )
    assert session._try_single_shot_login_click(first_page) is True
    assert first_page._submit.clicks == 1

    # Move the timestamp backward beyond the cooldown so the next
    # call is allowed. Real wall-clock waiting would make the test
    # multi-hour; we instead manipulate the class state directly.
    PlaywrightCdpBrowserSession._auto_login_last_attempt_at = (
        datetime.now(timezone.utc)
        - PlaywrightCdpBrowserSession._AUTO_LOGIN_COOLDOWN
        - timedelta(seconds=1)
    )

    second_page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click="https://www.ove.com/buy#/",
    )
    assert session._try_single_shot_login_click(second_page) is True
    assert second_page._submit.clicks == 1


def test_single_shot_auto_login_does_not_consume_cooldown_when_button_not_found(tmp_path) -> None:
    """Regression for the 2026-05-07 AutoCheck OAuth incident.

    Pre-fix: when the Sign-In selector chain failed to match (e.g.
    against PingFederate's <a id="signOnButton"> on auth.manheim.com,
    which our original selectors didn't recognize), the in-process
    cooldown was already consumed BEFORE the locator chain ran. That
    silently disabled auto-login for 6h on every subsequent VIN —
    every "Skipping auto-login click: last attempt 0:00:01 ago"
    log line came from this bug.

    Post-fix: button-not-found is treated as a config/page-mismatch
    (not a consumed Manheim auth attempt), so the cooldown stays
    untouched and the next call gets a fresh chance once selectors
    are updated."""
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeLoginPage(
        url="https://auth.manheim.com/as/authorization.oauth2",
        password_filled=True,
        button_findable=False,
    )

    result = session._try_single_shot_login_click(page)

    assert result is False
    assert page._submit.clicks == 0
    # The critical assertion: cooldown must NOT have been consumed.
    assert PlaywrightCdpBrowserSession._auto_login_last_attempt_at is None


def test_single_shot_auto_login_returns_false_if_still_on_login_after_click(tmp_path) -> None:
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    # url_after_click=None means the URL never changes, so _is_login_page
    # keeps returning True and the wait loop times out.
    page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=True,
        url_after_click=None,
    )
    # Patch monotonic so the 10s wait budget exhausts in a few iterations.
    import ove_scraper.cdp_browser as cdp_browser_mod
    original_monotonic = cdp_browser_mod.time.monotonic
    ticks = iter([0.0] + [t * 1.5 for t in range(1, 20)])
    cdp_browser_mod.time.monotonic = lambda: next(ticks)  # type: ignore[assignment]
    try:
        result = session._try_single_shot_login_click(page)
    finally:
        cdp_browser_mod.time.monotonic = original_monotonic

    assert result is False
    assert page._submit.clicks == 1  # click fired once, then no retry


def test_inspect_cr_popups_ignores_closed_and_benign_popups(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    benign = FakeCrUnavailablePage(
        url="https://insightcr.manheim.com/cr/x#details",
        body_text="Condition Report  Overall Grade 3.5  Exterior  Frame",
    )
    closed = FakeCrUnavailablePage(url="about:blank", body_text="")
    closed._closed = True

    session._inspect_cr_popups_for_auth(
        [benign, closed],
        tmp_path / "artifacts",
        "https://insightcr.manheim.com/cr/x",
    )


def test_extract_image_urls_from_html_dedupes_thumbnail_variants() -> None:
    html = """
    <script>
      window.__DATA__ = {
        "mainImage": {
          "largeUrl": "https://images.cdn.manheim.com/example-vehicle-1.jpg",
          "smallUrl": "https://images.cdn.manheim.com/example-vehicle-1.jpg?size=w86h64"
        }
      };
    </script>
    <img src="https://images.cdn.manheim.com/example-vehicle-2.jpg?size=w344h256" />
    <img src="https://images.cdn.manheim.com/example-vehicle-2.jpg" />
    <img src="https://images.cdn.manheim.com/20170803033944-cbd21c63-18e2-4a36-ab01-c6ffe8dc14b8.jpg" />
    """

    assert _extract_image_urls_from_html(html) == [
        "https://images.cdn.manheim.com/example-vehicle-1.jpg",
        "https://images.cdn.manheim.com/example-vehicle-2.jpg",
        "https://images.cdn.manheim.com/20170803033944-cbd21c63-18e2-4a36-ab01-c6ffe8dc14b8.jpg",
    ]


# ----------------------------------------------------------------------
# 2026-04-25: hash-route navigation as primary CR-open strategy. The
# click-based flow has a 100% historical failure rate for insightcr-
# hosted listings because the OVE React app does not render the CR
# anchor for protocol-relative insightcr URLs. The hash-route fallback
# triggers the same React route handler the click would have produced.
# ----------------------------------------------------------------------

class FakeHashRoutePage:
    """Minimal Page double for testing hash-route logic. Tracks evaluate
    calls (hash mutation) and time-controlled URL transitions so we can
    assert the polling loop terminates correctly.
    """
    def __init__(self, initial_url: str, target_url: str | None = None,
                 transition_after_calls: int = 1,
                 unavailable_text: str = "") -> None:
        self.url = initial_url
        self._target_url = target_url
        self._transition_after = transition_after_calls
        self.evaluate_calls: list[tuple[str, object]] = []
        self.wait_for_timeout_calls: list[int] = []
        self.wait_for_load_state_calls: list[tuple[str, int | None]] = []
        self._url_polls = 0
        self._unavailable_text = unavailable_text
        self._title = ""
        self._content = ""

    def evaluate(self, script: str, *args):
        self.evaluate_calls.append((script, args[0] if args else None))
        # If the hash setter is called, track that and let URL transition.
        return self._unavailable_text if "innerText" in script else None

    def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_for_timeout_calls.append(timeout_ms)
        # Each timeout call advances the simulated URL.
        self._url_polls += 1
        if (self._target_url is not None and
                self._url_polls >= self._transition_after):
            self.url = self._target_url

    def wait_for_load_state(self, state: str, timeout: int | None = None) -> None:
        self.wait_for_load_state_calls.append((state, timeout))

    def title(self) -> str:
        return self._title

    def content(self) -> str:
        return self._content

    def screenshot(self, **_kwargs) -> None:
        pass


def test_extract_vin_from_source_page_parses_ove_detail_url(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakePage("https://www.ove.com/search/results#/details/1FT7W2BT4REE13166/OVE")

    assert session._extract_vin_from_source_page(page) == "1FT7W2BT4REE13166"


def test_extract_vin_from_source_page_returns_none_when_url_lacks_details(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakePage("https://www.ove.com/search/results")

    assert session._extract_vin_from_source_page(page) is None


def test_extract_vin_from_source_page_uppercases_vin(tmp_path) -> None:
    """OVE URLs sometimes lowercase the VIN; ensure we normalize."""
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakePage("https://www.ove.com/search/results#/details/1ft7w2bt4ree13166/OVE")

    assert session._extract_vin_from_source_page(page) == "1FT7W2BT4REE13166"


def test_open_via_hash_route_evaluates_correct_hash_string(tmp_path) -> None:
    """The hash mutation must use the canonical
    #/details/{VIN}/OVE/conditionInformation route so React Router
    actually mounts the CR view."""
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    vin = "1HGCY1F27SA019762"
    target_url = f"https://www.ove.com/search/results#/details/{vin}/OVE/conditionInformation"
    page = FakeHashRoutePage(
        initial_url=f"https://www.ove.com/search/results#/details/{vin}/OVE",
        target_url=target_url,
        transition_after_calls=1,
    )

    result = session._open_via_hash_route(
        page, vin, "https://insightcr.manheim.com/cr-display?...", tmp_path,
    )

    # The first evaluate call should set window.location.hash to the
    # canonical conditionInformation route.
    assert page.evaluate_calls, "evaluate() was never called"
    first_script, first_arg = page.evaluate_calls[0]
    assert "window.location.hash" in first_script
    assert first_arg == f"#/details/{vin}/OVE/conditionInformation"
    # On URL transition we expect the page to be returned.
    assert result is page


def test_open_via_hash_route_returns_none_if_route_never_settles(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    vin = "1FT7W2BT4REE13166"
    # transition_after_calls=999 means the URL never reaches the CR route
    # within the polling budget.
    page = FakeHashRoutePage(
        initial_url=f"https://www.ove.com/search/results#/details/{vin}/OVE",
        target_url=None,
        transition_after_calls=999,
    )

    # Patch time.monotonic-bound deadline by mocking time.monotonic.
    # Easier: rely on wait_for_timeout consuming most polls and the loop
    # exiting after the deadline. We don't have access to monotonic;
    # instead, verify the function returns None when URL never matches
    # by asserting it's not the success branch (returns None on timeout
    # — but our test would take ~20s without a mock). We patch out the
    # deadline by setting the regex to never match and trusting the
    # timing-bound exit.
    import time as _time
    real_monotonic = _time.monotonic
    fake_now = [real_monotonic()]
    def fake_monotonic():
        fake_now[0] += 5.0  # each call advances 5s; loop exits within 4 polls
        return fake_now[0]
    _time.monotonic = fake_monotonic
    try:
        result = session._open_via_hash_route(
            page, vin, "https://insightcr.manheim.com/cr-display?...", tmp_path,
        )
    finally:
        _time.monotonic = real_monotonic

    assert result is None
    # Even on failure, the hash mutation should have been attempted once.
    assert any("window.location.hash" in script for script, _ in page.evaluate_calls)


def test_open_via_hash_route_raises_on_auth_redirect(tmp_path) -> None:
    """If the hash-route lands on auth.manheim.com, we must propagate
    ManheimAuthRedirectError so the caller routes to auth_expired
    instead of falling through to a doomed click attempt."""
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    vin = "1FT7W2BT4REE13166"
    auth_url = "https://auth.manheim.com/as/authorization.oauth2?x=y"
    page = FakeHashRoutePage(
        initial_url=f"https://www.ove.com/search/results#/details/{vin}/OVE",
        target_url=f"https://www.ove.com/search/results#/details/{vin}/OVE/conditionInformation",
        transition_after_calls=1,
    )
    # The flow: hash setter → poll → URL becomes conditionInformation →
    # _CR_HASH_ROUTE_RE matches → wait_for_load_state called → then
    # _raise_if_auth_redirect inspects page.url. We swap page.url to
    # auth_url INSIDE wait_for_load_state so the auth detector sees it.
    def wait_then_redirect(state, timeout=None):
        page.wait_for_load_state_calls.append((state, timeout))
        page.url = auth_url
    page.wait_for_load_state = wait_then_redirect

    with pytest.raises(ManheimAuthRedirectError):
        session._open_via_hash_route(
            page, vin, "https://insightcr.manheim.com/cr-display?...", tmp_path,
        )


# ----------------------------------------------------------------------
# 2026-04-26: AutoCheck Snapshot Report parser fixes. Forensic analysis
# of 188 captured autocheck-report.html files showed:
#   - 178 of 182 snapshots had buyback_protection extracted as
#     "Program." because the regex matched descriptive text inside the
#     title-brand explainer block instead of the actual finding.
#   - All 182 captures had trailing "More info \n More info" tooltip
#     noise concatenated to every check value.
#   - 5 of 188 captures were Experian "Your request cannot be
#     processed" error pages stored as scrape_status='success' with
#     empty fields.
# ----------------------------------------------------------------------

# Realistic Snapshot Report body text — pulled from a captured artifact.
# Inner-text-style: newlines preserved, contiguous whitespace collapsed.
_SNAPSHOT_OK_TEXT = """\
Welcome to AutoCheck - Snapshot Report
 AutoCheck Report
 2023 Jeep Wrangler Rubicon
 SUV 2D
 VIN: 1C4HJXCG7PW628257
No. of Historical Events
8
Calculated Owners
1
Number of Accidents
0
 Last Reported Event Date: 03/03/2026
 Last Reported Mileage: 17,790
 96
 90
 95
 This vehicle's AutoCheck Score: 96
 Other comparable 2023 vehicles in the SUV - Midsize typically score between 90-95.
Major State Title Brand Check -
 OK
 More info
 More info
Major State Title Brand Check
AutoCheck checks the following major title brands. If there are any title brand below
reported, the vehicle is not qualified for Buyback Protection Program.
Fire brand
Salvage brand
Accident Check -
 OK
 More info
 More info
Damage Check -
 OK
 More info
 More info
Odometer Check -
 OK
 More info
 More info
Other Title Brand and Specific Event Check -
 OK
 More info
 More info
Vehicle Usage Check -
 Personal Use
 More info
 More info
AutoCheck Buyback Protection -
 Qualifies
 More info
 More info
"""

_SNAPSHOT_BRANDED_TEXT = _SNAPSHOT_OK_TEXT.replace(
    "Major State Title Brand Check -\n OK\n More info\n More info",
    "Major State Title Brand Check -\n Problem Reported\n More info\n More info",
).replace(
    "AutoCheck Buyback Protection -\n Qualifies",
    "AutoCheck Buyback Protection -\n Does Not Qualify",
)


def test_parse_autocheck_buyback_protection_extracts_actual_value() -> None:
    """Regression: prior regex matched 'Buyback Protection Program.' in the
    title-brand explainer block. Anchored-on-'AutoCheck Buyback Protection -'
    fix must extract the real finding instead."""
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_OK_TEXT)
    assert parsed["buyback_protection"] == "Qualifies"
    assert "Program" not in parsed["buyback_protection"]


def test_parse_autocheck_buyback_protection_extracts_does_not_qualify() -> None:
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_BRANDED_TEXT)
    assert parsed["buyback_protection"] == "Does Not Qualify"


def test_parse_autocheck_strips_more_info_trailing_noise() -> None:
    """All check fields must come out clean, not with trailing
    '\\n More info \\n More info' tooltip text."""
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_OK_TEXT)
    assert parsed["title_brand_check"] == "OK", parsed["title_brand_check"]
    assert parsed["accident_check"] == "OK"
    assert parsed["damage_check"] == "OK"
    assert parsed["odometer_check"] == "OK"
    assert parsed["vehicle_use"] == "Personal Use"
    # No "More info" anywhere in any extracted value.
    for field in ("title_brand_check", "accident_check", "damage_check",
                  "odometer_check", "vehicle_use", "buyback_protection"):
        assert "More info" not in parsed[field], (field, parsed[field])


def test_parse_autocheck_problem_reported_still_caught() -> None:
    """The screener relies on 'problem reported' substring to fail
    branded-title VINs. The cleanup must not strip the keyword."""
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_BRANDED_TEXT)
    assert parsed["title_brand_check"] == "Problem Reported"
    assert "problem reported" in parsed["title_brand_check"].lower()


def test_parse_autocheck_score_owners_accidents_extracted() -> None:
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_OK_TEXT)
    assert parsed["autocheck_score"] == 96
    assert parsed["owner_count"] == 1
    assert parsed["accident_count"] == 0


def test_parse_autocheck_graphical_report_fields_extracted() -> None:
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_content(_SNAPSHOT_OK_TEXT)
    assert parsed["historical_event_count"] == 8
    assert parsed["last_reported_event_date"] == "03/03/2026"
    assert parsed["last_reported_mileage"] == "17,790"
    assert parsed["score_range_low"] == 90
    assert parsed["score_range_high"] == 95
    assert parsed["comparable_vehicle_year"] == 2023
    assert parsed["comparable_vehicle_class"] == "SUV - Midsize"
    assert parsed["other_title_brand_specific_event_check"] == "OK"


def test_clean_check_value_handles_edge_cases() -> None:
    clean = PlaywrightCdpBrowserSession._clean_check_value
    # Standard snapshot output
    assert clean("OK \n More info \n More info") == "OK"
    # Single trailing tooltip
    assert clean("Problem Reported\n More info") == "Problem Reported"
    # No tooltip — pass through unchanged but trimmed
    assert clean("  Personal Use  ") == "Personal Use"
    # Empty / falsy
    assert clean("") == ""
    assert clean(None) == ""  # type: ignore[arg-type]
    # Multi-word value with internal spaces preserved
    assert clean("Does Not Qualify\nMore info\nMore info") == "Does Not Qualify"


def test_is_experian_error_page_detects_request_cannot_be_processed() -> None:
    detect = PlaywrightCdpBrowserSession._is_experian_error_page
    # The actual ~3KB error stub Experian returns
    assert detect("Your request cannot be processed h1 { color: #004986 }") is True
    # Variant casing
    assert detect("YOUR REQUEST CANNOT BE PROCESSED") is True
    # Real snapshot — must NOT be flagged
    assert detect("Welcome to AutoCheck - Snapshot Report ...") is False
    # Empty / None safe
    assert detect("") is False
    assert detect(None) is False  # type: ignore[arg-type]


def test_parse_autocheck_listing_json_fallback_extracts_score_range_and_checks() -> None:
    parsed = PlaywrightCdpBrowserSession._parse_autocheck_listing_json(
        {
            "autocheck": {
                "odometerCheckOK": True,
                "vehicleUseAndEventCheckOK": False,
                "titleAndProblemCheckOK": True,
                "numberOfAccidents": 1,
                "ownerCount": 2,
                "score": 94,
                "compareScoreRangeLow": 91,
                "compareScoreRangeHigh": 96,
            }
        }
    )

    assert parsed["autocheck_score"] == 94
    assert parsed["score_range_low"] == 91
    assert parsed["score_range_high"] == 96
    assert parsed["owner_count"] == 2
    assert parsed["accident_count"] == 1
    assert parsed["title_brand_check"] == "OK"
    assert parsed["other_title_brand_specific_event_check"] == "OK"
    assert parsed["odometer_check"] == "OK"
    assert parsed["accident_check"] == "Information Reported(1)"
    assert parsed["vehicle_use"] == "Other Use Reported"


class FakeDetailDetectionPage:
    def __init__(self, url: str, evaluate_result: bool) -> None:
        self.url = url
        self.evaluate_result = evaluate_result

    def evaluate(self, _script: str, *_args):
        return self.evaluate_result


def test_page_looks_like_detail_rejects_search_results_even_with_vin(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    vin = "1C4HJXEN9NW250788"
    page = FakeDetailDetectionPage(
        f"https://www.ove.com/search/results#/results?keyword={vin}",
        evaluate_result=True,
    )

    assert session._page_looks_like_detail_for_vin(page, vin) is False


def test_page_looks_like_detail_accepts_detail_route(tmp_path) -> None:
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    vin = "1C4HJXEN9NW250788"
    page = FakeDetailDetectionPage(
        f"https://www.ove.com/search/results#/details/{vin}/OVE",
        evaluate_result=False,
    )

    assert session._page_looks_like_detail_for_vin(page, vin) is True


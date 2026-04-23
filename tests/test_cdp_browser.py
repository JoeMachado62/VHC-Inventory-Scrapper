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
    def __init__(self, *, visible: bool = True) -> None:
        self._visible = visible
        self.clicks = 0

    @property
    def first(self):
        return self

    def count(self) -> int:
        return 1

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
    ) -> None:
        self.url = url
        self._password_filled = password_filled
        self._url_after_click = url_after_click
        self._on_login = True
        self._submit = FakeLoginLocator()
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
    PlaywrightCdpBrowserSession._auto_login_attempted_this_process = False


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
    assert PlaywrightCdpBrowserSession._auto_login_attempted_this_process is True


def test_single_shot_auto_login_does_not_click_if_password_not_prefilled(tmp_path) -> None:
    _reset_auto_login_flag()
    session = PlaywrightCdpBrowserSession(make_settings(tmp_path))
    page = FakeLoginPage(
        url="https://login.example.com/as/authorization",
        password_filled=False,
    )

    assert session._try_single_shot_login_click(page) is False
    assert page._submit.clicks == 0
    # The flag IS set — we used our one chance and found the
    # precondition unmet. Operator must log in manually.
    assert PlaywrightCdpBrowserSession._auto_login_attempted_this_process is True


def test_single_shot_auto_login_is_process_wide_single_shot(tmp_path) -> None:
    """Regression guard against the Manheim account lock incident.
    Even if the scraper tries multiple times to recover auth within
    one Python process, we only ever click Sign In ONCE."""
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
    # Second call is blocked by the process-wide flag — no click attempted.
    assert second_result is False
    assert second_page._submit.clicks == 0


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

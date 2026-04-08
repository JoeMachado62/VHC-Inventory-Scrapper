from __future__ import annotations

import json

from ove_scraper.browser import BrowserSessionError
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

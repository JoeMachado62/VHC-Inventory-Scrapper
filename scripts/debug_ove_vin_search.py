from __future__ import annotations

import json
import sys

from ove_scraper.config import Settings, load_env_file
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession


def main() -> None:
    vin = sys.argv[1]
    load_env_file()
    settings = Settings.from_env()
    browser = PlaywrightCdpBrowserSession(settings)
    try:
        browser.ensure_session()
        live_browser = browser._connect_browser()
        page = browser._get_ove_page(live_browser.contexts)
        page.goto(settings.ove_listings_url, wait_until="domcontentloaded")

        search = browser._find_vin_search_input(page)
        search.click(timeout=10000)
        search.fill(vin)
        search.press("Enter")
        page.wait_for_timeout(4000)
        page.wait_for_load_state("domcontentloaded")

        payload = page.evaluate(
            f"""
            () => {{
              const vin = {json.dumps(vin)};
              const bodyText = document.body.innerText || "";
              return {{
                title: document.title,
                url: location.href,
                bodyIncludesVin: bodyText.includes(vin),
                bodySnippet: bodyText.slice(0, 4000),
                anchors: Array.from(document.querySelectorAll("a")).map((a) => ({{
                  text: (a.textContent || "").trim().slice(0, 160),
                  href: a.href,
                  className: a.className,
                }})).filter((a) => a.text.includes(vin) || a.href.includes(vin)).slice(0, 50),
                vinNodes: Array.from(document.querySelectorAll("*")).map((el) => ({{
                  tag: el.tagName,
                  text: (el.textContent || "").trim().slice(0, 160),
                  className: el.className || "",
                }})).filter((el) => el.text.includes(vin)).slice(0, 50),
              }};
            }}
            """
        )
        print(json.dumps(payload, indent=2))
    finally:
        browser.close()


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import sys

from ove_scraper.config import Settings, load_env_file
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession


def main() -> None:
    url = sys.argv[1]
    load_env_file()
    settings = Settings.from_env()
    browser = PlaywrightCdpBrowserSession(settings)
    try:
        browser.ensure_session()
        live_browser = browser._connect_browser()
        context = live_browser.contexts[0]
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        payload = page.evaluate(
            """
            () => ({
              title: document.title,
              url: location.href,
              bodyText: (document.body?.innerText || "").slice(0, 8000),
              tables: Array.from(document.querySelectorAll("table")).slice(0, 10).map((table) => ({
                text: (table.innerText || "").slice(0, 2000),
              })),
            })
            """
        )
        print(json.dumps(payload, indent=2))
        page.close()
    finally:
        browser.close()


if __name__ == "__main__":
    main()

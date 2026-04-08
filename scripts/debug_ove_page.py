from __future__ import annotations

import json

from ove_scraper.config import Settings, load_env_file
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession


def main() -> None:
    load_env_file()
    settings = Settings.from_env()
    browser = PlaywrightCdpBrowserSession(settings)
    try:
        browser.ensure_session()
        live_browser = browser._connect_browser()
        page = browser._get_ove_page(live_browser.contexts)
        payload = page.evaluate(
            """
            () => ({
              title: document.title,
              url: location.href,
              inputs: Array.from(document.querySelectorAll("input")).slice(0, 40).map((i) => ({
                type: i.type,
                name: i.name,
                placeholder: i.placeholder,
                id: i.id,
                className: i.className,
              })),
              buttons: Array.from(document.querySelectorAll("button,a")).slice(0, 120).map((el) => ({
                tag: el.tagName,
                text: (el.textContent || "").trim().slice(0, 120),
                href: el.getAttribute("href"),
                id: el.id,
                className: el.className,
              })),
            })
            """
        )
        print(json.dumps(payload, indent=2))
    finally:
        browser.close()


if __name__ == "__main__":
    main()

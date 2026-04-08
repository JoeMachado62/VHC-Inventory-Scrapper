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
              const anchors = Array.from(document.querySelectorAll("a, button, [role='button']"));
              const interesting = anchors.map((node) => {{
                const text = (node.textContent || "").trim();
                const href = node.getAttribute("href");
                const title = node.getAttribute("title");
                const aria = node.getAttribute("aria-label");
                const cls = node.className?.toString?.() || "";
                const outer = node.outerHTML.slice(0, 800);
                return {{ text, href, title, aria, className: cls, outer }};
              }}).filter((item) => {{
                const signal = `${{item.text}} ${{item.href || ""}} ${{item.title || ""}} ${{item.aria || ""}} ${{item.className || ""}}`.toLowerCase();
                return signal.includes("cr") || signal.includes("condition") || signal.includes("4.7") || signal.includes(vin.toLowerCase());
              }});

              const vinCard = Array.from(document.querySelectorAll("*")).find((el) => (el.textContent || "").includes("2022 BMW 7 Series"));
              const cardHtml = vinCard ? (vinCard.closest("a, div, article, li, section")?.outerHTML || vinCard.outerHTML).slice(0, 4000) : null;

              return {{
                url: location.href,
                interesting,
                cardHtml,
              }};
            }}
            """
        )
        print(json.dumps(payload, indent=2))
    finally:
        browser.close()


if __name__ == "__main__":
    main()

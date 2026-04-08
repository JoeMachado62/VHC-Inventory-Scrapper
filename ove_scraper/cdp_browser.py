from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Download,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

from ove_scraper.browser import BrowserSessionError, DeepScrapeResult, ListingNotFoundError
from ove_scraper.condition_report_normalizer import normalize_condition_report
from ove_scraper.config import Settings
from ove_scraper.cr_parsers import identify_report_family
from ove_scraper.schemas import ConditionReport, ListingSnapshot

LOGGER = logging.getLogger(__name__)
AUTH_PATH_HINTS = ("/auth/", "/login")
LOGIN_COPY_HINTS = (
    "sign in",
    "log in",
    "login",
    "forgot password",
    "reset password",
    "username",
    "user id",
    "email address",
    "dealer id",
)
LOGIN_FIELD_HINTS = ("user", "login", "email", "dealer", "account", "sign")
VIN_SEARCH_FIELD_HINTS = ("vin", "make", "search", "keyword")


DETAIL_EXTRACTION_SCRIPT = """
() => {
  const root = document.querySelector(ROOT_SELECTOR) || document.body;
  const pickText = (selectors) => {
    for (const selector of selectors) {
      const node = document.querySelector(selector);
      if (node && node.textContent && node.textContent.trim()) {
        return node.textContent.trim();
      }
    }
    return null;
  };

  const collectFactsFromNode = (container) => {
    const facts = [];
    if (!container) return facts;

    const dtNodes = container.querySelectorAll("dt");
    dtNodes.forEach((dt) => {
      const dd = dt.nextElementSibling;
      const label = dt.textContent?.trim();
      const value = dd?.textContent?.trim();
      if (label && value) facts.push({ label, value });
    });

    const rows = container.querySelectorAll("tr");
    rows.forEach((row) => {
      const cells = row.querySelectorAll("th, td");
      if (cells.length >= 2) {
        const label = cells[0].textContent?.trim();
        const value = cells[1].textContent?.trim();
        if (label && value) facts.push({ label, value });
      }
    });

    const candidates = container.querySelectorAll("[class*='detail'], [class*='fact'], [class*='spec'], [class*='stat']");
    candidates.forEach((node) => {
      const labelNode = node.querySelector("[class*='label'], [class*='title'], strong, b");
      const valueNode = node.querySelector("[class*='value'], [class*='content'], span, div");
      const label = labelNode?.textContent?.trim();
      const value = valueNode?.textContent?.trim();
      if (label && value && label !== value) facts.push({ label, value });
    });

    const seen = new Set();
    return facts.filter((fact) => {
      const key = `${fact.label}::${fact.value}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  };

  const sections = [];
  root.querySelectorAll("section, article, .panel, .card, [class*='section']").forEach((section) => {
    const titleNode = section.querySelector("h1, h2, h3, h4, [class*='heading'], [class*='title']");
    const title = titleNode?.textContent?.trim();
    const paragraphs = Array.from(section.querySelectorAll("p, li"))
      .map((node) => node.textContent?.trim())
      .filter(Boolean)
      .slice(0, 30);
    const facts = collectFactsFromNode(section);
    if (title || facts.length || paragraphs.length) {
      sections.push({ title: title || "Section", facts, paragraphs });
    }
  });

  const icons = [];
  root.querySelectorAll("svg, use, img[alt], img[title], [role='img']").forEach((node) => {
    const label = node.getAttribute("aria-label") || node.getAttribute("alt") || node.getAttribute("title") || null;
    if (node.tagName.toLowerCase() === "svg") {
      icons.push({ kind: "svg", label, svg_markup: node.outerHTML, source_url: null });
      return;
    }
    if (node.tagName.toLowerCase() === "use") {
      icons.push({ kind: "sprite", label, svg_markup: null, source_url: node.getAttribute("href") || node.getAttribute("xlink:href") || null });
      return;
    }
    icons.push({ kind: "image", label, svg_markup: null, source_url: node.getAttribute("src") || null });
  });

  const images = Array.from(root.querySelectorAll("img"))
    .map((img) => img.currentSrc || img.src)
    .filter((src) => src && /^https?:/i.test(src));

  const findConditionReportLink = () => {
    const normalizeHref = (value) => {
      if (!value) return null;
      try {
        return new URL(value, window.location.href).href;
      } catch (_err) {
        return value;
      }
    };

    const scorePattern = /^[0-5](\\.\\d)?$/;
    const candidates = [];

    const registerCandidate = (node, reason) => {
      if (!node) return;
      const href = normalizeHref(
        node.getAttribute?.("href") ||
        node.dataset?.href ||
        node.dataset?.url ||
        node.getAttribute?.("data-href") ||
        node.getAttribute?.("data-url")
      );
      const text = node.textContent?.trim() || null;
      const title = node.getAttribute?.("title") || node.getAttribute?.("aria-label") || null;
      if (!href && !text) return;
      candidates.push({ href, text, title, reason });
    };

    root.querySelectorAll("a[data-test-id='condition-report'], a.VehicleReportLink__condition-report-link, a, button, [role='button']").forEach((node) => {
      const text = node.textContent?.trim() || "";
      const href = node.getAttribute?.("href") || "";
      const title = node.getAttribute?.("title") || node.getAttribute?.("aria-label") || "";
      const labelText = node.getAttribute?.("data-label-text") || "";
      const valueText = node.getAttribute?.("data-value-text") || "";
      const className = node.className?.toString?.() || "";
      const signal = `${text} ${href} ${title} ${labelText} ${valueText} ${className}`.toLowerCase();
      if (
        signal.includes("condition report") ||
        signal.includes("cond report") ||
        signal.includes("conditionreport") ||
        signal.includes("condition-report") ||
        labelText === "CR"
      ) {
        registerCandidate(node, "explicit-condition-report");
      } else if (signal.includes(" cr ") || signal.startsWith("cr") || signal.includes("grade")) {
        registerCandidate(node, "cr-or-grade");
      }
    });

    root.querySelectorAll("*").forEach((node) => {
      const text = node.textContent?.trim() || "";
      if (!scorePattern.test(text)) return;
      const anchor = node.closest?.("a, button, [role='button']") || node.parentElement?.closest?.("a, button, [role='button']");
      if (anchor) registerCandidate(anchor, "score-node-parent");
    });

    const deduped = [];
    const seen = new Set();
    for (const candidate of candidates) {
      const key = `${candidate.href || ""}::${candidate.text || ""}::${candidate.reason}`;
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(candidate);
    }

    const scored = deduped.map((candidate) => {
      let score = 0;
      const signal = `${candidate.href || ""} ${candidate.text || ""} ${candidate.title || ""}`.toLowerCase();
      if ((candidate.text || "").match(scorePattern)) score += 4;
      if ((candidate.href || "").includes("inspectionreport.manheim.com")) score += 20;
      if ((candidate.href || "").includes("insightcr.manheim.com")) score += 20;
      if ((candidate.href || "").includes("content.liquidmotors.com/IR/")) score += 20;
      if ((candidate.href || "").includes("mmsc400.manheim.com/MABEL/ECR2I.htm")) score += 20;
      if (signal.includes("condition report")) score += 8;
      if (signal.includes("condition")) score += 4;
      if (signal.includes("report")) score += 3;
      if (signal.includes("grade")) score += 2;
      if (signal.includes("/condition")) score += 6;
      if (signal.includes("/report")) score += 4;
      if ((candidate.href || "").includes("order_condition_reports")) score -= 25;
      return { ...candidate, score };
    }).sort((a, b) => b.score - a.score);

    return scored[0] || null;
  };

  return {
    title: pickText(["h1", "[class*='vehicle-title']", "[class*='listing-title']"]),
    subtitle: pickText(["h2", "[class*='subtitle']", "[class*='vehicle-subtitle']"]),
    badges: Array.from(root.querySelectorAll(".badge, [class*='badge'], [class*='chip']")).map((node) => node.textContent?.trim()).filter(Boolean),
    hero_facts: collectFactsFromNode(root).slice(0, 20),
    sections,
    icons,
    images,
    seller_comments: pickText(["[class*='seller-comment']", "[class*='comments']", "#seller-comments"]),
    condition_report_link: findConditionReportLink(),
    page_url: window.location.href,
    body_text: root.innerText || ""
  };
}
"""


class PlaywrightCdpBrowserSession:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None

    def close(self) -> None:
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except RuntimeError:
                pass
        self._playwright = None
        self._browser = None

    def ensure_session(self) -> None:
        try:
            browser = self._connect_browser()
            page = self._get_ove_page(browser.contexts)
            page.wait_for_load_state("domcontentloaded", timeout=5000)
            if self._is_login_page(page):
                raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            if self.settings.ove_base_url not in page.url or self._is_error_page(page):
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded")
        except Exception as exc:
            self.close()
            raise BrowserSessionError(f"OVE browser session unavailable: {exc}") from exc

    def touch_session(self) -> None:
        try:
            browser = self._connect_browser()
            page = self._open_dedicated_ove_page(browser)
            try:
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1000)
                if self._is_login_page(page):
                    raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            finally:
                self._close_page(page)
        except Exception as exc:
            self.close()
            raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc

    def list_saved_searches(self) -> tuple[str, ...]:
        browser = self._connect_browser()
        page = self._open_dedicated_ove_page(browser)
        try:
            page.goto(self._saved_searches_url(), wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            names = page.evaluate(
                """
                () => {
                  const seen = new Set();
                  const values = [];
                  const pushValue = (value) => {
                    if (!value) return;
                    const cleaned = value
                      .replace(/New\\s*\\(\\d+\\)$/i, "")
                      .replace(/\\s+/g, " ")
                      .trim();
                    if (!cleaned || seen.has(cleaned)) return;
                    seen.add(cleaned);
                    values.push(cleaned);
                  };

                  const explicit = Array.from(document.querySelectorAll("[data-test-id^='search name:']"));
                  explicit.forEach((node) => {
                    const testId = node.getAttribute("data-test-id") || "";
                    const match = testId.match(/^search name:\\s*(.+)$/i);
                    if (match) pushValue(match[1]);
                  });
                  if (values.length) return values;

                  const cardTitles = Array.from(document.querySelectorAll("a, button, [role='button'], h1, h2, h3, h4"))
                    .map((node) => (node.textContent || "").replace(/\\s+/g, " ").trim())
                    .filter((text) => /hub/i.test(text) && !/^find a saved search$/i.test(text) && !/view all/i.test(text))
                    .filter((text) => text.length < 80);
                  cardTitles.forEach(pushValue);
                  return values;
                }
                """
            )
            search_names = tuple(name for name in names if isinstance(name, str) and name.strip())
            if not search_names:
                raise BrowserSessionError("No saved searches were visible on the OVE saved-search page")
            return search_names
        finally:
            self._close_page(page)

    def export_saved_search(self, search_name: str, export_dir: Path) -> Path:
        export_dir.mkdir(parents=True, exist_ok=True)
        browser = self._connect_browser()
        page = self._open_dedicated_ove_page(browser)
        target_path = export_dir / f"{slugify(search_name)}.csv"
        last_error: Exception | None = None

        try:
            for attempt in range(3):
                try:
                    self._remove_file_if_present(target_path)
                    self._open_saved_search(page, search_name)
                    download = self._trigger_export(page)
                    self._persist_download(download, target_path, search_name)
                    return target_path
                except (BrowserSessionError, PlaywrightTimeoutError) as exc:
                    last_error = exc

                page.goto(self._saved_searches_url(), wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
        finally:
            self._close_page(page)

        if last_error is None:
            raise BrowserSessionError(f"Could not export saved search '{search_name}'")
        raise BrowserSessionError(str(last_error))

    def deep_scrape_vin(self, vin: str) -> DeepScrapeResult:
        self.ensure_session()
        artifact_dir = self.settings.artifact_dir / vin
        artifact_dir.mkdir(parents=True, exist_ok=True)

        browser = self._connect_browser()
        seed_page = self._get_ove_page(browser.contexts)
        page = self._create_worker_page(seed_page.context, start_url=self._vin_results_url())
        try:
            LOGGER.info("Stage open_listing: starting for VIN %s", vin)
            detail_page, result_card_report_link = self._open_listing_for_vin(page, vin, artifact_dir)
            LOGGER.info("Stage extract_payload: starting for VIN %s", vin)
            payload = detail_page.evaluate(
                DETAIL_EXTRACTION_SCRIPT.replace("ROOT_SELECTOR", json.dumps(self.settings.ove_section_root_selector))
            )
            html_path = artifact_dir / "listing.html"
            screenshot_path = artifact_dir / "listing.png"
            html_path.write_text(detail_page.content(), encoding="utf-8")
            detail_page.screenshot(path=str(screenshot_path), full_page=True)

            condition_report_link = self._select_valid_condition_report_link(
                result_card_report_link,
                payload.get("condition_report_link"),
            )
            LOGGER.info(
                "Stage capture_condition_report: starting for VIN %s (link present=%s)",
                vin,
                bool(condition_report_link),
            )
            report_page_payload = self._capture_condition_report_page(browser, detail_page, condition_report_link, artifact_dir)
            report_page_text = report_page_payload.get("body_text") if report_page_payload else None

            snapshot = ListingSnapshot(
                title=payload.get("title"),
                subtitle=payload.get("subtitle"),
                page_url=payload.get("page_url"),
                badges=[{"label": badge} for badge in clean_strings(payload.get("badges", []))],
                hero_facts=clean_fact_items(payload.get("hero_facts", [])),
                sections=clean_section_items(payload.get("sections", [])),
                icons=clean_icon_items(payload.get("icons", [])),
                raw_html_ref=str(html_path),
                screenshot_refs=[str(screenshot_path)],
                metadata={
                    "captured_at": int(time.time()),
                    "condition_report_link": condition_report_link,
                    "condition_report_page": report_page_payload,
                },
            )
            condition_report = build_condition_report(snapshot, condition_report_link)
            condition_report = normalize_condition_report(
                condition_report,
                raw_text=report_page_text or payload.get("body_text"),
                report_link=condition_report_link,
            )
            if condition_report and report_page_payload:
                condition_report.metadata = {
                    **condition_report.metadata,
                    "report_page": report_page_payload,
                }
            seller_comments = payload.get("seller_comments")
            images = unique_urls([
                *(payload.get("images", []) or []),
                *((report_page_payload or {}).get("images", []) or []),
            ])

            result = DeepScrapeResult(
                images=images,
                condition_report=condition_report,
                seller_comments=seller_comments,
                listing_snapshot=snapshot,
            )
            LOGGER.info(
                "Stage assemble_result: complete for VIN %s (images=%d, has_condition_report=%s)",
                vin,
                len(images),
                condition_report is not None,
            )
            return result
        finally:
            self._close_page(page)

    def _connect_browser(self) -> Browser:
        if self._browser is not None:
            try:
                _ = self._browser.contexts
                return self._browser
            except Exception:
                self._browser = None

        if self._playwright is None:
            self._playwright = sync_playwright().start()

        try:
            self._browser = self._playwright.chromium.connect_over_cdp(
                f"http://{self.settings.chrome_debug_host}:{self.settings.chrome_debug_port}"
            )
        except Exception as exc:
            self._browser = None
            raise BrowserSessionError(
                f"Unable to connect to Chrome CDP at "
                f"http://{self.settings.chrome_debug_host}:{self.settings.chrome_debug_port}"
            ) from exc
        return self._browser

    def _get_ove_page(self, contexts: list[BrowserContext]) -> Page:
        for context in contexts:
            for page in context.pages:
                if (
                    self.settings.ove_base_url in page.url
                    and not self._is_login_page(page)
                    and not self._is_error_page(page)
                ):
                    return page
        for context in contexts:
            if context.pages:
                page = context.pages[0]
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded")
                if self._is_login_page(page) or self._is_error_page(page):
                    raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
                return page
        raise BrowserSessionError("No browser pages available in the CDP session")

    def _open_dedicated_ove_page(self, browser: Browser) -> Page:
        seed_page = self._get_ove_page(browser.contexts)
        return self._create_worker_page(seed_page.context)

    def _close_page(self, page: Page | None) -> None:
        if page is None:
            return
        try:
            if not page.is_closed():
                page.close()
        except Exception:
            pass

    def _create_worker_page(self, context: BrowserContext, *, start_url: str | None = None) -> Page:
        page = context.new_page()
        page.goto(start_url or self._saved_searches_url(), wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        if self._is_login_page(page) or self._is_error_page(page):
            self._close_page(page)
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
        return page

    def _open_saved_search(self, page: Page, search_name: str) -> None:
        page.goto(self._saved_searches_url(), wait_until="domcontentloaded")
        matched_name = self._resolve_saved_search_name(page, search_name)
        selectors = [
            f"[data-test-id='search name: {matched_name}']",
            self.settings.ove_saved_search_link_selector.format(search_name=matched_name),
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count():
                    locator.click(timeout=10000)
                    page.wait_for_load_state("networkidle")
                    return
            except PlaywrightTimeoutError:
                continue
        title_locator = page.get_by_text(matched_name, exact=False).first
        try:
            title_locator.click(timeout=10000)
            page.wait_for_load_state("networkidle")
            return
        except PlaywrightTimeoutError as exc:
            raise BrowserSessionError(f"Unable to locate saved search '{search_name}'") from exc

    def _resolve_saved_search_name(self, page: Page, search_name: str) -> str:
        available = self._collect_saved_search_names(page)
        if not available:
            return search_name

        exact_match = next((name for name in available if name == search_name), None)
        if exact_match is not None:
            return exact_match

        target_key = self._normalize_saved_search_name(search_name)
        normalized_matches = [name for name in available if self._normalize_saved_search_name(name) == target_key]
        if normalized_matches:
            return normalized_matches[0]

        target_signature = self._saved_search_signature(search_name)
        signature_matches = [name for name in available if self._saved_search_signature(name) == target_signature]
        if signature_matches:
            return signature_matches[0]

        same_region = [
            name
            for name in available
            if self._saved_search_signature(name)[0] == target_signature[0]
        ]
        candidates = same_region or list(available)
        ranked = sorted(
            candidates,
            key=lambda name: self._saved_search_match_score(search_name, name),
            reverse=True,
        )
        best = ranked[0]
        if self._saved_search_match_score(search_name, best) <= 0:
            return search_name
        return best

    def _collect_saved_search_names(self, page: Page) -> tuple[str, ...]:
        names = page.evaluate(
            """
            () => {
              const seen = new Set();
              const values = [];
              const pushValue = (value) => {
                if (!value) return;
                const cleaned = value
                  .replace(/New\\s*\\(\\d+\\)$/i, "")
                  .replace(/\\s+/g, " ")
                  .trim();
                if (!cleaned || seen.has(cleaned)) return;
                seen.add(cleaned);
                values.push(cleaned);
              };

              const explicit = Array.from(document.querySelectorAll("[data-test-id^='search name:']"));
              explicit.forEach((node) => {
                const testId = node.getAttribute("data-test-id") || "";
                const match = testId.match(/^search name:\\s*(.+)$/i);
                if (match) pushValue(match[1]);
              });

              const clickable = Array.from(document.querySelectorAll("a, button, [role='button']"));
              clickable.forEach((node) => pushValue(node.textContent || ""));
              return values.filter((text) => /hub/i.test(text) && text.length < 80);
            }
            """
        )
        return tuple(name for name in names if isinstance(name, str) and name.strip())

    def _normalize_saved_search_name(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", value.lower())

    def _saved_search_signature(self, value: str) -> tuple[str, str]:
        lowered = value.lower()
        region = "east" if "east" in lowered else "west" if "west" in lowered else ""
        normalized = lowered.replace("newer", "or newer")
        years = re.findall(r"\d{4}", normalized)
        if "or newer" in normalized and years:
            bucket = f"{years[0]}+"
        elif len(years) >= 2:
            bucket = f"{years[0]}-{years[1]}"
        elif len(years) == 1:
            bucket = years[0]
        else:
            bucket = self._normalize_saved_search_name(value)
        return region, bucket

    def _saved_search_match_score(self, requested: str, candidate: str) -> int:
        requested_signature = self._saved_search_signature(requested)
        candidate_signature = self._saved_search_signature(candidate)
        requested_key = self._normalize_saved_search_name(requested)
        candidate_key = self._normalize_saved_search_name(candidate)
        score = 0
        if requested_signature[0] and requested_signature[0] == candidate_signature[0]:
            score += 10
        if requested_signature[1] and requested_signature[1] == candidate_signature[1]:
            score += 20
        requested_years = set(re.findall(r"\d{4}", requested))
        candidate_years = set(re.findall(r"\d{4}", candidate))
        score += len(requested_years & candidate_years) * 3
        if requested_key in candidate_key or candidate_key in requested_key:
            score += 5
        return score

    def _trigger_export(self, page: Page) -> Download:
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(2000)

        direct_download_locators = [
            page.get_by_role("button", name=re.compile(r"export\s*csv", re.I)).first,
            page.get_by_role("link", name=re.compile(r"export\s*csv", re.I)).first,
            page.get_by_text("EXPORT CSV", exact=False).first,
            page.get_by_text("Export CSV", exact=False).first,
        ]
        for locator in direct_download_locators:
            download = self._try_download_click(page, locator)
            if download is not None:
                return download

        trigger_locators = [
            page.locator("#export-action-button").first,
            page.locator("[data-testid='export-data-button']").first,
            page.get_by_role("button", name=re.compile(r"^export$", re.I)).first,
            page.locator(self.settings.ove_export_button_selector).first,
            page.get_by_text("EXPORT", exact=False).first,
            page.get_by_text("Export", exact=False).first,
        ]
        csv_locators = [
            page.get_by_role("menuitem", name=re.compile(r"export\s*csv|csv", re.I)).first,
            page.get_by_role("button", name=re.compile(r"export\s*csv|csv", re.I)).first,
            page.get_by_role("link", name=re.compile(r"export\s*csv|csv", re.I)).first,
            page.get_by_text("EXPORT CSV", exact=False).first,
            page.get_by_text("Export CSV", exact=False).first,
            page.get_by_text(".csv", exact=False).first,
        ]

        for trigger in trigger_locators:
            if not self._locator_ready(trigger):
                continue

            download = self._try_download_click(page, trigger)
            if download is not None:
                return download

            try:
                self._click_locator(trigger)
                page.wait_for_timeout(750)
            except Exception:
                continue

            for csv_locator in csv_locators:
                download = self._try_download_click(page, csv_locator)
                if download is not None:
                    return download

        raise BrowserSessionError("Could not trigger CSV export from OVE")

    def _try_download_click(self, page: Page, locator: Locator) -> Download | None:
        if not self._locator_ready(locator):
            return None
        try:
            with page.expect_download(timeout=60000) as download_info:
                self._click_locator(locator)
            return download_info.value
        except PlaywrightTimeoutError:
            return None
        except Exception:
            return None

    def _click_locator(self, locator: Locator) -> None:
        locator.scroll_into_view_if_needed(timeout=3000)
        try:
            locator.click(timeout=10000)
        except Exception:
            locator.click(timeout=10000, force=True)

    def _locator_ready(self, locator: Locator) -> bool:
        try:
            return locator.count() > 0 and locator.is_visible(timeout=1000) and locator.is_enabled(timeout=1000)
        except Exception:
            return False

    def _persist_download(self, download: Download, target_path: Path, search_name: str) -> None:
        failure = download.failure()
        if failure:
            raise BrowserSessionError(
                f"OVE export download failed for '{search_name}'"
                f" (suggested filename: {download.suggested_filename}): {failure}"
            )
        try:
            download.save_as(str(target_path))
        except Exception as exc:
            raise BrowserSessionError(
                f"Could not save exported CSV for '{search_name}'"
                f" (suggested filename: {download.suggested_filename})"
            ) from exc
        if target_path.exists() and target_path.stat().st_size > 0:
            return
        raise BrowserSessionError(
            f"Exported CSV for '{search_name}' was empty"
            f" (suggested filename: {download.suggested_filename})"
        )

    def _remove_file_if_present(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    def _open_listing_for_vin(self, page: Page, vin: str, artifact_dir: Path) -> tuple[Page, dict[str, str] | None]:
        page = self._prepare_vin_search_page(page)
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                if attempt:
                    page = self._prepare_vin_search_page(page)
                LOGGER.info("Submitting OVE VIN search for %s on attempt %s", vin, attempt + 1)
                self._submit_vin_search(page, vin)
                listing = self._wait_for_listing_for_vin(page, vin)
                result_card_report_link = None
                if listing is not None:
                    result_card_report_link = self._extract_result_card_report_link(page, vin)
                    detail_href = self._find_detail_href_for_vin(page, vin)
                    if detail_href:
                        LOGGER.info("Found direct OVE detail href for VIN %s: %s", vin, detail_href)
                        page.goto(detail_href, wait_until="domcontentloaded", timeout=30000)
                    else:
                        LOGGER.info("Result card detected for VIN %s; clicking listing card", vin)
                        listing.click(timeout=10000)
                    if not self._wait_for_detail_page(page, vin, timeout_seconds=10):
                        if detail_href:
                            LOGGER.warning(
                                "Direct detail navigation did not hydrate for VIN %s; retrying href %s",
                                vin,
                                detail_href,
                            )
                            page.goto(detail_href, wait_until="domcontentloaded", timeout=30000)
                        if not self._wait_for_detail_page(page, vin, timeout_seconds=10):
                            raise BrowserSessionError(f"Detail page never opened after selecting VIN {vin}")
                else:
                    LOGGER.info("OVE search for VIN %s navigated directly to the detail page", vin)
                    if not self._wait_for_detail_page(page, vin, timeout_seconds=10):
                        raise BrowserSessionError(f"OVE navigated away without opening the VIN {vin} detail page")
                LOGGER.info("OVE detail page opened successfully for VIN %s at %s", vin, page.url)
                return page, result_card_report_link
            except (PlaywrightTimeoutError, ListingNotFoundError, BrowserSessionError) as exc:
                last_error = exc
                self._capture_debug_state(page, artifact_dir, f"open-listing-attempt-{attempt + 1}", vin, exc)
                if attempt == 0:
                    try:
                        page.goto(self._vin_results_url(), wait_until="domcontentloaded")
                        page.wait_for_timeout(1500)
                    except Exception:
                        pass
                    continue
                if isinstance(exc, ListingNotFoundError):
                    raise
                raise BrowserSessionError(f"Unable to open detail page for VIN {vin}") from exc
        if isinstance(last_error, ListingNotFoundError):
            raise last_error
        raise BrowserSessionError(f"Unable to open detail page for VIN {vin}")

    def _submit_vin_search(self, page: Page, vin: str) -> None:
        if self._is_login_page(page):
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
        self._clear_active_search_filters(page)
        search = self._find_vin_search_input(page)
        search.click(timeout=10000)
        if self._is_login_page(page):
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
        self._populate_vin_search_input(page, search, vin)
        if not self._trigger_vin_search(page, search, vin):
            raise BrowserSessionError(f"Unable to submit the OVE VIN search for {vin}")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(1000)

    def _trigger_vin_search(self, page: Page, search: Locator, vin: str) -> bool:
        start_url = page.url
        adjacent_strategy = self._click_search_trigger_near_input(search)
        if adjacent_strategy:
            LOGGER.info("Triggered OVE VIN search for %s using %s", vin, adjacent_strategy)
            if self._wait_for_search_submission(page, vin, start_url):
                return True

        trigger_locators = [
            page.locator("[data-test-id='search-button']").first,
            page.get_by_role("button", name=re.compile(r"search", re.I)).first,
            page.locator(".KeywordSearch__search").first,
        ]
        for locator in trigger_locators:
            try:
                if locator.count() and locator.is_visible(timeout=1000) and locator.is_enabled(timeout=1000):
                    self._click_locator(locator)
                    LOGGER.info("Triggered OVE VIN search for %s using locator %s", vin, locator)
                    if self._wait_for_search_submission(page, vin, start_url):
                        return True
            except Exception:
                continue
        try:
            clicked = page.evaluate(
                """
                () => {
                    const selectors = [
                        "[data-test-id='search-button']",
                        ".KeywordSearch__search",
                    ];
                    for (const selector of selectors) {
                        const node = document.querySelector(selector);
                        if (!(node instanceof HTMLElement)) continue;
                        node.click();
                        return true;
                    }
                    const candidates = Array.from(document.querySelectorAll("button, a, [role='button']"));
                    const trigger = candidates.find((node) =>
                        (node.textContent || "").replace(/\\s+/g, " ").trim().toLowerCase().includes("search")
                    );
                    if (!(trigger instanceof HTMLElement)) return false;
                    trigger.click();
                    return true;
                }
                """
            )
            if clicked and self._wait_for_search_submission(page, vin, start_url):
                LOGGER.info("Triggered OVE VIN search for %s using DOM fallback search-button click", vin)
                return True
        except Exception:
            pass
        try:
            search.press("Enter")
            LOGGER.info("Triggered OVE VIN search for %s using Enter key fallback", vin)
            return self._wait_for_search_submission(page, vin, start_url)
        except Exception:
            return False

    def _clear_active_search_filters(self, page: Page) -> None:
        if not self._search_page_has_active_filters(page):
            return

        toggle_locators = [
            page.locator("[data-test-id='advanced-filter-button']").first,
            page.get_by_role("button", name=re.compile(r"filters", re.I)).first,
            page.locator("#filter-button").first,
        ]
        for locator in toggle_locators:
            try:
                if locator.count() and locator.is_visible(timeout=1000):
                    locator.click(timeout=5000)
                    page.wait_for_timeout(500)
                    break
            except Exception:
                continue

        clear_locators = [
            page.locator("[data-test-id='clear-all']").first,
            page.get_by_text("Clear All", exact=False).first,
        ]
        for locator in clear_locators:
            try:
                if locator.count() and locator.is_visible(timeout=1500):
                    locator.click(timeout=5000)
                    page.wait_for_timeout(1500)
                    self._close_filter_panel(page)
                    return
            except Exception:
                continue

        # Fall back to clicking the element directly if the slideout was rendered but Playwright
        # could not interact with the control due to overlay timing.
        try:
            cleared = page.evaluate(
                """
                () => {
                    const clear = document.querySelector("[data-test-id='clear-all']");
                    if (!(clear instanceof HTMLElement)) return false;
                    clear.click();
                    return true;
                }
                """
            )
            if cleared:
                page.wait_for_timeout(1500)
                self._close_filter_panel(page)
        except Exception:
            pass

        if self._search_page_has_active_filters(page):
            try:
                page.goto(self._vin_results_url(), wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1500)
            except Exception:
                pass

    def _search_page_has_active_filters(self, page: Page) -> bool:
        try:
            return bool(
                page.evaluate(
                    """
                    () => {
                        const bodyText = document.body?.innerText || '';
                        const chip = document.querySelector('.filter-count-chip');
                        const chipText = chip?.textContent?.trim() || '';
                        const topButton = document.querySelector("[data-test-id='advanced-filter-button']");
                        const topButtonText = topButton?.textContent?.trim() || '';
                        const breadcrumbCount = document.querySelectorAll("[data-test-id='breadcrumb-container']").length;

                        const countMatch = (topButtonText || bodyText).match(/Filters\\s*\\((\\d+)\\)/i);
                        const count = countMatch ? Number.parseInt(countMatch[1], 10) : 0;
                        const chipCount = Number.parseInt(chipText || '0', 10);
                        return count > 0 || chipCount > 0 || breadcrumbCount > 1;
                    }
                    """
                )
            )
        except Exception:
            return False

    def _close_filter_panel(self, page: Page) -> None:
        close_locators = [
            page.locator("#close_primary_button").first,
            page.locator("[data-testid='ids-slideout-close-button']").first,
            page.get_by_role("button", name="Close").first,
        ]
        for locator in close_locators:
            try:
                if locator.count() and locator.is_visible(timeout=1000):
                    locator.click(timeout=3000)
                    page.wait_for_timeout(500)
                    return
            except Exception:
                continue

    def _wait_for_listing_for_vin(self, page: Page, vin: str):
        deadline = time.monotonic() + 15
        saw_no_results = False
        while time.monotonic() < deadline:
            if self._page_looks_like_detail_for_vin(page, vin):
                LOGGER.info("OVE search for VIN %s appears to have navigated directly to detail page", vin)
                return None
            locator = self._lookup_listing_for_vin(page, vin)
            if locator is not None:
                LOGGER.info("OVE search for VIN %s produced a result locator", vin)
                return locator
            if self._page_has_no_results(page):
                saw_no_results = True
            page.wait_for_timeout(500)
        if self._page_looks_like_detail_for_vin(page, vin):
            LOGGER.info("OVE search for VIN %s eventually resolved to detail page after wait loop", vin)
            return None
        if saw_no_results:
            LOGGER.warning("OVE search for VIN %s returned a no-results state", vin)
            raise ListingNotFoundError(f"VIN {vin} is not available in OVE search results")
        raise BrowserSessionError(f"Timed out waiting for VIN {vin} search results")

    def _prepare_vin_search_page(self, page: Page) -> Page:
        if self._is_login_page(page):
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")

        candidate_urls = [
            self._vin_results_url(),
            f"{self.settings.ove_base_url}/buy#/",
            self.settings.ove_listings_url,
            self.settings.ove_base_url,
        ]
        tried: set[str] = set()
        for url in candidate_urls:
            if not url or url in tried:
                continue
            tried.add(url)
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            if self._is_login_page(page):
                raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            try:
                self._find_vin_search_input(page)
                if url == self._vin_results_url():
                    self._clear_active_search_filters(page)
                LOGGER.info("Prepared OVE VIN search page at %s", page.url)
                return page
            except BrowserSessionError:
                continue
        raise BrowserSessionError("Unable to locate the OVE VIN search input")

    def _wait_for_search_submission(self, page: Page, vin: str, start_url: str, timeout_seconds: int = 8) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self._is_login_page(page):
                raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            if page.url != start_url:
                LOGGER.info("OVE VIN search for %s changed URL to %s", vin, page.url)
                return True
            if self._page_looks_like_detail_for_vin(page, vin):
                LOGGER.info("OVE VIN search for %s reached the detail page during submit wait", vin)
                return True
            if self._lookup_listing_for_vin(page, vin) is not None:
                LOGGER.info("OVE VIN search for %s rendered a visible result during submit wait", vin)
                return True
            if self._page_has_no_results(page):
                LOGGER.info("OVE VIN search for %s rendered a no-results state during submit wait", vin)
                return True
            page.wait_for_timeout(300)
        return False

    def _wait_for_detail_page(self, page: Page, vin: str, timeout_seconds: int = 10) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self._page_looks_like_detail_for_vin(page, vin):
                return True
            try:
                page.wait_for_load_state("domcontentloaded", timeout=1000)
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(300)
        return self._page_looks_like_detail_for_vin(page, vin)

    def _capture_debug_state(
        self,
        page: Page,
        artifact_dir: Path,
        stem: str,
        vin: str,
        error: Exception,
    ) -> None:
        payload: dict[str, Any] = {
            "vin": vin,
            "error": str(error),
            "timestamp": int(time.time()),
        }
        try:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            html_path = artifact_dir / f"{stem}.html"
            screenshot_path = artifact_dir / f"{stem}.png"
            json_path = artifact_dir / f"{stem}.json"
            try:
                html_path.write_text(page.content(), encoding="utf-8")
                payload["html_ref"] = str(html_path)
            except Exception as html_exc:
                payload["html_capture_error"] = str(html_exc)
            try:
                page.screenshot(path=str(screenshot_path), full_page=True)
                payload["screenshot_ref"] = str(screenshot_path)
            except Exception as screenshot_exc:
                payload["screenshot_capture_error"] = str(screenshot_exc)
            try:
                payload["url"] = page.url
            except Exception as url_exc:
                payload["url_capture_error"] = str(url_exc)
            try:
                payload["title"] = page.title()
            except Exception as title_exc:
                payload["title_capture_error"] = str(title_exc)
            try:
                payload["body_text"] = page.locator("body").inner_text(timeout=3000)
            except Exception as body_exc:
                payload["body_text_capture_error"] = str(body_exc)
            json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            LOGGER.warning("Captured OVE deep-scrape debug state for VIN %s at %s", vin, json_path)
        except Exception:
            LOGGER.exception("Failed to capture OVE deep-scrape debug state for VIN %s", vin)

    def _click_search_trigger_near_input(self, search: Locator) -> str | None:
        try:
            result = search.evaluate(
                """
                (node) => {
                    const selectors = [
                        "[data-test-id='search-button']",
                        ".KeywordSearch__search",
                        "button[type='submit']",
                        "button",
                        "[role='button']",
                        "a[role='button']",
                    ];
                    const containers = [
                        node.closest("[data-test-id*='search']"),
                        node.closest("[class*='KeywordSearch']"),
                        node.closest("[class*='search']"),
                        node.closest("form"),
                        node.parentElement,
                        node.parentElement?.parentElement,
                        document,
                    ].filter(Boolean);

                    for (const container of containers) {
                        for (const selector of selectors) {
                            const candidate = container.querySelector(selector);
                            if (!(candidate instanceof HTMLElement) || candidate === node) continue;
                            candidate.click();
                            return `adjacent-dom:${selector}`;
                        }
                    }
                    return null;
                }
                """
            )
        except Exception:
            return None
        return str(result).strip() or None

    def _find_vin_search_input(self, page: Page):
        if self._is_login_page(page):
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
        locators = [
            page.get_by_placeholder("VIN or Make Search").first,
            page.locator("input[placeholder*='VIN' i]").first,
            page.locator("input[aria-label*='VIN' i]").first,
            page.locator("input[name*='vin' i]").first,
            page.locator("input[id*='vin' i]").first,
            page.locator("input[placeholder*='VIN']").first,
            page.locator("input[placeholder*='Make']").first,
            page.locator("input[name*='search']").first,
            page.locator("input[type='search']").first,
            page.locator("input[type='text']").first,
        ]
        for locator in locators:
            try:
                if (
                    locator.count()
                    and locator.is_visible(timeout=1000)
                    and locator.is_enabled(timeout=1000)
                    and not self._locator_looks_like_login_input(locator)
                ):
                    return locator
            except Exception:
                continue
        raise BrowserSessionError("Unable to locate the OVE VIN search input")

    def _populate_vin_search_input(self, page: Page, search: Locator, vin: str) -> None:
        for strategy in (
            lambda: self._fill_locator(search, vin),
            lambda: self._type_locator(page, search, vin, delay=35),
            lambda: self._set_locator_value_via_dom(search, vin),
        ):
            try:
                strategy()
            except Exception:
                continue
            if self._locator_value_matches(search, vin):
                return
        raise BrowserSessionError(f"Unable to populate the OVE VIN search input for {vin}")

    def _fill_locator(self, search: Locator, vin: str) -> None:
        search.fill("")
        search.fill(vin)

    def _type_locator(self, page: Page, search: Locator, vin: str, *, delay: int) -> None:
        search.click(timeout=10000)
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.type(vin, delay=delay)

    def _set_locator_value_via_dom(self, search: Locator, vin: str) -> None:
        search.evaluate(
            """(node, value) => {
                node.focus();
                node.value = '';
                node.dispatchEvent(new Event('input', { bubbles: true }));
                node.value = value;
                node.dispatchEvent(new Event('input', { bubbles: true }));
                node.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            vin,
        )

    def _locator_value_matches(self, search: Locator, vin: str) -> bool:
        try:
            return search.input_value(timeout=1000).strip().upper() == vin.strip().upper()
        except Exception:
            return False

    def _find_listing_for_vin(self, page: Page, vin: str):
        locator = self._lookup_listing_for_vin(page, vin)
        if locator is not None:
            return locator
        raise ListingNotFoundError(f"VIN {vin} is not available in OVE search results")

    def _lookup_listing_for_vin(self, page: Page, vin: str):
        detail_href = self._find_detail_href_for_vin(page, vin)
        if detail_href:
            detail_locators = [
                page.locator(f"a[href='{detail_href}']").first,
                page.locator(f"a[href*='/details/{vin}/OVE']").first,
                page.locator(f"a[href*='#/details/{vin}/OVE']").first,
                page.locator(f"a[href*='/details/{vin}/']").first,
                page.locator(f"a[href*='#/details/{vin}/']").first,
            ]
            for locator in detail_locators:
                try:
                    if locator.count() and locator.is_visible(timeout=3000):
                        return locator
                except PlaywrightTimeoutError:
                    continue

        vin_text = page.get_by_text(vin, exact=False).first
        try:
            if vin_text.count() and vin_text.is_visible(timeout=5000):
                return vin_text
        except PlaywrightTimeoutError:
            pass

        locators = [
            page.locator(f"a[href*='{vin}']").first,
            page.locator(f"[href*='{vin}']").first,
            page.locator(f"a[href*='/details/{vin}/OVE']").first,
            page.locator(f"a[href*='#/details/{vin}/OVE']").first,
            page.locator(f"a[href*='/details/{vin}/']").first,
            page.locator(f"a[href*='#/details/{vin}/']").first,
            page.locator(self.settings.ove_result_link_selector).filter(has_text=vin).first,
            page.locator(f"a:has-text('{vin}')").first,
            page.locator(f"[data-test-id*='{vin}']").first,
            page.locator(f"text={vin}").first,
            page.locator(self.settings.ove_result_link_selector).first,
        ]
        for locator in locators:
            try:
                if locator.count() and locator.is_visible(timeout=5000):
                    return locator
            except PlaywrightTimeoutError:
                continue
        return None

    def _find_detail_href_for_vin(self, page: Page, vin: str) -> str | None:
        try:
            href = page.evaluate(
                """(needle) => {
                    const anchors = Array.from(document.querySelectorAll('a[href]'));
                    const match = anchors.find((anchor) => {
                        const href = anchor.href || anchor.getAttribute('href') || '';
                        return href.includes(`/details/${needle}/`);
                    });
                    return match ? match.href : null;
                }""",
                vin,
            )
        except Exception:
            return None
        return str(href).strip() or None

    def _page_looks_like_detail_for_vin(self, page: Page, vin: str) -> bool:
        url = (page.url or "").lower()
        vin_lower = vin.lower()
        if vin_lower in url and "/buy#/" not in url and "/saved_searches#/" not in url:
            return True
        try:
            return bool(
                page.evaluate(
                    """(needle) => {
                        const text = (document.body?.innerText || '').toLowerCase();
                        if (!text.includes(needle.toLowerCase())) return false;
                        const href = location.href.toLowerCase();
                        if (href.includes('/buy#/') || href.includes('/saved_searches#/')) return false;
                        return Boolean(
                            document.querySelector("h1, h2, [class*='vehicle-title'], [class*='listing-title']") ||
                            document.querySelector("[data-test-id*='condition'], [class*='condition-report'], [class*='vehicle-detail']")
                        );
                    }""",
                    vin,
                )
            )
        except Exception:
            return False

    def _page_has_no_results(self, page: Page) -> bool:
        markers = (
            "Your search did not match any vehicles.",
            "0 results",
            "No vehicles found",
        )
        try:
            body_text = page.locator("body").inner_text(timeout=1000)
        except Exception:
            return False
        lowered = body_text.lower()
        return any(marker.lower() in lowered for marker in markers)

    def _extract_result_card_report_link(self, page: Page, vin: str) -> dict[str, str] | None:
        candidate = page.evaluate(
            f"""
            () => {{
              const vin = {json.dumps(vin)};
              const normalizeHref = (value) => {{
                if (!value) return null;
                try {{
                  return new URL(value, window.location.href).href;
                }} catch (_err) {{
                  return value;
                }}
              }};

              const candidates = Array.from(
                document.querySelectorAll(
                  "a[data-test-id='condition-report'], a.VehicleReportLink__condition-report-link[data-label-text='CR'], a.CardView__condition-report-link, a[href*='inspectionreport.manheim.com'], a[href*='insightcr.manheim.com'], a[href*='content.liquidmotors.com/IR/'], a[href*='ECR2I.htm']"
                )
              );

              const scored = candidates.map((node) => {{
                const href = normalizeHref(node.getAttribute("href"));
                const text = (node.textContent || "").trim() || null;
                const title = node.getAttribute("title") || node.getAttribute("aria-label") || null;
                const valueText = node.getAttribute("data-value-text") || null;
                const labelText = node.getAttribute("data-label-text") || null;
                const card = node.closest("article, li, section, [class*='card'], [class*='result']");
                const cardText = (card?.textContent || "").trim();
                let score = 0;
                if (cardText.includes(vin)) score += 10;
                if ((href || "").includes(vin)) score += 8;
                if ((href || "").toLowerCase().includes("ecr")) score += 8;
                if ((href || "").includes("inspectionreport.manheim.com")) score += 20;
                if ((href || "").includes("insightcr.manheim.com")) score += 20;
                if ((href || "").includes("content.liquidmotors.com/IR/")) score += 20;
                if ((href || "").includes("mmsc400.manheim.com/MABEL/ECR2I.htm")) score += 20;
                if ((href || "").includes("order_condition_reports")) score -= 25;
                if ((text || "").includes("CR")) score += 4;
                if (labelText === "CR") score += 10;
                if (valueText) score += 4;
                return {{ href, text, title, labelText, valueText, score }};
              }}).sort((a, b) => b.score - a.score);

              return scored[0] || null;
            }}
            """
        )
        return self._normalize_report_link_candidate(candidate)

    def _is_login_page(self, page: Page) -> bool:
        url = (page.url or "").lower()
        if any(hint in url for hint in AUTH_PATH_HINTS):
            return True
        try:
            indicators = page.evaluate(
                """
                () => {
                  const text = `${document.title || ""} ${document.body?.innerText || ""}`
                    .replace(/\\s+/g, " ")
                    .trim()
                    .toLowerCase();
                  const controls = Array.from(document.querySelectorAll("input, button, a, label"));
                  const controlText = controls
                    .map((node) =>
                      (
                        node.getAttribute("aria-label") ||
                        node.getAttribute("placeholder") ||
                        node.getAttribute("name") ||
                        node.getAttribute("id") ||
                        node.getAttribute("value") ||
                        node.textContent ||
                        ""
                      )
                        .replace(/\\s+/g, " ")
                        .trim()
                        .toLowerCase()
                    )
                    .filter(Boolean)
                    .join(" ");
                  return {
                    hasPasswordField: !!document.querySelector("input[type='password']"),
                    combinedText: `${text} ${controlText}`,
                  };
                }
                """
            )
        except Exception:
            return False

        combined_text = str(indicators.get("combinedText") or "")
        if indicators.get("hasPasswordField"):
            return True
        return any(hint in combined_text for hint in LOGIN_COPY_HINTS)

    def _locator_looks_like_login_input(self, locator: Locator) -> bool:
        try:
            attrs = " ".join(
                filter(
                    None,
                    [
                        locator.get_attribute("type"),
                        locator.get_attribute("name"),
                        locator.get_attribute("id"),
                        locator.get_attribute("placeholder"),
                        locator.get_attribute("autocomplete"),
                        locator.get_attribute("aria-label"),
                    ],
                )
            ).lower()
        except Exception:
            return False

        if "password" in attrs:
            return True
        if any(hint in attrs for hint in VIN_SEARCH_FIELD_HINTS):
            return False
        return any(hint in attrs for hint in LOGIN_FIELD_HINTS)

    def _is_error_page(self, page: Page) -> bool:
        try:
            title = (page.title() or "").strip().lower()
        except Exception:
            return False
        return title == "server error"

    def _saved_searches_url(self) -> str:
        return f"{self.settings.ove_base_url}/saved_searches#/"

    def _vin_results_url(self) -> str:
        return f"{self.settings.ove_base_url}/search/results#/results"

    def _capture_condition_report_page(
        self,
        browser: Browser,
        source_page: Page,
        report_link: dict[str, str] | None,
        artifact_dir: Path,
    ) -> dict[str, Any] | None:
        href = str((report_link or {}).get("href") or "").strip()
        if not href:
            return None

        owns_context = not browser.contexts
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page: Page | None = None
        try:
            page = self._open_condition_report_page(source_page, context, href)
            if page is None:
                return None
            return self._snapshot_page(page, artifact_dir / "condition-report")
        except Exception:
            return None
        finally:
            if page is not None and page is not source_page:
                self._close_page(page)
            if owns_context:
                try:
                    context.close()
                except Exception:
                    pass

    def _open_condition_report_page(self, source_page: Page, context: BrowserContext, href: str) -> Page | None:
        locator = self._find_condition_report_locator(source_page)
        if locator is not None:
            original_url = source_page.url
            popup_page = self._click_condition_report_locator(source_page, locator)
            if popup_page is not None:
                LOGGER.info("Opened condition report in popup from OVE detail page")
                return popup_page
            if source_page.url != original_url or "auth.manheim.com" in (source_page.url or "").lower():
                LOGGER.info("Condition report click navigated the OVE detail page directly to %s", source_page.url)
                return source_page

        LOGGER.info("Falling back to direct condition report navigation for %s", href)
        page = context.new_page()
        page.goto(href, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)
        return page

    def _find_condition_report_locator(self, page: Page) -> Locator | None:
        locators = [
            page.locator("[data-test-id='condition-report']").first,
            page.locator("a.VehicleReportLink__condition-report-link").first,
            page.locator("a[href*='insightcr.manheim.com']").first,
            page.locator("a[href*='inspectionreport.manheim.com']").first,
        ]
        for locator in locators:
            try:
                if locator.count() and locator.is_visible(timeout=1500):
                    return locator
            except Exception:
                continue
        return None

    def _click_condition_report_locator(self, page: Page, locator: Locator) -> Page | None:
        try:
            with page.expect_popup(timeout=5000) as popup_info:
                self._click_locator(locator)
            popup = popup_info.value
            popup.wait_for_load_state("domcontentloaded", timeout=60000)
            popup.wait_for_timeout(2500)
            return popup
        except PlaywrightTimeoutError:
            pass
        except Exception:
            pass

        try:
            self._click_locator(locator)
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)
        except Exception:
            return None
        return None

    def _snapshot_page(self, page: Page, base_path: Path) -> dict[str, Any]:
        html_path = base_path.with_suffix(".html")
        screenshot_path = base_path.with_suffix(".png")
        page_html = page.content()
        html_path.write_text(page_html, encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)

        payload = page.evaluate(
            """
            () => ({
              title: document.title,
              url: location.href,
              body_text: document.body?.innerText || "",
            })
            """
        )
        payload["html_ref"] = str(html_path)
        payload["screenshot_ref"] = str(screenshot_path)
        payload["images"] = self._extract_page_image_urls(page, page_html)
        return payload

    def _extract_page_image_urls(self, page: Page, page_html: str | None = None) -> list[str]:
        image_urls: list[str] = []
        try:
            dom_images = page.evaluate(
                """
                () => Array.from(document.querySelectorAll("img"))
                  .map((img) => img.currentSrc || img.src || "")
                  .filter((src) => /^https?:/i.test(src))
                """
            )
            if isinstance(dom_images, list):
                image_urls.extend(str(value) for value in dom_images if isinstance(value, str))
        except Exception:
            pass

        try:
            stockwave_blobs = page.evaluate(
                """
                () => Array.from(
                  document.querySelectorAll("[data-test-id='stockwave-info'], .stockwave-vehicle-info")
                )
                  .map((node) => node.textContent || "")
                  .filter((text) => text && text.trim().startsWith("{"))
                """
            )
            if isinstance(stockwave_blobs, list):
                for raw_blob in stockwave_blobs:
                    if isinstance(raw_blob, str):
                        image_urls.extend(_extract_stockwave_image_urls(raw_blob))
        except Exception:
            pass

        if len(unique_urls(image_urls)) < 2 and page_html:
            image_urls.extend(_extract_image_urls_from_html(page_html))

        return unique_urls(image_urls)

    def _select_valid_condition_report_link(self, *candidates: object) -> dict[str, str] | None:
        for candidate in candidates:
            normalized = self._normalize_report_link_candidate(candidate)
            if normalized is not None:
                return normalized
        return None

    def _normalize_report_link_candidate(self, candidate: object) -> dict[str, str] | None:
        if not isinstance(candidate, dict):
            return None
        href = str(candidate.get("href") or "").strip()
        if not href:
            return None
        descriptor = identify_report_family(href)
        if descriptor is None:
            return None
        return {
            "href": href,
            "text": str(candidate.get("text") or "").strip() or None,
            "title": str(candidate.get("title") or "").strip() or None,
            "labelText": str(candidate.get("labelText") or "").strip() or None,
            "valueText": str(candidate.get("valueText") or "").strip() or None,
            "score": int(candidate.get("score") or 0),
        }


def clean_strings(values: list[str]) -> list[str]:
    return [value.strip() for value in values if isinstance(value, str) and value.strip()]


def clean_fact_items(raw_facts: list[dict[str, str]]) -> list[dict[str, str | None]]:
    facts: list[dict[str, str | None]] = []
    for item in raw_facts:
        label = str(item.get("label", "")).strip()
        value = str(item.get("value", "")).strip()
        if label and value:
            facts.append({"label": label, "value": value, "icon_label": item.get("icon_label")})
    return facts


def clean_section_items(raw_sections: list[dict[str, object]]) -> list[dict[str, object]]:
    sections: list[dict[str, object]] = []
    for item in raw_sections:
        title = str(item.get("title", "Section")).strip() or "Section"
        facts = clean_fact_items(item.get("facts", []))
        paragraphs = clean_strings(item.get("paragraphs", []))
        text_items = [{"kind": "paragraph", "text": value} for value in paragraphs]
        sections.append(
            {
                "id": slugify(title),
                "title": title,
                "subtitle": None,
                "layout": "facts-and-text",
                "items": facts + text_items,
                "metadata": {},
            }
        )
    return sections


def clean_icon_items(raw_icons: list[dict[str, str]]) -> list[dict[str, str | None]]:
    icons: list[dict[str, str | None]] = []
    for item in raw_icons:
        kind = str(item.get("kind", "unknown")).strip() or "unknown"
        label = item.get("label")
        source_url = item.get("source_url")
        svg_markup = item.get("svg_markup")
        icons.append({"kind": kind, "label": label, "source_url": source_url, "svg_markup": svg_markup})
    return icons


def build_condition_report(snapshot: ListingSnapshot, report_link: dict[str, str] | None = None) -> ConditionReport | None:
    section_facts = []
    for section in snapshot.sections:
        for item in section.get("items", []):
            label = item.get("label")
            value = item.get("value")
            if label and value:
                section_facts.append({"label": label, "value": value})

    all_facts = snapshot.hero_facts + section_facts
    if not all_facts:
        if not report_link:
            return None
        return ConditionReport(metadata={"report_link": report_link})

    fact_map = {normalize_label(fact["label"]): str(fact["value"]) for fact in all_facts}
    announcements = []
    for section in snapshot.sections:
        if "announcement" in normalize_label(str(section.get("title", ""))):
            for item in section.get("items", []):
                if "text" in item:
                    announcements.append(str(item["text"]))
                elif "value" in item:
                    announcements.append(str(item["value"]))

    return ConditionReport(
        overall_grade=fact_map.get("grade") or fact_map.get("condition"),
        structural_damage=parse_bool(fact_map.get("structural damage")),
        paint_condition=fact_map.get("paint condition"),
        interior_condition=fact_map.get("interior condition"),
        tire_condition=fact_map.get("tire condition"),
        announcements=clean_strings(announcements),
        raw_text="\n".join(
            f'{fact["label"]}: {fact["value"]}' for fact in all_facts[:50]
        ),
        metadata={"report_link": report_link} if report_link else {},
    )


def normalize_label(value: str) -> str:
    return re.sub(r"\\s+", " ", value.strip().lower())


def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"yes", "true", "present"}:
        return True
    if lowered in {"no", "false", "none"}:
        return False
    return None


def unique_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        if not isinstance(url, str):
            continue
        value = _canonicalize_image_url(url)
        if not value or value in seen or not value.startswith(("http://", "https://")):
            continue
        if not _is_vehicle_image_url(value):
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _canonicalize_image_url(url: str) -> str:
    value = url.strip()
    if not value:
        return ""
    return re.sub(r"\?size=[^#]*$", "", value, flags=re.IGNORECASE)


def _is_vehicle_image_url(url: str) -> bool:
    lowered = url.strip().lower()
    if not lowered:
        return False

    blocked_signals = (
        "strike-assets.manheim.com",
        "/assets/ove/header/",
        "/build/images/",
        "logo",
        "icon",
        "sprite",
        "autocheck",
        "greencheck",
        "header",
        ".svg",
        ".gif",
    )
    if any(signal in lowered for signal in blocked_signals):
        return False

    base_url = lowered.split("?", 1)[0]
    return base_url.endswith((".jpg", ".jpeg", ".png", ".webp"))


def _extract_stockwave_image_urls(raw_json: str) -> list[str]:
    try:
        payload = json.loads(raw_json)
    except Exception:
        return []
    return _collect_image_urls_from_value(payload)


def _extract_image_urls_from_html(page_html: str) -> list[str]:
    return unique_urls(
        re.findall(
            r"https://images\.cdn\.manheim\.com/[^\"'\s>)]+?\.(?:jpg|jpeg|png|webp)(?:\?[^\"'\s>)]+)?",
            page_html,
            flags=re.IGNORECASE,
        )
    )


def _collect_image_urls_from_value(value: object) -> list[str]:
    image_urls: list[str] = []

    if isinstance(value, dict):
        for child in value.values():
            image_urls.extend(_collect_image_urls_from_value(child))
        return unique_urls(image_urls)

    if isinstance(value, list):
        for child in value:
            image_urls.extend(_collect_image_urls_from_value(child))
        return unique_urls(image_urls)

    if isinstance(value, str):
        candidate = _canonicalize_image_url(value)
        if candidate.startswith(("http://", "https://")) and _is_vehicle_image_url(candidate):
            return [candidate]

    return []


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")

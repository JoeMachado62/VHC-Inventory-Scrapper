from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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

from ove_scraper.browser import (
    BrowserSessionError,
    ConditionReportClickFailedError,
    DeepScrapeResult,
    ListingNotFoundError,
    ManheimAuthRedirectError,
    SavedSearchPageEmpty,
)
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
      // Capture additional attributes that the scoring loop uses to
      // distinguish the real CR badge from marketing footer links. The
      // real OVE CR badge typically has empty visible text but carries
      // data-test-id="condition-report", data-label-text="CR", and a
      // data-value-text="<grade>". Marketing footer links have visible
      // text like "Condition Reporting" but no test-id and no label-text.
      const labelText = node.getAttribute?.("data-label-text") || null;
      const valueText = node.getAttribute?.("data-value-text") || null;
      const testId = node.getAttribute?.("data-test-id") || null;
      const className = (node.className?.toString && node.className.toString()) || null;
      if (!href && !text) return;
      candidates.push({ href, text, title, labelText, valueText, testId, className, reason });
    };

    // Scan the WHOLE document, not just `root`. The OVE webapp does not
    // render a <main> element on every detail variant; the configured root
    // selector falls back to [role='main'] which on some VINs (verified
    // 2026-04-09 against 1N4BL4EV2NN423240) does NOT contain the CR link
    // element — the CR link lives in a sibling div outside the main role
    // container. Restricting candidate collection to `root` was causing
    // findConditionReportLink to return null even when the CR link was
    // clearly present in the DOM (the wait_for_selector check that runs
    // BEFORE this script does scan the whole document and was finding it).
    // CR link elements are unique enough on the page that the scoring
    // logic below filters out false positives without needing the root
    // restriction.
    document.querySelectorAll("a[data-test-id='condition-report'], a.VehicleReportLink__condition-report-link, a, button, [role='button']").forEach((node) => {
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

    // Score-pattern walk: only do this within `root` because the
    // unrestricted whole-document version is too noisy (lots of "4.6"-style
    // numbers in unrelated parts of the page). The score-pattern path is a
    // fallback anyway — vehicles with a normal CR link will already be
    // registered by the explicit-attribute pass above.
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
      // Build the signal from EVERY attribute we have, not just href+text+title.
      // The Nissan Altima 1N4BL4EV2NN423240 case revealed that the real CR
      // badge has empty <a> text content (the score chip is rendered via
      // ::before/::after CSS), so href+text+title-only signal scores it
      // at +20 (host bonus alone) while the marketing footer link
      // "Condition Reporting" scores +15 from text alone — the wrong
      // candidate wins. Including className/labelText/valueText in the
      // signal lifts the real badge well above the footer noise because
      // its className is "VehicleReportLink__condition-report-link".
      const signal = [
        candidate.href || "",
        candidate.text || "",
        candidate.title || "",
        candidate.labelText || "",
        candidate.valueText || "",
        candidate.className || "",
        candidate.testId || "",
      ].join(" ").toLowerCase();
      // The single most reliable signal that this is THE CR badge (not a
      // marketing footer link) is data-test-id="condition-report". Boost
      // it heavily so it always wins over signal-keyword matches on links
      // that just happen to mention "condition report" in their text.
      if (candidate.testId === "condition-report") score += 100;
      // data-label-text="CR" is the secondary high-confidence signal —
      // OVE only sets this attribute on the actual CR badge element.
      if (candidate.labelText === "CR") score += 50;
      // The "VehicleReportLink__condition-report-link" class name is
      // OVE-specific and only used on the real CR badge.
      if ((candidate.className || "").includes("VehicleReportLink__condition-report-link")) score += 50;
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
      // Negative signals: marketing pages and order-flow URLs are NOT
      // CR data sources. Penalize heavily so they can never outrank a
      // real CR badge even if their text is keyword-rich.
      if ((candidate.href || "").includes("order_condition_reports")) score -= 25;
      if ((candidate.href || "").includes("site.manheim.com")) score -= 50;
      return { ...candidate, score };
    }).sort((a, b) => b.score - a.score);

    return { result: scored[0] || null, debug: { candidate_count: candidates.length, deduped_count: deduped.length, scored_top: scored.slice(0, 3) } };
  };

  // Diagnostic shape: findConditionReportLink now returns
  // { result: <link>|null, debug: { candidate_count, deduped_count, scored_top } }
  // so the Python side can log WHY the result is null when it shouldn't be.
  // We also defensively check for whether the data-test-id="condition-report"
  // anchor exists in the DOM at all at this exact moment, separately from
  // the candidate-collection scoring path. If it does exist but the scoring
  // returns null, the bug is in scoring; if it doesn't exist, the bug is
  // upstream (React unmount race or wrong page).
  const crLinkOutcome = findConditionReportLink();
  const directProbe = (() => {
    const direct = document.querySelector("a[data-test-id='condition-report']");
    if (!direct) return { exists: false };
    return {
      exists: true,
      href: direct.getAttribute("href") || null,
      label_text: direct.getAttribute("data-label-text") || null,
      value_text: direct.getAttribute("data-value-text") || null,
      class_name: direct.className || null,
      text_content: (direct.textContent || "").trim() || null,
    };
  })();

  return {
    title: pickText(["h1", "[class*='vehicle-title']", "[class*='listing-title']"]),
    subtitle: pickText(["h2", "[class*='subtitle']", "[class*='vehicle-subtitle']"]),
    badges: Array.from(root.querySelectorAll(".badge, [class*='badge'], [class*='chip']")).map((node) => node.textContent?.trim()).filter(Boolean),
    hero_facts: collectFactsFromNode(root).slice(0, 20),
    sections,
    icons,
    images,
    seller_comments: pickText(["[class*='seller-comment']", "[class*='comments']", "#seller-comments"]),
    condition_report_link: crLinkOutcome.result,
    condition_report_link_debug: crLinkOutcome.debug,
    condition_report_link_direct_probe: directProbe,
    page_url: window.location.href,
    body_text: root.innerText || ""
  };
}
"""


class PlaywrightCdpBrowserSession:
    # Process-wide single-shot flag for the auto-click login recovery.
    # Set to True after the FIRST auto-click attempt (success OR failure)
    # and never reset for the lifetime of this Python process. Rebuilding
    # the PlaywrightCdpBrowserSession instance (main.py's auth-fail
    # rebuild path) does NOT reset it because it's a class attribute.
    #
    # Guardrail reason: the prior autonomous login feature (commit
    # 1f1d8ee, reverted in eaf37cf 2026-04-21) retried the login in a
    # tight loop on failure and Manheim locked the OVE account. A
    # process-wide single-shot limits us to exactly one click per
    # scraper process — a restart is required to try again, which
    # bounds total click volume to the scheduled-task restart budget
    # (Count=3). Memory: feedback_ove_auto_login_account_lock.md.
    _auto_login_attempted_this_process: bool = False

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
                # Try the single-shot auto-click ONCE per process before
                # giving up. Only clicks if Chrome has pre-filled the
                # credentials; never types them ourselves.
                if self._try_single_shot_login_click(page):
                    # Recheck — a successful click lands us off the login
                    # page. Fall through to the is_error_page check below.
                    pass
                else:
                    raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            if self.settings.ove_base_url not in page.url or self._is_error_page(page):
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:
            self._browser = None  # drop stale browser handle, keep Playwright alive
            raise BrowserSessionError(f"OVE browser session unavailable: {exc}") from exc

    def _try_single_shot_login_click(self, page: Page) -> bool:
        """Click the OVE login 'Sign In' button ONCE if Chrome has
        pre-filled the credentials, then wait briefly to see if the
        click navigated us off the login page.

        Returns True if the page is no longer on the login screen after
        the click; False in every other case (not attempted because
        fields aren't pre-filled, process-wide flag already set, click
        failed, or we're still on login after waiting).

        Never clicks more than once per Python process. Never types
        credentials. Never retries. The prior revert (eaf37cf) was
        because the reverted implementation retried on failure and
        Manheim locked the OVE account — this version is single-shot by
        construction. See feedback_ove_auto_login_account_lock.md.
        """
        if type(self)._auto_login_attempted_this_process:
            LOGGER.info(
                "Skipping auto-login click: already attempted once in this process. "
                "A process restart is required to try again."
            )
            return False
        # Mark FIRST — before we even try — so that any exception during
        # the click still counts as a consumed attempt. This matches the
        # spirit of "single-shot" strictly: one chance to recover, not
        # one successful click.
        type(self)._auto_login_attempted_this_process = True

        try:
            filled = page.evaluate(
                """
                () => {
                    const pw = document.querySelector("input[type='password']");
                    if (!(pw instanceof HTMLInputElement)) return false;
                    return pw.value.length > 0;
                }
                """
            )
        except Exception as exc:
            LOGGER.warning("Auto-login pre-check (password-populated) failed: %s", exc)
            return False
        if not filled:
            LOGGER.warning(
                "Auto-login skipped: password field is empty — Chrome's saved "
                "credentials are not available. Operator must log in manually."
            )
            return False

        submit_locators = [
            page.locator("button[type='submit']").first,
            page.get_by_role("button", name=re.compile(r"^\s*(sign\s*in|log\s*in|login)\s*$", re.I)).first,
            page.locator("input[type='submit']").first,
        ]
        submit: Locator | None = None
        for candidate in submit_locators:
            try:
                if candidate.count() and candidate.is_visible(timeout=1000):
                    submit = candidate
                    break
            except Exception:
                continue
        if submit is None:
            LOGGER.warning("Auto-login skipped: could not locate a Sign In button on the login page")
            return False

        LOGGER.info("Auto-login: clicking Sign In (single-shot, credentials were pre-filled)")
        try:
            submit.click(timeout=5000)
        except Exception as exc:
            LOGGER.warning("Auto-login Sign In click raised: %s", exc)
            return False

        # Wait up to 10s for the URL to leave the login page. Using a
        # simple polling loop instead of expect_navigation because SPA
        # logins commonly flip the URL via history.replaceState without
        # firing a navigation event.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                if not self._is_login_page(page):
                    LOGGER.info(
                        "Auto-login succeeded: page left the login screen (url=%s)",
                        page.url,
                    )
                    return True
            except Exception:
                pass
            page.wait_for_timeout(250)
        LOGGER.warning(
            "Auto-login click fired but page is still on the login screen after 10s. "
            "Giving up — will NOT re-click. Operator must intervene."
        )
        return False

    def touch_session(self) -> None:
        try:
            browser = self._connect_browser()
            page = self._open_dedicated_ove_page(browser)
            try:
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
                if self._is_login_page(page):
                    # Single-shot auto-click recovery; fall through to
                    # raise if it didn't land us off login.
                    if not self._try_single_shot_login_click(page):
                        raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
            finally:
                self._close_page(page)
        except Exception as exc:
            self._browser = None
            raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc

    def list_saved_searches(self) -> tuple[str, ...]:
        browser = self._connect_browser()
        page = self._open_dedicated_ove_page(browser)
        try:
            page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
            try:
                self._wait_for_saved_search_cards(page, timeout_ms=15_000)
            except SavedSearchPageEmpty:
                LOGGER.warning("Initial load returned empty saved searches; attempting recovery")
                try:
                    self._recover_empty_saved_searches(page)
                except SavedSearchPageEmpty:
                    # Last resort: close this page, open fresh one
                    LOGGER.warning("In-page recovery failed; trying fresh browser page")
                    self._close_page(page)
                    page = self._open_dedicated_ove_page(browser)
                    page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
                    self._wait_for_saved_search_cards(page, timeout_ms=15_000)
                    # If still SavedSearchPageEmpty, let it propagate
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
        target_path = export_dir / f"{slugify(search_name)}.csv"
        last_error: Exception | None = None
        max_attempts = max(1, getattr(self.settings, "ove_export_max_attempts", 5))

        # Open the dedicated page with retry — the initial goto to
        # /saved_searches#/ can time out on transient OVE slowness.
        # OVE is notoriously slow at 9 AM ET (peak auction load); a single
        # 30s timeout is not enough during that window. 5 attempts with
        # increasing backoff gives OVE up to ~4 minutes to respond before
        # we give up. The hourly sync has plenty of wall-clock budget for
        # this.
        page: Page | None = None
        open_attempts = 5
        for open_attempt in range(open_attempts):
            try:
                page = self._open_dedicated_ove_page(browser)
                break
            except Exception as exc:
                LOGGER.warning(
                    "Failed to open dedicated OVE page (attempt %s/%s) for '%s': %s",
                    open_attempt + 1, open_attempts, search_name, exc,
                )
                last_error = exc
                # Backoff: 5s, 10s, 20s, 30s between attempts
                time.sleep(min(30, 5 * (2 ** open_attempt)))
        if page is None:
            raise BrowserSessionError(
                f"Could not open dedicated OVE page for '{search_name}' after {open_attempts} attempts: {last_error}"
            )

        try:
            for attempt in range(max_attempts):
                # Linear backoff between attempts: 0s, 3s, 6s, 9s, 12s.
                # Short enough that the hourly sync still completes inside
                # its window, long enough that an OVE backend hiccup or a
                # render race has time to settle.
                if attempt > 0:
                    backoff_seconds = min(15, attempt * 3)
                    LOGGER.info(
                        "Saved-search export retry %s/%s for '%s' in %ss after error: %s",
                        attempt + 1,
                        max_attempts,
                        search_name,
                        backoff_seconds,
                        last_error,
                    )
                    page.wait_for_timeout(backoff_seconds * 1000)
                try:
                    self._remove_file_if_present(target_path)
                    self._open_saved_search(page, search_name)
                    download = self._trigger_export(page)
                    self._persist_download(download, target_path, search_name)
                    return target_path
                except SavedSearchPageEmpty as exc:
                    # OVE returned "No Saved Searches" — don't waste time on
                    # the normal reset-and-retry, attempt escalating recovery.
                    last_error = exc
                    LOGGER.warning(
                        "OVE returned 'No Saved Searches' during export attempt %s/%s "
                        "for '%s'; attempting recovery before next retry",
                        attempt + 1, max_attempts, search_name,
                    )
                    try:
                        self._recover_empty_saved_searches(page)
                        continue  # recovery succeeded; skip normal reset nav
                    except SavedSearchPageEmpty:
                        LOGGER.warning(
                            "In-page recovery exhausted; opening fresh browser page"
                        )
                        self._close_page(page)
                        page = self._open_dedicated_ove_page(browser)
                        continue
                except (BrowserSessionError, PlaywrightTimeoutError) as exc:
                    last_error = exc

                # Normal reset between retries (for non-empty-page failures).
                try:
                    page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
                    self._wait_for_saved_search_cards(page, timeout_ms=15_000)
                except SavedSearchPageEmpty:
                    LOGGER.warning(
                        "Reset to saved-searches returned empty page between retries"
                    )
                except Exception as nav_exc:
                    LOGGER.warning(
                        "Failed to reset page to saved-searches between export retries: %s",
                        nav_exc,
                    )
        finally:
            # Capture debug artifacts BEFORE closing the page so a follow-up
            # debugging session can see what OVE actually rendered when the
            # export failed. Only fired on the failure path; the success
            # path skips this entirely.
            if last_error is not None:
                self._capture_export_failure_artifacts(page, search_name, last_error)
            self._close_page(page)

        if last_error is None:
            raise BrowserSessionError(f"Could not export saved search '{search_name}'")
        # Preserve SavedSearchPageEmpty subclass so run_browser_operation's
        # cookie-clearing + re-login path fires. Wrapping it as a generic
        # BrowserSessionError demotes the signal and the weaker recovery
        # (kill Chrome, relaunch with same cookies) runs instead.
        exc_type = (
            SavedSearchPageEmpty
            if isinstance(last_error, SavedSearchPageEmpty)
            else BrowserSessionError
        )
        raise exc_type(
            f"Could not export saved search '{search_name}' after {max_attempts} attempts: {last_error}"
        )

    def _capture_export_failure_artifacts(
        self,
        page: Page,
        search_name: str,
        last_error: Exception,
    ) -> None:
        try:
            timestamp = time.strftime("%Y%m%dT%H%M%S")
            slug = slugify(search_name)
            failure_dir = self.settings.artifact_dir / "sync-failures" / f"{timestamp}-{slug}"
            failure_dir.mkdir(parents=True, exist_ok=True)
            html_path = failure_dir / "saved-search-page.html"
            screenshot_path = failure_dir / "saved-search-page.png"
            error_path = failure_dir / "error.txt"
            try:
                html_path.write_text(page.content(), encoding="utf-8")
            except Exception as exc:
                LOGGER.warning("Could not capture failure HTML for '%s': %s", search_name, exc)
            try:
                page.screenshot(path=str(screenshot_path), full_page=True)
            except Exception as exc:
                LOGGER.warning("Could not capture failure screenshot for '%s': %s", search_name, exc)
            try:
                error_path.write_text(
                    f"Search: {search_name}\nLast error: {last_error}\n",
                    encoding="utf-8",
                )
            except Exception:
                pass
            LOGGER.error(
                "Saved debug artifacts for failed export of '%s' to %s",
                search_name,
                failure_dir,
            )
            # Stash the directory on the exception so the sync layer can
            # forward it to the notifier without re-deriving the path.
            try:
                setattr(last_error, "debug_artifact_dir", str(failure_dir))
            except Exception:
                pass
        except Exception as outer_exc:
            LOGGER.error(
                "Failed to capture export failure artifacts for '%s': %s",
                search_name,
                outer_exc,
            )

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
            # The OVE detail panel is React-rendered and the CR link
            # appears AFTER initial DOM mount. Verified empirically against
            # VIN 1FTFW1E82MFB44312: the CR link was present in the saved
            # listing.html (captured ~700ms after extraction) but absent
            # from the DETAIL_EXTRACTION_SCRIPT result, so the script ran
            # before React hydrated the link. Wait up to 8s for any of the
            # known CR link selectors to appear before scraping. Not raising
            # on timeout — VINs without a CR (rare under the East/West Hub
            # filters but possible) should still produce a useful payload.
            self._wait_for_cr_link_to_render(detail_page, timeout_ms=8000)
            # NOTE: Earlier this session I added _wait_for_listing_gallery_to_render
            # here, on the theory that the OVE listing panel's SimpleViewer
            # gallery just needed time to lazy-load. Empirical test against
            # 1N4BL4EV2NN423240 on 2026-04-09 16:06 disproved this: after
            # 12s of polling the listing panel still had only 1 Manheim CDN
            # image. The full image gallery for OVE listings does NOT live
            # in the listing panel — it lives inside the CR popup, which is
            # what the F-150 manual-rerun-payload-4 capture proved (the F-150
            # listing.html only had the gallery because the CR popup had
            # already been opened earlier in the session). The right strategy
            # is to skip the listing-panel gallery wait entirely and rely on
            # the CR popup capture as the canonical image source. The wait
            # was removed to (a) save 12s per scrape and (b) avoid letting
            # the OVE React app re-render the panel during the wait, which
            # could move the CR badge between wait_for_selector and the
            # extraction script.
            LOGGER.info("Stage extract_payload: starting for VIN %s", vin)
            payload = detail_page.evaluate(
                DETAIL_EXTRACTION_SCRIPT.replace("ROOT_SELECTOR", json.dumps(self.settings.ove_section_root_selector))
            )
            html_path = artifact_dir / "listing.html"
            screenshot_path = artifact_dir / "listing.png"
            html_path.write_text(detail_page.content(), encoding="utf-8")
            detail_page.screenshot(path=str(screenshot_path), full_page=True)

            # Diagnostic logging for CR link extraction. The findConditionReportLink
            # JS now returns a debug bundle (candidate counts + scored top 3 +
            # a direct querySelector probe) so we can see WHY the link is null
            # when the badge appears in the saved listing.html. Empirically the
            # Nissan Altima 1N4BL4EV2NN423240 hits this case every time despite
            # the badge being structurally present in the saved DOM.
            cr_link_value = payload.get("condition_report_link")
            cr_link_debug = payload.get("condition_report_link_debug") or {}
            cr_link_probe = payload.get("condition_report_link_direct_probe") or {}
            if cr_link_value is None:
                LOGGER.warning(
                    "VIN %s: findConditionReportLink returned None. "
                    "candidate_count=%s deduped_count=%s scored_top=%s direct_probe=%s",
                    vin,
                    cr_link_debug.get("candidate_count"),
                    cr_link_debug.get("deduped_count"),
                    cr_link_debug.get("scored_top"),
                    cr_link_probe,
                )
            else:
                # Always log the top-3 scored candidates so we can see when
                # findConditionReportLink picks the wrong winner. The
                # marketing footer "Condition Reporting" link bug on the
                # Nissan Altima 1N4BL4EV2NN423240 was invisible until this
                # diagnostic surfaced it.
                LOGGER.info(
                    "VIN %s: findConditionReportLink returned href=%s labelText=%s "
                    "valueText=%s testId=%s (candidate_count=%s scored_top=%s direct_probe=%s)",
                    vin,
                    cr_link_value.get("href"),
                    cr_link_value.get("labelText"),
                    cr_link_value.get("valueText"),
                    cr_link_value.get("testId"),
                    cr_link_debug.get("candidate_count"),
                    cr_link_debug.get("scored_top"),
                    cr_link_probe,
                )

            condition_report_link = self._select_valid_condition_report_link(
                result_card_report_link,
                payload.get("condition_report_link"),
            )
            # Pull the full OVE listing JSON out of the detail page DOM. This
            # is the structurally-stable source for announcements,
            # conditionGrade, autocheck, conditionReportUrl, paint colors,
            # and installedEquipment — none of which depend on the Manheim
            # CR navigation succeeding. Capturing it BEFORE the CR click
            # means we still get a usable payload even if the CR view fails
            # to load.
            listing_json = self._extract_ove_listing_json(detail_page)
            if listing_json is None:
                LOGGER.warning("Could not extract OVE listing JSON for VIN %s", vin)

            # Capture AutoCheck BEFORE navigating to the CR page — the inline
            # AutoCheck data lives on the listing page and will be lost after
            # the CR click navigates away.
            autocheck_data: dict[str, Any] | None = None
            try:
                LOGGER.info("Stage capture_autocheck: starting for VIN %s", vin)
                autocheck_data = self._capture_autocheck_on_page(detail_page, vin, artifact_dir)
                LOGGER.info(
                    "Stage capture_autocheck: complete for VIN %s (score=%s, status=%s)",
                    vin,
                    autocheck_data.get("autocheck_score") if autocheck_data else None,
                    autocheck_data.get("scrape_status", "unknown") if autocheck_data else "skipped",
                )
            except Exception as ac_exc:
                LOGGER.warning("AutoCheck capture failed (non-fatal) for VIN %s: %s", vin, ac_exc)
                autocheck_data = {"scrape_status": "failed", "failure_category": type(ac_exc).__name__, "failure_message": str(ac_exc)}

            LOGGER.info(
                "Stage capture_condition_report: starting for VIN %s (link present=%s, listing_json=%s)",
                vin,
                bool(condition_report_link),
                bool(listing_json),
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
                listing_json=listing_json,
            )
            # Attach AutoCheck report to the condition report
            if condition_report and autocheck_data and autocheck_data.get("scrape_status") != "not_attempted":
                from ove_scraper.schemas import AutoCheckReport
                condition_report.autocheck = AutoCheckReport.model_validate(autocheck_data)
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
        try:
            # Explicit 60s timeout (Playwright default is 30s) — OVE can be
            # slow to respond at peak auction times (9 AM ET, for example).
            # Combined with the retry wrapper in export_saved_search, this
            # lets us absorb multi-minute OVE hiccups.
            page.goto(start_url or self._saved_searches_url(), wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1500)
            if self._is_login_page(page) or self._is_error_page(page):
                self._close_page(page)
                raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")
        except BrowserSessionError:
            raise
        except Exception:
            self._close_page(page)
            raise
        return page

    # ------------------------------------------------------------------
    # Saved-search page: detection, wait, and recovery helpers
    # ------------------------------------------------------------------

    def _detect_empty_saved_searches_page(self, page: Page) -> bool:
        """Return True if the OVE saved-searches page rendered successfully
        but shows 'No Saved Searches' (i.e., OVE's API returned empty data).
        This is distinct from cards not rendering yet (slow React hydration)."""
        try:
            return page.evaluate(
                '() => (document.body?.innerText || "").toLowerCase().includes("no saved search")'
            )
        except Exception:
            return False

    def _wait_for_saved_search_cards(self, page: Page, *, timeout_ms: int = 15_000) -> None:
        """Wait for saved-search card elements to appear in the DOM.

        Raises:
            SavedSearchPageEmpty: page shows 'No Saved Searches' text.
            BrowserSessionError: login page detected or cards never rendered.
        """
        if self._is_login_page(page):
            raise BrowserSessionError("OVE session is not authenticated; browser is on the login page")

        if self._detect_empty_saved_searches_page(page):
            raise SavedSearchPageEmpty(
                "OVE saved-searches page shows 'No Saved Searches' — "
                "known intermittent OVE issue"
            )

        try:
            page.wait_for_selector("[data-test-id^='search name:']", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            if self._is_login_page(page):
                raise BrowserSessionError(
                    "OVE session expired during wait; browser redirected to login page"
                )
            if self._detect_empty_saved_searches_page(page):
                raise SavedSearchPageEmpty(
                    "OVE saved-searches page shows 'No Saved Searches' after "
                    f"{timeout_ms}ms wait — known intermittent OVE issue"
                )
            raise BrowserSessionError(
                f"Saved search cards did not render within {timeout_ms}ms — "
                "possible cold session or OVE outage"
            )

    def _recovery_hard_reload(self, page: Page) -> None:
        """Cache-busting reload of the saved-searches page."""
        page.evaluate("() => location.reload(true)")
        page.wait_for_load_state("domcontentloaded", timeout=15_000)
        page.wait_for_timeout(2000)

    def _recovery_navigate_roundtrip(self, page: Page) -> None:
        """Navigate to OVE homepage, wait, then back to saved searches.
        Forces a fresh API call from the React app."""
        page.goto(self.settings.ove_base_url, wait_until="domcontentloaded", timeout=15_000)
        page.wait_for_timeout(3000)
        page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=15_000)
        page.wait_for_timeout(2000)

    def _recover_empty_saved_searches(self, page: Page) -> None:
        """Attempt escalating recovery when OVE returns 'No Saved Searches'.

        Strategies tried in order:
          1. Hard reload — clears cached response.
          2. Navigate away to OVE homepage, then back — forces fresh API call.
          3. Raise so the caller can try a fresh browser page.

        Raises SavedSearchPageEmpty if all in-page strategies fail.
        """
        strategies: list[tuple[str, Any]] = [
            ("hard_reload", self._recovery_hard_reload),
            ("navigate_away_and_back", self._recovery_navigate_roundtrip),
        ]
        for i, (name, fn) in enumerate(strategies):
            LOGGER.warning(
                "Empty saved-searches recovery attempt %d/%d: strategy=%s",
                i + 1, len(strategies) + 1, name,
            )
            try:
                fn(page)
            except Exception as exc:
                LOGGER.warning("Recovery strategy '%s' raised: %s", name, exc)
                continue

            try:
                self._wait_for_saved_search_cards(page, timeout_ms=10_000)
                LOGGER.info("Recovery strategy '%s' succeeded", name)
                return
            except SavedSearchPageEmpty:
                LOGGER.warning("Recovery strategy '%s' did not resolve empty page", name)
                continue
            except BrowserSessionError:
                raise  # login page or other hard failure

        raise SavedSearchPageEmpty(
            "OVE 'No Saved Searches' persists after all in-page recovery strategies"
        )

    def _open_saved_search(self, page: Page, search_name: str) -> None:
        # Use domcontentloaded rather than networkidle: the OVE saved-searches
        # page runs continuous analytics/polling XHRs that prevent networkidle
        # from ever firing (verified 2026-04-15 — the page renders fully but
        # the 500ms-idle condition never clears, so goto times out at 30s even
        # though the saved-search cards are visible). The _wait_for_saved_search_cards
        # call below is the real readiness signal, and also distinguishes
        # "No Saved Searches" (SavedSearchPageEmpty) from slow hydration.
        page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
        self._wait_for_saved_search_cards(page, timeout_ms=15_000)
        matched_name = self._resolve_saved_search_name(page, search_name)
        # The saved-search card in the OVE UI is a nested div structure:
        #   <div data-test-id="search name: East Hub 2022-2024">      ← outer wrapper (NOT clickable)
        #     <div class="SavedSearchItem__container">
        #       <div class="SavedSearchItem__top-row">
        #         <div class="SavedSearchItem__title-container">
        #           <span class="Tracker__container">
        #             <div class="SavedSearchItem__title">            ← React click handler lives HERE
        #               East Hub 2022-2024
        #               <div class="newListingText">New (5759)</div>
        #
        # The previous code clicked the outer wrapper div (line 748's
        # data-test-id selector). That div does NOT have a React click
        # handler — only the inner title div does. The click "succeeded"
        # from Playwright's POV but no navigation happened, and the code
        # returned as if it had navigated, then _trigger_export tried to
        # find the export button on the LIST page and failed.
        #
        # Fix: locate the card wrapper by data-test-id, then drill into
        # the inner title element and click THAT. If the inner title
        # selector changes in the future, fall back to clicking the text.
        card_selector = f"[data-test-id='search name: {matched_name}']"
        card = page.locator(card_selector).first
        original_url = page.url
        if card.count():
            # Strategy 1: click the inner title div inside the card
            title_in_card = card.locator(".SavedSearchItem__title, .SavedSearchItem__title-container").first
            click_target = title_in_card if title_in_card.count() else card
            try:
                click_target.click(timeout=10000)
                # Wait for the page to actually navigate away from the
                # saved-searches list. The previous code used
                # wait_for_load_state("networkidle") which returns
                # immediately if no navigation happened. We now verify
                # the URL actually changed.
                page.wait_for_timeout(2000)
                if page.url != original_url and "/saved_searches" not in page.url:
                    # Don't wait for networkidle — OVE has persistent XHR
                    # polling that never settles. domcontentloaded is the
                    # real signal, and the subsequent export selector wait
                    # ensures the results rendered.
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                    LOGGER.info("Navigated to saved search '%s' results at %s", matched_name, page.url)
                    return
                LOGGER.warning(
                    "Click on saved search '%s' did not navigate away from list page (url=%s)",
                    matched_name,
                    page.url,
                )
            except PlaywrightTimeoutError:
                LOGGER.warning("Click on saved search '%s' card timed out", matched_name)

        # Strategy 2: configured link selector (a:has-text / button:has-text)
        link_selector = self.settings.ove_saved_search_link_selector.format(search_name=matched_name)
        link_locator = page.locator(link_selector).first
        if link_locator.count():
            try:
                link_locator.click(timeout=10000)
                page.wait_for_timeout(2000)
                if page.url != original_url and "/saved_searches" not in page.url:
                    # Don't wait for networkidle — OVE has persistent XHR
                    # polling that never settles. domcontentloaded is the
                    # real signal, and the subsequent export selector wait
                    # ensures the results rendered.
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                    LOGGER.info("Navigated to saved search '%s' via link selector at %s", matched_name, page.url)
                    return
            except PlaywrightTimeoutError:
                pass

        # Strategy 3: click the text directly
        title_locator = page.get_by_text(matched_name, exact=False).first
        try:
            title_locator.click(timeout=10000)
            page.wait_for_timeout(2000)
            if page.url != original_url and "/saved_searches" not in page.url:
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                LOGGER.info("Navigated to saved search '%s' via text click at %s", matched_name, page.url)
                return
        except PlaywrightTimeoutError:
            pass

        raise BrowserSessionError(
            f"Unable to navigate into saved search '{search_name}': all click strategies failed "
            f"(card found={card.count() > 0}, url after attempts={page.url})"
        )

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
        # Defensive: ensure search-card elements are in the DOM before
        # querying. The caller (_resolve_saved_search_name via
        # _open_saved_search) should have already waited, but this method
        # is also reachable from list_saved_searches which has its own
        # navigation path.
        try:
            self._wait_for_saved_search_cards(page, timeout_ms=10_000)
        except (SavedSearchPageEmpty, BrowserSessionError):
            LOGGER.warning("Search-card elements not found in DOM; _collect_saved_search_names may return empty")
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

        # Wait for actual vehicle results to render before exporting.
        # Without this, OVE can export a 0-byte CSV when the React app
        # has navigated to the results URL but hasn't populated the
        # results grid yet (seen at peak OVE load).
        try:
            page.wait_for_selector(
                "[data-test-id*='vehicle'], [class*='VehicleCard'], tr[data-test-id*='vehicle']",
                state="visible",
                timeout=20000,
            )
            page.wait_for_timeout(1500)  # let the rest of the grid catch up
        except PlaywrightTimeoutError:
            LOGGER.warning("Vehicle results selector not found before export; proceeding anyway")

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
        # Try Playwright's save_as first. When Playwright connects over CDP
        # to a user-launched Chrome, the download object often has no real
        # content (Chrome has already auto-saved to the user's Downloads
        # folder before Playwright's interceptor attaches). The save_as
        # call may succeed but produce a 0-byte file.
        save_as_worked = False
        try:
            download.save_as(str(target_path))
            if target_path.exists() and target_path.stat().st_size > 0:
                save_as_worked = True
        except Exception:
            pass

        if save_as_worked:
            return

        # Fallback: find the actual CSV in Chrome's default Downloads folder.
        # OVE names exports "Export.csv", "Export (1).csv", "Export (2).csv",
        # etc. — we pick the newest Export*.csv modified in the last 2 min.
        import time as _time
        chrome_downloads = Path.home() / "Downloads"
        cutoff = _time.time() - 120  # 2 minutes
        candidates = sorted(
            (p for p in chrome_downloads.glob("Export*.csv")
             if p.stat().st_mtime >= cutoff and p.stat().st_size > 0),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            source = candidates[0]
            import shutil
            try:
                shutil.copy2(source, target_path)
                LOGGER.info(
                    "Copied OVE export for '%s' from Chrome Downloads (%s, %d bytes)",
                    search_name, source.name, source.stat().st_size,
                )
                # Clean up Chrome's copy so we don't re-pick it on next retry
                try:
                    source.unlink()
                except Exception:
                    pass
                if target_path.exists() and target_path.stat().st_size > 0:
                    return
            except Exception as exc:
                LOGGER.warning("Fallback copy from Chrome Downloads failed: %s", exc)

        raise BrowserSessionError(
            f"Exported CSV for '{search_name}' was empty"
            f" (suggested filename: {download.suggested_filename})"
        )

    def _remove_file_if_present(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # AutoCheck modal scraping (Hot Deal pipeline)
    # ------------------------------------------------------------------

    # Universal AutoCheck click target across all OVE CR types: the
    # branded Experian image wrapped by Manheim's Tracker__container,
    # which carries the click delegate that opens the full report.
    # data-test-id="autocheck-link" is on the <img>; click it with
    # expect_popup and the full Experian report opens in a new window.
    _AUTOCHECK_BUTTON_SELECTORS = [
        "[data-test-id='autocheck-link']",
        "a.Autocheck__fullreport-link",
        "[data-test-id='report-data'] a",
        "a[href*='autocheckreport']",
        "a[data-label-text='AutoCheck']",
        "a[data-label-text='AC']",
        ".VehicleReportLink__autocheck-link",
    ]

    # JavaScript to extract inline AutoCheck data from OVE listing page.
    # The AutoCheck section uses data-test-id attributes for each field.
    _AUTOCHECK_INLINE_EXTRACT_JS = """
    () => {
        const result = {raw_text: '', score: null, owners: null, accidents: null,
                        title_probs: '', odo: '', use_event: '', view_report_href: ''};
        const section = document.querySelector('[data-test-id="auto-check"]')
                     || document.querySelector('[data-test-id="auto-check-data"]');
        if (!section) return result;
        result.raw_text = section.innerText || '';
        const scoreEl = section.querySelector('[data-test-id="score-data"]')
                     || section.querySelector('[data-test-id="score"]');
        if (scoreEl) {
            const m = scoreEl.innerText.match(/(\\d+)/);
            if (m) result.score = parseInt(m[1]);
        }
        // Walk text nodes looking for structured data
        const text = result.raw_text;
        const ownersMatch = text.match(/Owners\\s*(\\d+)/i);
        if (ownersMatch) result.owners = parseInt(ownersMatch[1]);
        const accMatch = text.match(/Accidents?\\s*(?:ACDNT)?\\s*(\\d+)/i);
        if (accMatch) result.accidents = parseInt(accMatch[1]);
        // Check for title/problem indicators
        if (/Titles?\\/Probs?/i.test(text)) {
            // Look for icon indicators near it
            const titleSection = text.match(/Titles?\\/Probs?(.{0,50})/i);
            result.title_probs = titleSection ? titleSection[1].trim() : '';
        }
        if (/\\bODO\\b/i.test(text)) {
            const odoSection = text.match(/ODO(.{0,50})/i);
            result.odo = odoSection ? odoSection[1].trim() : '';
        }
        // VIEW REPORT link — OVE's test-id is report-data (class
        // Autocheck__fullreport-link). Keep the host/path fallbacks
        // for defense against future DOM churn.
        const reportLink = section.querySelector('[data-test-id="report-data"] a')
                        || section.querySelector('a.Autocheck__fullreport-link')
                        || section.querySelector('a[href*="autocheckreport"]')
                        || section.querySelector('a[href*="vehiclehistservice"]')
                        || section.querySelector('a[href*="autocheck"]');
        if (reportLink) result.view_report_href = reportLink.href || '';
        return result;
    }
    """

    def _capture_autocheck_on_page(
        self, detail_page: Page, vin: str, artifact_dir: Path,
    ) -> dict[str, Any]:
        """Extract AutoCheck data from an already-open OVE detail page.

        Called from deep_scrape_vin (before CR navigation) and from
        scrape_autocheck_modal (standalone Hot Deal wrapper).
        """
        ac_artifact_dir = artifact_dir / "autocheck"
        ac_artifact_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Extract inline AutoCheck indicators from the listing DOM.
        # OVE does NOT display the numeric AutoCheck score inline — score,
        # title brands, odometer branding and narrative only live in the
        # full Experian report — so this step gathers owners/accidents/href
        # and is purely a precursor to the mandatory Experian navigation.
        LOGGER.info("AutoCheck: extracting inline indicators for VIN %s", vin)
        inline_data = detail_page.evaluate(self._AUTOCHECK_INLINE_EXTRACT_JS) or {}
        has_inline_section = bool(
            inline_data.get("view_report_href")
            or inline_data.get("owners") is not None
            or inline_data.get("accidents") is not None
            or (inline_data.get("raw_text") or "").strip()
        )
        if has_inline_section:
            detail_page.screenshot(path=str(ac_artifact_dir / "autocheck-inline.png"), full_page=True)
            (ac_artifact_dir / "autocheck-inline.html").write_text(detail_page.content(), encoding="utf-8")
            LOGGER.info(
                "AutoCheck inline indicators for VIN %s: owners=%s accidents=%s href=%s",
                vin,
                inline_data.get("owners"),
                inline_data.get("accidents"),
                bool(inline_data.get("view_report_href")),
            )
            result = self._parse_autocheck_inline(inline_data)
        else:
            LOGGER.warning("AutoCheck: no inline section detected for VIN %s", vin)
            detail_page.screenshot(path=str(ac_artifact_dir / "autocheck-none.png"), full_page=True)
            (ac_artifact_dir / "autocheck-none.html").write_text(detail_page.content(), encoding="utf-8")
            return {
                "scrape_status": "failed",
                "failure_category": "not_found",
                "failure_message": "No AutoCheck section on listing page",
                "raw_text": "",
            }

        # Step 2: MANDATORY — open the full Experian report. Title brands,
        # odometer branding and other serious issues are frequently omitted
        # from the inline listing view but always appear in the full report.
        # Per user directive 2026-04-21: never short-circuit to inline-only
        # success; a deep scrape that skipped Experian is flagged partial
        # with a clear failure_category so downstream consumers can see the
        # data gap.
        experian_outcome = self._capture_experian_autocheck_report(
            detail_page, result, ac_artifact_dir, vin,
        )
        result["scrape_status"] = experian_outcome["scrape_status"]
        if experian_outcome.get("failure_category"):
            result["failure_category"] = experian_outcome["failure_category"]
        if experian_outcome.get("failure_message"):
            result["failure_message"] = experian_outcome["failure_message"]
        return result

    def _capture_experian_autocheck_report(
        self,
        detail_page: Page,
        result: dict[str, Any],
        ac_artifact_dir: Path,
        vin: str,
    ) -> dict[str, Any]:
        """Open the full Experian AutoCheck report and merge its data into
        ``result``. Tries direct URL navigation first (same browser context
        = same Manheim SSO cookies), then falls back to clicking the
        branded autocheck-link image with expect_popup. Returns a dict with
        scrape_status and optional failure_category/failure_message."""
        view_href = (result.get("view_report_href") or "").strip()
        if not view_href:
            # Re-extract from the DOM at the moment of navigation, in case
            # the inline JS snapshot missed a late-rendered link.
            for selector in (
                "a.Autocheck__fullreport-link",
                "[data-test-id='report-data'] a",
                "a[href*='autocheckreport']",
                "a[href*='vehiclehistservice']",
            ):
                try:
                    report_link = detail_page.locator(selector).first
                    if report_link.is_visible(timeout=2000):
                        view_href = (report_link.get_attribute("href", timeout=2000) or "").strip()
                        if view_href:
                            result["view_report_href"] = view_href
                            break
                except Exception:
                    continue

        # Path A: direct navigation to the report URL in a new tab. This is
        # the most reliable path — same context inherits Manheim SSO cookies
        # and Playwright controls the page fully.
        if view_href:
            try:
                report_page = detail_page.context.new_page()
                report_page.goto(view_href, wait_until="domcontentloaded", timeout=20000)
                report_page.wait_for_timeout(3000)
                report_page.screenshot(path=str(ac_artifact_dir / "autocheck-report.png"), full_page=True)
                (ac_artifact_dir / "autocheck-report.html").write_text(report_page.content(), encoding="utf-8")
                full_text = report_page.inner_text("body")
                report_page.close()
                if self._looks_like_login_page(full_text):
                    LOGGER.warning("AutoCheck full report navigation landed on a login page for VIN %s", vin)
                else:
                    self._merge_full_experian_report(result, full_text)
                    LOGGER.info("AutoCheck full report captured via direct nav for VIN %s (%d chars)", vin, len(full_text))
                    return {"scrape_status": "success"}
            except Exception as report_exc:
                LOGGER.warning("AutoCheck direct navigation failed for VIN %s: %s", vin, report_exc)

        # Path B: click the branded autocheck-link image with expect_popup.
        # Per user 2026-04-21: this is the universal trigger across CR types
        # — the Tracker__container wrapper fires a JS click delegate that
        # opens the Experian report in a popup window.
        for selector in ("[data-test-id='autocheck-link']", "a.Autocheck__fullreport-link"):
            try:
                trigger = detail_page.locator(selector).first
                if not trigger.is_visible(timeout=1500):
                    continue
                LOGGER.info("AutoCheck: attempting popup click via %s for VIN %s", selector, vin)
                with detail_page.expect_popup(timeout=8000) as popup_info:
                    trigger.click(timeout=5000)
                popup = popup_info.value
                popup.wait_for_load_state("domcontentloaded", timeout=20000)
                popup.wait_for_timeout(3000)
                popup.screenshot(path=str(ac_artifact_dir / "autocheck-popup.png"), full_page=True)
                (ac_artifact_dir / "autocheck-popup.html").write_text(popup.content(), encoding="utf-8")
                popup_text = popup.inner_text("body")
                popup.close()
                if self._looks_like_login_page(popup_text):
                    LOGGER.warning("AutoCheck popup landed on a login page for VIN %s", vin)
                    continue
                self._merge_full_experian_report(result, popup_text)
                LOGGER.info("AutoCheck full report captured via popup click for VIN %s (%d chars)", vin, len(popup_text))
                return {"scrape_status": "success"}
            except Exception as popup_exc:
                LOGGER.debug("AutoCheck popup click via %s failed for VIN %s: %s", selector, vin, popup_exc)

        # Could not reach the Experian report. Keep the inline indicators
        # but mark partial so consumers know title-brand / odometer-brand
        # coverage is incomplete for this VIN.
        detail_page.screenshot(path=str(ac_artifact_dir / "autocheck-experian-missing.png"), full_page=True)
        return {
            "scrape_status": "partial",
            "failure_category": "experian_report_unreachable",
            "failure_message": (
                "Inline AutoCheck indicators captured but the full Experian "
                "report could not be opened; title-brand and odometer-brand "
                "checks may be incomplete."
            ),
        }

    @staticmethod
    def _looks_like_login_page(text: str) -> bool:
        head = (text or "").lower()[:400]
        return ("sign in" in head) or ("username" in head) or ("password" in head and "log in" in head)

    def _merge_full_experian_report(self, result: dict[str, Any], full_text: str) -> None:
        """Parse the Experian report body and merge its fields into result.
        Overwrites inline-derived OK/Problem placeholders because the full
        report is authoritative for title-brand and odometer branding."""
        result["full_report_text"] = full_text
        full_parsed = self._parse_autocheck_content(full_text)
        # The full report's title_brand_check / odometer_check / accident_check
        # override the inline heuristics. Inline only knows whether an icon
        # was green/red — the full report knows *what* the brand is.
        for key in (
            "title_brand_check",
            "odometer_check",
            "accident_check",
            "damage_check",
            "vehicle_use",
            "buyback_protection",
        ):
            full_value = full_parsed.get(key)
            if full_value:
                result[key] = full_value
        if full_parsed.get("autocheck_score") is not None:
            result["autocheck_score"] = full_parsed["autocheck_score"]

    def scrape_autocheck_modal(self, vin: str, artifact_dir: Path) -> dict[str, Any]:
        """Standalone AutoCheck scrape — opens its own page for the VIN.

        Used by the Hot Deal pipeline when a deep scrape isn't needed.
        For deep scrape integration, _capture_autocheck_on_page is called
        directly on the already-open detail page.
        """
        self.ensure_session()
        browser = self._connect_browser()
        seed_page = self._get_ove_page(browser.contexts)
        page = self._create_worker_page(seed_page.context, start_url=self._vin_results_url())

        try:
            LOGGER.info("AutoCheck scrape: opening listing for VIN %s", vin)
            detail_page, _ = self._open_listing_for_vin(page, vin, artifact_dir)
            self._wait_for_cr_link_to_render(detail_page, timeout_ms=8000)
            return self._capture_autocheck_on_page(detail_page, vin, artifact_dir)
        except Exception as exc:
            LOGGER.error("AutoCheck scrape failed for VIN %s: %s", vin, exc)
            ac_dir = artifact_dir / "autocheck"
            ac_dir.mkdir(parents=True, exist_ok=True)
            try:
                self._capture_debug_state(page, ac_dir, "autocheck-error", vin, exc)
            except Exception:
                pass
            raise
        finally:
            try:
                page.close()
            except Exception:
                pass

    @staticmethod
    def _parse_autocheck_inline(data: dict) -> dict[str, Any]:
        """Convert inline JS extraction result into the structured format
        expected by the screener."""
        raw_text = data.get("raw_text", "")
        result: dict[str, Any] = {"raw_text": raw_text}
        result["autocheck_score"] = data.get("score")
        result["owner_count"] = data.get("owners")
        result["accident_count"] = data.get("accidents")
        result["view_report_href"] = data.get("view_report_href", "")

        # Derive screening-compatible fields from inline indicators.
        # The inline section shows icons/text for title problems and
        # odometer issues but doesn't spell out "Problem Reported" —
        # a non-empty title_probs or odo field indicates an issue.
        title_probs = data.get("title_probs", "").strip()
        odo = data.get("odo", "").strip()

        # The inline view shows checkmark/X icons via CSS classes.
        # If the raw_text around Titles/Probs contains indicator text
        # (anything other than whitespace), flag it.
        result["title_brand_check"] = "Problem Reported" if title_probs else "OK"
        result["odometer_check"] = "Problem Reported" if odo else "OK"
        accidents = data.get("accidents") or 0
        result["accident_check"] = (
            f"{accidents} accident(s) reported" if accidents > 0 else "OK"
        )
        result["damage_check"] = ""
        result["vehicle_use"] = ""
        result["buyback_protection"] = ""
        return result

    @staticmethod
    def _parse_autocheck_content(raw_text: str) -> dict[str, Any]:
        """Parse raw AutoCheck modal text into structured sections."""
        result: dict[str, Any] = {"raw_text": raw_text}

        sections = {
            "title_brand_check": r"(?:Major\s*)?(?:State\s*)?Title\s*Brand\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "accident_check": r"Accident\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "damage_check": r"Damage\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "odometer_check": r"Odometer\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "vehicle_use": r"Vehicle\s*Usage?\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "buyback_protection": r"(?:AutoCheck\s*)?Buyback\s*Protection\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
        }

        for key, pattern in sections.items():
            match = re.search(pattern, raw_text, re.IGNORECASE | re.DOTALL)
            result[key] = match.group(1).strip() if match else ""

        # Extract score if present
        score_match = re.search(r"(?:AutoCheck\s*Score|score)\s*:?\s*(\d+)", raw_text, re.IGNORECASE)
        if score_match:
            result["autocheck_score"] = int(score_match.group(1))

        # Extract accident count
        accident_match = re.search(r"Number\s*of\s*Accidents?\s*:?\s*(\d+)", raw_text, re.IGNORECASE)
        if accident_match:
            result["accident_count"] = int(accident_match.group(1))

        # Extract owner count
        owner_match = re.search(r"(?:Calculated\s*)?Owners?\s*:?\s*(\d+)", raw_text, re.IGNORECASE)
        if owner_match:
            result["owner_count"] = int(owner_match.group(1))

        return result

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
            page = self._open_condition_report_page(source_page, context, href, artifact_dir)
            if page is None:
                return None
            # Wait for the OVE-internal CR view to finish lazy-loading its
            # image gallery BEFORE we snapshot the DOM. The gallery is
            # React-rendered after the route mount; manual-rerun-payload-2
            # and -3 captured 1 image while -4 captured all 17, all on the
            # same VIN with the same code path — proof the timing race is
            # real. We poll the rendered Manheim CDN <img> count until it
            # is stable for 2 consecutive polls and matches the gallery
            # widget's "of N" hint when present.
            self._wait_for_cr_gallery_stable(page)
            snapshot = self._snapshot_page(page, artifact_dir / "condition-report")
            # Defense in depth: even after the popup/click navigation
            # appears to succeed, double-check the captured page is not an
            # auth redirect. This protects against new OAuth failure modes
            # we have not seen yet, and against quirks where a page redirects
            # AFTER domcontentloaded but BEFORE we extract content.
            self._raise_if_auth_redirect(page, artifact_dir, "post_snapshot", href)
            return snapshot
        except ManheimAuthRedirectError:
            # Let auth-redirect errors propagate so the worker can route them
            # via /fail with auth_expired (or pre-push validation can hard-stop
            # the entire CR push). DO NOT swallow.
            raise
        except ConditionReportClickFailedError:
            # Same reasoning: exhausted CR click retries are ~always a stale
            # OVE SSO. Propagate so _classify_failure routes to auth_expired
            # and the single-attempt fail-streak cap escalates to /terminal
            # immediately. Previously this fell through to the generic
            # Exception handler below, which returned None and let the scrape
            # assemble a partial payload that then failed the tire-depths
            # validation gate as page_structure_changed (cap=5) — the real
            # mechanism behind the Ferrari ZFF96NMA4N0272307 loop.
            raise
        except Exception as exc:
            LOGGER.warning(
                "Condition report capture failed (non-auth) for %s: %s",
                href,
                exc,
            )
            return None
        finally:
            if page is not None and page is not source_page:
                self._close_page(page)
            if owns_context:
                try:
                    context.close()
                except Exception:
                    pass

    # Hosts that serve their CR HTML directly without bouncing through
    # Manheim SSO. For these we use a simple context.new_page().goto(href).
    # Empirical justification per host:
    #
    #   inspectionreport.manheim.com - WAUE Audi (03-19) and 5N1B Nissan
    #     (03-26) captures both produced cleanly structured Manheim CR text
    #     that the per-family parsers extract perfectly via direct popup
    #     load. Accepts OVE session cookies, no SSO bounce.
    #
    #   mmsc400.manheim.com - Manheim Express ECR endpoint. Same pattern:
    #     plain HTML, no SSO bounce. Has its own per-family parser
    #     (_parse_manheim_ecr).
    #
    #   content.liquidmotors.com - Third-party CR provider used by some
    #     OVE Partner Auction listings. Per user 2026-04-09: the "Auto Grade
    #     only" saved-search filter does NOT actually exclude liquidmotors-
    #     hosted CRs; OVE Partner Auction listings still slip through with
    #     content.liquidmotors.com/IR/{dealer_id}/{cr_id}.html links.
    #     Verified live against VIN 1N4BL4EV2NN423240. The href carries the
    #     OVE session token via ?username=CIAplatform query param, so a
    #     direct goto loads the CR HTML without an SSO bounce. Has its own
    #     per-family parser (_parse_liquidmotors_ir at cr_parsers.py:120)
    #     which dispatches to the inspectionreport or ECR parser based on
    #     text markers.
    _DIRECT_GOTO_CR_HOSTS = frozenset(
        {
            "inspectionreport.manheim.com",
            "mmsc400.manheim.com",
            "content.liquidmotors.com",
        }
    )

    # Host that requires Manheim SSO (which the scraper has no credentials
    # for). Direct goto on this host always 302s to auth.manheim.com and the
    # scraper captures the login form in place of the CR. We must instead
    # click the CR link on the OVE detail page and let the OVE webapp
    # perform the SSO bounce server-side, which renders the CR inside an
    # iframe at the #/details/{vin}/OVE/conditionInformation hash route.
    _OVE_INTERNAL_CR_HOSTS = frozenset(
        {
            "insightcr.manheim.com",
        }
    )

    def _open_condition_report_page(
        self,
        source_page: Page,
        context: BrowserContext,
        href: str,
        artifact_dir: Path,
    ) -> Page | None:
        # Dispatch by CR-link host. The two Manheim CR providers in scope
        # (per the "Auto Grade only" filter) require completely different
        # navigation strategies:
        #
        #   inspectionreport.manheim.com / mmsc400.manheim.com (Manheim Express)
        #     -> direct context.new_page().goto(href). Accepts OVE session
        #        cookies, no SSO bounce, CR HTML loads cleanly.
        #
        #   insightcr.manheim.com
        #     -> click the CR link on the OVE detail page and wait for the
        #        OVE webapp's internal hash route. Direct goto on this host
        #        ALWAYS lands on auth.manheim.com because Manheim requires
        #        an SSO bounce that only the OVE webapp itself can perform.
        #
        # An earlier version of this function (commit d6136d4) deleted the
        # direct-goto path globally to fix the insightcr auth-corruption
        # bug, but in doing so it forced the previously-working
        # inspectionreport vehicles through the same fragile click+hash-route
        # flow as the broken insightcr ones. The fix is to keep the direct
        # goto for the hosts that work with it.
        host = ""
        try:
            host = (urlparse(href).netloc or "").lower()
        except Exception:
            host = ""

        if host in self._DIRECT_GOTO_CR_HOSTS:
            return self._open_via_direct_goto(context, href, artifact_dir)

        if host in self._OVE_INTERNAL_CR_HOSTS:
            return self._open_via_ove_internal_viewer(source_page, href, artifact_dir)

        # Unknown / new host. Default to the OVE-internal viewer because it
        # is the safer choice (server-side SSO is harmless on hosts that
        # don't require it). Log loudly so the host can be added to the
        # explicit dispatch table.
        LOGGER.warning(
            "Unknown CR host %r; defaulting to OVE-internal viewer for href=%s",
            host,
            href,
        )
        return self._open_via_ove_internal_viewer(source_page, href, artifact_dir)

    def _open_via_direct_goto(
        self,
        context: BrowserContext,
        href: str,
        artifact_dir: Path,
    ) -> Page | None:
        # Direct popup load for inspectionreport.manheim.com /
        # mmsc400.manheim.com. Returns the new Page on success. On failure
        # we close the page and return None so the lease queue retries via
        # the standard /fail flow.
        page: Page | None = None
        try:
            page = context.new_page()
            LOGGER.info("Opening CR via direct goto: %s", href)
            page.goto(href, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                # networkidle is best-effort. Manheim CR pages occasionally
                # keep a long-lived analytics socket open; the DOM we need
                # is already there by the time domcontentloaded fires.
                pass
            # Defense in depth: if Manheim ever adds an SSO bounce on a host
            # that previously didn't need one, the auth detector at
            # _capture_condition_report_page will catch it post-snapshot
            # and route the request via /fail with auth_expired. We do NOT
            # silently swallow that case here.
            return page
        except Exception as exc:
            LOGGER.warning("Direct-goto CR open failed for %s: %s", href, exc)
            if page is not None:
                self._close_page(page)
            try:
                artifact_dir.mkdir(parents=True, exist_ok=True)
                (artifact_dir / "cr-direct-goto-failed.txt").write_text(
                    f"href={href}\nerror={exc!r}\n", encoding="utf-8"
                )
            except Exception:
                pass
            return None

    def _open_via_ove_internal_viewer(
        self,
        source_page: Page,
        href: str,
        artifact_dir: Path,
    ) -> Page | None:
        # The OVE webapp serves its OWN condition-report viewer at the
        # client-side hash route #/details/{vin}/OVE/conditionInformation.
        # That route is reachable ONLY by clicking the CR link element on
        # the OVE detail page — the click triggers an OVE React handler
        # that performs an internal SSO bounce against Manheim's backend
        # and renders the CR (with all images) inside the OVE webapp,
        # without a popup, without an OAuth handshake the scraper can see.
        #
        # We do NOT pass `href` to a goto here. We use the href ONLY to
        # label debug artifacts on failure, and the click on the OVE
        # locator element is the sole navigation mechanism.
        #
        # Under a stale OVE SSO the React handler sometimes delegates to
        # window.open(insightcr...) instead of a hash-route change. The
        # resulting popup lands on auth.manheim.com or Manheim's
        # "Sorry, condition reports are not available right now" error
        # page. Without a popup listener those tabs leaked — 4 retries ×
        # 1 orphan each = the 5-tab pile-up behind the Ferrari
        # ZFF96NMA4N0272307 incident. We now attach a context-level
        # popup listener for the whole retry window, inspect each new
        # page for auth/unavailable signals, raise
        # ManheimAuthRedirectError on first sight, and close every
        # orphan in the finally block.
        max_attempts = 2
        last_error: Exception | None = None
        collected_popups: list[Page] = []
        claimed_page: Page | None = None
        context = source_page.context

        def _on_popup(new_page: Page) -> None:
            collected_popups.append(new_page)

        context.on("page", _on_popup)
        try:
            for attempt in range(max_attempts):
                # Re-resolve the locator on every attempt — a previous click
                # may have rebuilt the DOM and the prior handle could be stale.
                locator = self._find_condition_report_locator(source_page)
                if locator is None:
                    LOGGER.warning(
                        "Condition report locator not found on OVE detail page (attempt %s/%s)",
                        attempt + 1,
                        max_attempts,
                    )
                    source_page.wait_for_timeout(1500)
                    continue
                try:
                    cr_page = self._click_condition_report_locator(source_page, locator)
                except ManheimAuthRedirectError:
                    # The click DID navigate, but to an auth page. Propagate
                    # immediately — retrying with the same source page will
                    # not fix an auth state problem.
                    raise
                except Exception as click_exc:
                    last_error = click_exc
                    LOGGER.warning(
                        "Condition report click attempt %s/%s raised %s; will retry",
                        attempt + 1,
                        max_attempts,
                        click_exc,
                    )
                    source_page.wait_for_timeout(1500)
                    self._inspect_cr_popups_for_auth(collected_popups, artifact_dir, href)
                    continue

                # Inspect any popups spawned by the click BEFORE deciding
                # success. Raises ManheimAuthRedirectError if a popup
                # landed on login or the Manheim "not available" page.
                self._inspect_cr_popups_for_auth(collected_popups, artifact_dir, href)

                if cr_page is not None:
                    LOGGER.info(
                        "Opened OVE-internal condition report view (attempt %s/%s, url=%s)",
                        attempt + 1,
                        max_attempts,
                        cr_page.url,
                    )
                    self._raise_if_auth_redirect(cr_page, artifact_dir, "post_click", href)
                    if self._is_cr_unavailable_page(cr_page):
                        raise ManheimAuthRedirectError(
                            "Manheim returned 'condition reports are not available' for "
                            f"intended_href={href}; treating as stale-session auth failure."
                        )
                    claimed_page = cr_page
                    return cr_page
                LOGGER.warning(
                    "Condition report click attempt %s/%s produced no navigation; will retry",
                    attempt + 1,
                    max_attempts,
                )
                source_page.wait_for_timeout(1500)

            # All click attempts exhausted. We deliberately do NOT fall back
            # to a direct goto on the raw Manheim URL. Capture a debug
            # snapshot of the source page so a follow-up investigation can
            # see why the click never produced a navigation, then raise so
            # the lease retries.
            try:
                artifact_dir.mkdir(parents=True, exist_ok=True)
                (artifact_dir / "cr-click-failed-source.html").write_text(
                    source_page.content(), encoding="utf-8"
                )
                source_page.screenshot(
                    path=str(artifact_dir / "cr-click-failed-source.png"),
                    full_page=True,
                )
            except Exception as snap_exc:
                LOGGER.warning("Could not capture cr-click-failed debug snapshot: %s", snap_exc)
            raise ConditionReportClickFailedError(
                f"Could not open OVE condition report after {max_attempts} click attempts; "
                f"intended_href={href}; last_error={last_error}"
            )
        finally:
            try:
                context.remove_listener("page", _on_popup)
            except Exception:
                pass
            for popup in collected_popups:
                if popup is claimed_page:
                    continue
                self._close_page(popup)

    def _inspect_cr_popups_for_auth(
        self,
        popups: list[Page],
        artifact_dir: Path,
        href: str,
    ) -> None:
        """Raise ManheimAuthRedirectError if any popup opened by the CR
        click landed on an auth page or the Manheim 'not available' error
        page. These pages are the signature of a stale OVE SSO and
        retrying cannot recover from them — the user must re-login Chrome."""
        for popup in popups:
            try:
                if popup.is_closed():
                    continue
            except Exception:
                continue
            if self._is_manheim_auth_page(popup):
                self._raise_if_auth_redirect(popup, artifact_dir, "cr_popup_auth", href)
            if self._is_cr_unavailable_page(popup):
                raise ManheimAuthRedirectError(
                    "Manheim CR popup showed 'condition reports are not available' for "
                    f"intended_href={href}; treating as stale-session auth failure."
                )

    def _is_manheim_auth_page(self, page: Page) -> bool:
        try:
            url = (page.url or "").lower()
        except Exception:
            url = ""
        if "auth.manheim.com" in url or "/as/authorization" in url:
            return True
        try:
            title = (page.title() or "").strip().lower()
        except Exception:
            title = ""
        if title == "sign in" or title == "log in" or title == "login":
            return True
        return False

    def _is_cr_unavailable_page(self, page: Page) -> bool:
        """Detect Manheim's 'Sorry, condition reports are not available
        right now' stale-session error page. This is the wording Manheim
        shows when insightcr.manheim.com is reached with a bad / expired
        SSO token. Retrying cannot recover it; the user must re-login."""
        try:
            text = page.evaluate(
                "() => (document.body && document.body.innerText ? document.body.innerText : '').toLowerCase()"
            )
        except Exception:
            return False
        if not isinstance(text, str):
            return False
        head = text[:2000]
        if "condition reports are not available" in head:
            return True
        # Be conservative on fuzzier matches: require 'condition report'
        # AND 'not available' AND one of the apology/retry cues, all in
        # the top of the page, to avoid misfiring on listings that
        # legitimately mention 'Data Not Available' in a vehicle-history
        # section.
        if (
            "condition report" in head
            and "not available" in head
            and ("sorry" in head or "try again" in head or "right now" in head)
        ):
            return True
        return False

    def _raise_if_auth_redirect(
        self,
        page: Page,
        artifact_dir: Path,
        navigation_label: str,
        intended_href: str,
    ) -> None:
        if not self._is_manheim_auth_page(page):
            return
        captured_url = ""
        captured_title = ""
        try:
            captured_url = page.url or ""
        except Exception:
            pass
        try:
            captured_title = page.title() or ""
        except Exception:
            pass
        # Save the auth page DOM and a screenshot so a follow-up debugging
        # session can see exactly which OAuth step failed (state token
        # missing, cookie expired, referer wrong, etc).
        try:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            html_path = artifact_dir / f"auth-redirect-{navigation_label}.html"
            screenshot_path = artifact_dir / f"auth-redirect-{navigation_label}.png"
            html_path.write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(screenshot_path), full_page=True)
            LOGGER.error(
                "Manheim auth redirect detected during %s; saved debug artifacts to %s and %s",
                navigation_label,
                html_path,
                screenshot_path,
            )
        except Exception as artifact_exc:
            LOGGER.error(
                "Manheim auth redirect detected during %s; failed to save debug artifact: %s",
                navigation_label,
                artifact_exc,
            )
        raise ManheimAuthRedirectError(
            f"Condition report navigation ({navigation_label}) landed on Manheim auth page "
            f"instead of {intended_href}: url={captured_url} title={captured_title!r}"
        )

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

    # Hash-fragment route the OVE webapp navigates to when the user clicks
    # the condition-report link from the detail page. The {vin} segment is
    # the listing's VIN, OVE is the source platform marker, and
    # conditionInformation is the route name. The fragment is what the
    # OVE React router watches; window.location only updates the hash, so
    # waiting on full navigation events does not work — we have to poll
    # the URL fragment instead.
    _CR_HASH_ROUTE_RE = re.compile(
        r"#/details/[^/]+/[A-Z]+/conditionInformation", re.IGNORECASE
    )

    def _click_condition_report_locator(self, page: Page, locator: Locator) -> Page | None:
        # The OVE detail page handles the CR-link click client-side via its
        # React router. There is NO popup, NO new tab, NO OAuth handshake
        # the scraper can intercept — only an in-page hash-route change to
        # #/details/{vin}/OVE/conditionInformation. The previous code spent
        # 15 seconds waiting for a popup that never arrived, then fell into
        # a buggy fallback. We now skip the popup wait entirely and watch
        # for the hash-route navigation directly.
        original_url = page.url or ""
        try:
            self._click_locator(locator)
        except Exception as exc:
            LOGGER.warning("CR locator click raised: %s", exc)
            return None

        # Poll for the hash-route navigation. We do NOT use page.expect_navigation
        # here because pure hash changes do not always fire navigation events
        # in Playwright reliably across Chromium versions; polling is the
        # robust path. Total budget: 20 seconds.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            try:
                current_url = page.url or ""
            except Exception:
                current_url = ""
            if self._CR_HASH_ROUTE_RE.search(current_url):
                # Hash route confirmed. Give the OVE React app a moment to
                # mount the CR view's container, then return.
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except PlaywrightTimeoutError:
                    pass
                return page
            page.wait_for_timeout(250)

        # Hash route never appeared. Maybe the click did nothing, or maybe
        # the click navigated SOMEWHERE other than the OVE-internal CR view
        # (e.g., a popup or auth redirect). Check page.url one more time —
        # if it changed at all, surface that to the caller so the auth
        # detector can flag it; otherwise return None to trigger the retry.
        try:
            final_url = page.url or ""
        except Exception:
            final_url = ""
        if final_url and final_url != original_url:
            LOGGER.warning(
                "CR click navigated to unexpected URL %s (expected hash route, original=%s)",
                final_url,
                original_url,
            )
            # Returning the page lets the caller's auth-redirect check fire
            # if we landed on a Manheim sign-in page.
            return page
        return None

    def _wait_for_cr_link_to_render(self, detail_page: Page, *, timeout_ms: int = 8000) -> bool:
        """Wait for any of the known CR link selectors to be present in the DOM.

        The OVE React app mounts the CR link element AFTER the initial detail
        panel render. Calling DETAIL_EXTRACTION_SCRIPT before the link
        appears causes condition_report_link to come back None, which then
        cascades into capture_condition_report being skipped entirely
        (link present=False) and the worker pushing a payload with no CR.

        Returns True if at least one CR-link selector matched within the
        timeout, False otherwise. Does NOT raise on timeout — VINs that
        legitimately have no CR should still produce a usable listing
        snapshot. The pre-push validator (P1a rule 3) and the gallery-stable
        wait (P2-C) gate the actual data quality downstream.
        """
        cr_selectors = [
            "a[data-test-id='condition-report']",
            "a.VehicleReportLink__condition-report-link",
            "a[href*='insightcr.manheim.com']",
            "a[href*='inspectionreport.manheim.com']",
            "a[href*='content.liquidmotors.com/IR/']",
            "a[href*='ECR2I.htm']",
        ]
        joined = ", ".join(cr_selectors)
        try:
            detail_page.wait_for_selector(joined, state="attached", timeout=timeout_ms)
            LOGGER.info("Detail panel CR link is present in the DOM; proceeding with extraction")
            return True
        except PlaywrightTimeoutError:
            LOGGER.warning(
                "Detail panel CR link did not render within %sms; extraction will proceed without it. "
                "Pre-push validator will reject the result if the captured data looks corrupt.",
                timeout_ms,
            )
            return False
        except Exception as exc:
            LOGGER.warning("CR link wait raised unexpectedly: %s", exc)
            return False

    def _wait_for_listing_gallery_to_render(
        self, detail_page: Page, *, timeout_ms: int = 12000
    ) -> bool:
        """Wait for the OVE detail panel's SimpleViewer image gallery to load.

        The OVE detail panel embeds a SimpleViewer carousel
        (`div.svfy_carousel` + `span.svfy_count` showing "1 of N") that
        lazy-loads after the initial React render. The number of Manheim
        CDN <img> elements present in the panel HTML jumps from 1 (just
        the hero) to 10–35 (full gallery) once SimpleViewer initializes.

        Wait for either signal:
          (a) `span.svfy_count` element appears (the "1 of N" widget), or
          (b) at least 5 Manheim CDN <img> elements are visible

        Polls every 500ms. Does NOT raise on timeout — vehicles whose
        listing panel legitimately has no gallery (rare) should still
        produce a payload, and the pre-push contract validator will
        refuse the result if the image count is too low.

        Returns True if either signal fired within the timeout.
        """
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        last_count = 0
        while time.monotonic() < deadline:
            try:
                state = detail_page.evaluate(
                    """
                    () => {
                      const svfy = document.querySelector("span.svfy_count");
                      const cdnImgs = Array.from(document.querySelectorAll("img"))
                        .map((img) => img.currentSrc || img.src || "")
                        .filter((src) => /images\\.cdn\\.manheim\\.com/i.test(src));
                      const uniqueCdn = new Set(
                        cdnImgs.map((src) => src.replace(/[?#].*$/, ""))
                      );
                      return {
                        svfy_text: svfy ? (svfy.textContent || "").trim() : null,
                        cdn_count: uniqueCdn.size,
                      };
                    }
                    """
                )
            except Exception as exc:
                LOGGER.debug("Listing gallery poll evaluate failed: %s", exc)
                detail_page.wait_for_timeout(500)
                continue
            if not isinstance(state, dict):
                detail_page.wait_for_timeout(500)
                continue
            cdn_count = int(state.get("cdn_count") or 0)
            last_count = cdn_count
            svfy_text = state.get("svfy_text")
            if svfy_text or cdn_count >= 5:
                LOGGER.info(
                    "OVE listing gallery rendered (svfy=%r, cdn_count=%s)",
                    svfy_text,
                    cdn_count,
                )
                # One more 500ms settle pause so any in-flight image src
                # assignments finish before _extract_page_image_urls runs.
                detail_page.wait_for_timeout(500)
                return True
            detail_page.wait_for_timeout(500)
        LOGGER.warning(
            "OVE listing gallery did not render within %sms (last cdn_count=%s); "
            "extraction will proceed and the pre-push validator will gate the result",
            timeout_ms,
            last_count,
        )
        return False

    def _wait_for_cr_gallery_stable(self, page: Page) -> None:
        """Wait for the OVE-internal CR image gallery to finish loading.

        Polls every 500ms for up to 30 seconds. Returns as soon as:
          (a) the count of Manheim-CDN <img> elements is stable across two
              consecutive polls, AND
          (b) if the page text contains a gallery widget hint like "1 of N"
              or "VIEWING ALL N", the count is at least N.

        We do NOT raise on timeout — the pre-push validator (rule 3 in
        deep_scrape._validate_cr_payload_or_raise) will refuse the push if
        the gallery hint says N images and we captured zero. Returning
        without an exception lets the snapshot proceed and lets that
        downstream guard render a clean failure log instead of a Playwright
        timeout traceback.
        """
        deadline = time.monotonic() + 30.0
        previous_count = -1
        stable_polls = 0
        target_count: int | None = None
        last_count = 0
        while time.monotonic() < deadline:
            try:
                count_payload = page.evaluate(
                    """
                    () => {
                      const imgs = Array.from(document.querySelectorAll("img"))
                        .filter((img) => /images\\.cdn\\.manheim\\.com/i.test(img.currentSrc || img.src || ""));
                      const text = document.body ? document.body.innerText : "";
                      return { count: imgs.length, text: text || "" };
                    }
                    """
                )
            except Exception as exc:
                LOGGER.debug("CR gallery poll evaluate failed: %s", exc)
                page.wait_for_timeout(500)
                continue
            if not isinstance(count_payload, dict):
                page.wait_for_timeout(500)
                continue
            count = int(count_payload.get("count") or 0)
            last_count = count
            if target_count is None:
                text = str(count_payload.get("text") or "")
                target_count = self._extract_gallery_target_count(text)
                if target_count is not None:
                    LOGGER.info(
                        "CR gallery target count detected from page text: %s",
                        target_count,
                    )
            if count == previous_count and count > 0:
                stable_polls += 1
            else:
                stable_polls = 1 if count > 0 else 0
            previous_count = count
            target_satisfied = target_count is None or count >= target_count
            if stable_polls >= 2 and target_satisfied:
                LOGGER.info(
                    "CR gallery stable at %s images (target=%s)",
                    count,
                    target_count if target_count is not None else "unknown",
                )
                return
            page.wait_for_timeout(500)
        LOGGER.warning(
            "CR gallery did not stabilize within 30s (last_count=%s, target=%s); "
            "snapshot will proceed and the pre-push validator will gate the result",
            last_count,
            target_count if target_count is not None else "unknown",
        )

    @staticmethod
    def _extract_gallery_target_count(text: str) -> int | None:
        """Pull the expected image count out of the OVE CR gallery widget.

        The widget renders strings like "1 of 17" or "VIEWING ALL 17". We
        prefer the "of N" form because it is the most universally present
        marker in the OVE CR view. The "VIEWING ALL N" form appears only
        after the user toggles the gallery to show every image.
        """
        if not text:
            return None
        # "1 of 17" — the "of N" widget. Constrain N to >= 2 because a
        # single-image listing legitimately has no gallery counter.
        m = re.search(r"\b\d+\s*of\s*([2-9]|\d{2,})\b", text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        # "VIEWING ALL 17"
        m = re.search(r"viewing\s+all\s+([2-9]|\d{2,})\b", text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        return None

    def _snapshot_page(self, page: Page, base_path: Path) -> dict[str, Any]:
        html_path = base_path.with_suffix(".html")
        screenshot_path = base_path.with_suffix(".png")
        # Always capture the OUTER page's HTML and full-page screenshot for
        # debugging artifacts — those need to reflect what a human would see
        # when opening the OVE webapp at this point.
        page_html = page.content()
        html_path.write_text(page_html, encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)

        # Find the Manheim CR iframe if present. The OVE-internal viewer wraps
        # insightcr.manheim.com inside an <iframe>, and the actual CR text/DOM
        # lives inside that frame, NOT in the outer OVE document. Reading
        # body.innerText on the outer page returns OVE chrome plus the listing
        # JSON dump, which the per-family parsers in cr_parsers.py cannot
        # recognize as CR content. Switching the snapshot target to the iframe
        # lets the existing _parse_manheim_insightcr parser see the section
        # markers it was designed for ("ANNOUNCEMENTS & COMMENTS",
        # "TIRES AND WHEELS", "EQUIPMENT & OPTIONS", etc.).
        #
        # The direct-goto path (inspectionreport.manheim.com / Manheim Express)
        # opens a top-level Manheim page with no iframe, so this lookup
        # returns None and we snapshot the outer page as before — that path
        # was already producing clean CR text on the WAUE Audi (03-19) and
        # 5N1B Nissan (03-26) captures.
        cr_frame = self._find_manheim_cr_frame(page)
        snapshot_target: Any = cr_frame if cr_frame is not None else page
        if cr_frame is not None:
            LOGGER.info(
                "Snapshotting CR from Manheim iframe (url=%s) instead of outer OVE page",
                getattr(cr_frame, "url", "?"),
            )

        payload = snapshot_target.evaluate(
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
        payload["images"] = self._extract_page_image_urls(snapshot_target, page_html)
        payload["captured_from_iframe"] = cr_frame is not None
        return payload

    # Specific hostnames that legitimately render CR content inside an
    # iframe. Must be exact subdomain matches to avoid false positives
    # from tracking pixels (e.g. manheim.demdex.net matched the old
    # broad "manheim.com" check and returned a tracking iframe instead
    # of the CR, producing a payload with listing JSON as raw_text
    # instead of inspection report text).
    _CR_FRAME_HOSTS = (
        "inspectionreport.manheim.com",
        "insightcr.manheim.com",
        "mmsc400.manheim.com",
        "content.liquidmotors.com",
    )

    # Hosts that should NEVER be selected as the CR snapshot target.
    _CR_FRAME_EXCLUDE = (
        "auth.manheim.com",
        "demdex.net",
        "omtrdc.net",
        "assets.adobedtm.com",
    )

    def _find_manheim_cr_frame(self, page: Page) -> Any:
        """Locate the iframe rendering the Manheim/liquidmotors CR, if present.

        Returns a Playwright Frame whose URL is on a known CR-provider host,
        or None when no such frame exists (e.g. the direct-goto path where
        the CR is the top-level document).
        """
        try:
            frames = list(page.frames)
        except Exception:
            return None
        try:
            outer_url = (page.url or "").lower()
        except Exception:
            outer_url = ""
        for frame in frames:
            try:
                frame_url = (frame.url or "").lower()
            except Exception:
                continue
            if not frame_url:
                continue
            # Skip known non-CR iframes (tracking, auth, analytics)
            if any(excluded in frame_url for excluded in self._CR_FRAME_EXCLUDE):
                continue
            # Skip the outer OVE document itself — page.frames includes the
            # main frame as the first entry.
            if frame_url == outer_url:
                continue
            if any(host in frame_url for host in self._CR_FRAME_HOSTS):
                return frame
        return None

    def _extract_ove_listing_json(self, page: Page) -> dict[str, Any] | None:
        """Pull the OVE listing JSON object embedded in the detail page.

        The OVE detail page renders the full listing data as a JSON blob
        inside a stockwave-info element. The blob is structurally stable
        (it's the OVE backend's serialized listing object) and contains:
        announcementsEnrichment, autocheck (owners/accidents),
        conditionGrade, conditionReportUrl, installedEquipment, gallery
        image URLs, and many more fields. Parsing this object directly
        gives us a CR data source that does NOT depend on regex parsing
        of rendered HTML, which is what was failing intermittently when
        Manheim migrated CR delivery between providers.

        Returns the parsed dict on success, or None when no listing JSON
        was found in the DOM (rare — happens for VINs that fail to load
        the OVE detail panel at all).
        """
        try:
            blobs = page.evaluate(
                """
                () => Array.from(
                  document.querySelectorAll("[data-test-id='stockwave-info'], .stockwave-vehicle-info")
                )
                  .map((node) => node.textContent || "")
                  .filter((text) => text && text.trim().startsWith("{"))
                """
            )
        except Exception as exc:
            LOGGER.warning("Failed to query OVE listing JSON blobs: %s", exc)
            return None
        if not isinstance(blobs, list):
            return None
        for blob in blobs:
            if not isinstance(blob, str):
                continue
            try:
                data = json.loads(blob)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(data, dict) and (
                data.get("source") == "OVE" or "conditionReportUrl" in data or "announcementsEnrichment" in data
            ):
                return data
        return None

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

        # Liquid Motors CR pages render their gallery as <ul id="lightgallery">
        # with one <li data-src="https://assets.cai-media-management.com/.../{uuid}.jpg">
        # per image (verified 2026-04-09 against the live CR for VIN
        # 1N4BL4EV2NN423240). The <img> tags inside the gallery are tiny
        # placeholder thumbnails on a different host; the full-resolution
        # source URLs only exist as data-src attributes. Read them
        # explicitly. Targets:
        #   #lightgallery        — all photos
        #   #lightgallery_normal — non-damage photos
        #   #lightgallery_damage — damage-only photos
        # Reading all three (with dedup downstream via unique_urls) gives
        # us the complete set without missing damage-only photos on CRs
        # where the lists are split.
        try:
            lightgallery_urls = page.evaluate(
                """
                () => {
                  const selectors = [
                    "#lightgallery li[data-src]",
                    "#lightgallery_normal li[data-src]",
                    "#lightgallery_damage li[data-src]",
                  ];
                  const urls = [];
                  for (const sel of selectors) {
                    document.querySelectorAll(sel).forEach((node) => {
                      const v = node.getAttribute("data-src");
                      if (v && /^https?:/i.test(v)) urls.push(v);
                    });
                  }
                  return urls;
                }
                """
            )
            if isinstance(lightgallery_urls, list):
                image_urls.extend(
                    str(value) for value in lightgallery_urls if isinstance(value, str)
                )
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

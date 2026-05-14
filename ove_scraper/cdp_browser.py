from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
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

from ove_scraper import auth_lockout
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
    # Process-wide timestamp of the most recent auto-click login
    # recovery attempt (success OR failure). None means "never
    # attempted in this process". Used together with
    # _AUTO_LOGIN_COOLDOWN below to gate the next attempt.
    #
    # Guardrail history: the prior autonomous login feature (commit
    # 1f1d8ee, reverted in eaf37cf 2026-04-21) retried the login in a
    # tight loop on failure and Manheim locked the OVE account. The
    # original fix was a hard "one click per Python process, ever"
    # boolean flag. That worked for short-lived processes but failed
    # for long-running ones: the 2026-04-30 22:12 incident saw a
    # 30-hour-uptime process whose flag had been consumed earlier in
    # its lifetime, leaving NO recovery path when a real auth event
    # finally arrived. Manheim ended up SMS-challenging the account
    # because the kill+relaunch loop tore through Chrome's session
    # state with no auto-recovery available.
    #
    # Fix 2 (2026-04-30): replace the boolean with a timestamp +
    # cooldown. After the cooldown window elapses, a new click is
    # allowed. The cross-process click ledger
    # (ove_scraper.auth_lockout) continues to enforce the absolute
    # rate limits (3 clicks / 10 min, etc.), so this CANNOT create a
    # click storm — at worst ~4 clicks per 24 hours.
    #
    # Cooldown choice: 6 hours. Aligns with the default Manheim
    # account-lock cooldown so a process whose lockout just expired
    # doesn't immediately attempt another click.
    _auto_login_last_attempt_at: datetime | None = None
    _AUTO_LOGIN_COOLDOWN: timedelta = timedelta(hours=6)

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        # Optional admin notifier wired in by build_runtime (main.py) after
        # construction. Used to fire distinct-subject alerts from deep
        # in the recovery path — e.g. when Chrome's password manager has
        # no saved credentials and auto-recovery is therefore disabled.
        # None in unit tests / standalone usage; gated everywhere by
        # `if self._notifier is not None`.
        self._notifier: Any = None
        # Persistent dedicated keepalive page (2026-05-04 fix). When
        # `settings.keepalive_persistent_tab` is True, touch_session
        # opens ONE worker tab on first call, reuses it across ticks,
        # and never closes it. Real ops continue spawning their own
        # worker tabs from the seed tab's context — they never collide.
        # The sweep_orphan_tabs identity-check skips this page.
        self._keepalive_page: Page | None = None
        # Per-instance decay-detection deque (last 10 cards_render_ms
        # samples). Decay condition: 3 of last 5 ticks > 12000ms.
        # When tripped, raise SavedSearchPageEmpty to route through the
        # existing cookie-clear + recover_browser_session path in
        # main.py:run_browser_operation.
        self._keepalive_render_history: list[int] = []
        # 2026-05-06 active keepalive state. Round-robin index into the
        # cached saved-search names tuple — incremented each tick so we
        # rotate through the user's saved searches over multiple ticks.
        self._keepalive_search_index: int = 0
        # Cached saved-search names so the keepalive doesn't have to
        # re-discover via the saved-searches list every tick. Refreshed
        # if list_saved_searches() is called by sync (it overwrites this
        # cache as a side-effect).
        self._cached_saved_search_names: tuple[str, ...] = ()

    def set_notifier(self, notifier: Any) -> None:
        """Attach an AdminNotifier so the recovery path can fire alerts
        about states the caller can't observe (e.g. credentials-not-saved
        detected during a single-shot login click). Safe to call multiple
        times; the most recent notifier wins."""
        self._notifier = notifier

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
        fields aren't pre-filled, process-wide cooldown active, click
        failed, or we're still on login after waiting).

        Click rate limiting is layered:

          1. In-process timestamp + cooldown: at most one click per
             cls._AUTO_LOGIN_COOLDOWN (default 6h) within a single
             Python process. Replaces the original "one click ever per
             process" boolean — see the 2026-04-30 22:12 incident
             write-up for why a hard one-shot was too strict for
             long-uptime processes.

          2. Cross-process click ledger
             (ove_scraper.auth_lockout.record_login_click): rate-
             limits clicks across the entire scraper deployment
             regardless of how many Python processes have come and
             gone. 3 clicks / 10 min, 5 / 60 min, 8 / 4 h are the
             current thresholds.

          3. Manheim account-lock detection: if a previous click
             produced an account-locked page, the lockout file is
             set and this method refuses for the lockout duration.

        Together these prevent the original tight-loop revert
        (eaf37cf) and the long-uptime starvation (2026-04-30) from
        recurring. Never types credentials.
        """
        # Layer 1: in-process cooldown. Check BEFORE consulting the
        # disk-backed lockout because a No-op return is cheaper than
        # an I/O read on the keepalive hot path.
        last = type(self)._auto_login_last_attempt_at
        if last is not None:
            now = datetime.now(timezone.utc)
            elapsed = now - last
            cooldown = type(self)._AUTO_LOGIN_COOLDOWN
            if elapsed < cooldown:
                LOGGER.info(
                    "Skipping auto-login click: last attempt %s ago (cooldown %s).",
                    elapsed, cooldown,
                )
                return False

        # Layer 2 + 3: cross-process lockout check. The in-process
        # cooldown above is reset every time the launcher loop
        # respawns Python, which is how 12-360 login-click attempts
        # per hour became possible during a sustained auth failure
        # (the 2026-04-28 incident). The disk-backed lockout is the
        # belt-and-suspenders defense: a click ledger written to
        # state/auth_lockout.json that survives process restarts and
        # rate-limits clicks across the entire scraper deployment.
        # Refusing here is the safest possible behavior — at worst
        # we leave the scraper unable to auto-recover and the
        # operator must log in manually, which is FINE compared to
        # triggering another Manheim account lock.
        try:
            lockout_state = auth_lockout.get_state(
                self.settings.artifact_dir, port=self.settings.chrome_debug_port,
            )
        except Exception as exc:
            LOGGER.warning("Auth-lockout state read failed (treating as unblocked): %s", exc)
            lockout_state = None
        if lockout_state is not None and lockout_state.blocked:
            LOGGER.error(
                "Auto-login click REFUSED by disk-backed lockout: %s",
                lockout_state.reason,
            )
            # Do NOT mark the in-process timestamp here. We didn't
            # actually click; the lockout will be re-checked on the
            # next legitimate call (e.g. after the lockout expires)
            # and the click can proceed then. Marking would waste the
            # in-process cooldown budget for no reason.
            return False

        # Autofill-race retry (2026-05-06 fix). Chrome's password-manager
        # autofill happens asynchronously after page load — typically
        # 200-2000ms but can be slower if the password manager is busy
        # or the page just rendered the form. Pre-fix this check fired
        # ONCE immediately and any miss got the operator a misleading
        # "auto-recovery disabled" email even though Chrome had the
        # password and would have filled it shortly after. Now: poll
        # up to ~5s for the field to be populated, focusing the field
        # on each iteration to nudge Chrome to autofill.
        filled = False
        autofill_check_start = time.monotonic()
        autofill_deadline = autofill_check_start + 5.0
        autofill_attempts = 0
        while time.monotonic() < autofill_deadline:
            autofill_attempts += 1
            try:
                filled = page.evaluate(
                    """
                    () => {
                        const pw = document.querySelector("input[type='password']");
                        if (!(pw instanceof HTMLInputElement)) return false;
                        // Focusing the field is a hint to Chrome's password
                        // manager to autofill if it hasn't already. Harmless
                        // if already filled.
                        try { pw.focus(); } catch (e) {}
                        return pw.value.length > 0;
                    }
                    """
                )
            except Exception as exc:
                LOGGER.warning("Auto-login pre-check (password-populated) failed: %s", exc)
                return False
            if filled:
                break
            page.wait_for_timeout(400)
        if filled and autofill_attempts > 1:
            LOGGER.info(
                "Auto-login pre-check: password populated after %d attempt(s) "
                "(%dms wait for Chrome autofill)",
                autofill_attempts,
                int((time.monotonic() - autofill_check_start) * 1000),
            )
        if not filled:
            LOGGER.warning(
                "Auto-login skipped: password field is empty — Chrome's saved "
                "credentials are not available. Operator must log in manually."
            )
            # Consume the in-process cooldown so this same Python process
            # doesn't burn 5s polling for autofill on every recovery
            # attempt for the next 6h. (Cross-process spam is gated by
            # the notifier's own cooldown.) This is the same semantics
            # the original code had, preserved across the 2026-05-07
            # button-not-found bug fix.
            type(self)._auto_login_last_attempt_at = datetime.now(timezone.utc)
            # Loud-failure alert (Fix D, 2026-04-28): empty password field
            # means Chrome has no saved credentials, which permanently
            # disables auto-recovery for the lifetime of this profile.
            # Without this alert the operator only sees per-VIN auth_lost
            # emails that don't say "your auto-recovery is broken at the
            # root" — exactly the failure mode that produced the 29-tab
            # auth-storm incident. Notifier has its own cooldown so this
            # cannot spam.
            if self._notifier is not None:
                try:
                    self._notifier.notify_credentials_not_saved(
                        port=self.settings.chrome_debug_port,
                        logger=LOGGER,
                    )
                except Exception as alert_exc:
                    LOGGER.warning(
                        "Failed to fire credentials-not-saved alert: %s",
                        alert_exc,
                    )
            return False

        # Locator chain ordered most-specific → most-generic. PingFederate
        # (Manheim's auth provider, used by both OVE and the AutoCheck
        # OAuth flow) renders the submit as <a id="signOnButton"> styled
        # as a button — not <button type="submit"> — and labels it
        # "Sign On" not "Sign In". The 2026-05-07 incident missed every
        # AutoCheck VIN for 6h because the original 3 locators only
        # matched OVE's main login form. Keep both flows covered.
        submit_locators = [
            page.locator("#signOnButton").first,
            page.locator("div.ping-buttons a.ping-button, div.ping-buttons button").first,
            page.locator("button[type='submit']").first,
            page.get_by_role(
                "button",
                name=re.compile(r"^\s*(sign\s*(in|on)|log\s*in|login)\s*$", re.I),
            ).first,
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
            # Bug fix (2026-05-07 AutoCheck OAuth incident): pre-fix the
            # in-process timestamp + cross-process ledger were both
            # consumed BEFORE this point, so a selector miss on a new
            # auth-page variant silently disabled auto-login for 6h.
            # By moving the timestamp/ledger writes to AFTER button-
            # found, "button not found" is now a true no-op: log it and
            # try again on the next call. If selectors are genuinely
            # broken, the operator sees the warning and ships a fix
            # without losing the cooldown budget in the meantime.
            LOGGER.warning(
                "Auto-login skipped: could not locate a Sign In button on the login page (url=%s)",
                page.url,
            )
            return False

        # Button found → this is the point of no return. Mark the
        # cooldown timestamp + ledger entry NOW, before the click, so
        # any exception during the click (timeout, navigation race,
        # account-lock detection) still counts as one consumed attempt.
        # See the layered-rate-limiting docstring above for why both
        # in-process and disk-backed gates are needed.
        type(self)._auto_login_last_attempt_at = datetime.now(timezone.utc)
        try:
            post_record_state = auth_lockout.record_login_click(
                self.settings.artifact_dir, port=self.settings.chrome_debug_port,
            )
            if post_record_state.blocked:
                LOGGER.error(
                    "Auto-login click REFUSED post-record (rate limit just tripped): %s",
                    post_record_state.reason,
                )
                return False
        except Exception as ledger_exc:
            LOGGER.error(
                "Auth-lockout ledger write FAILED — proceeding without rate-limit "
                "protection (this is dangerous; check disk permissions): %s",
                ledger_exc,
            )

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
                # Account-locked detection (2026-04-28): if the page
                # rendered Manheim's account-locked error page after
                # our click, we have triggered a lockout (or one was
                # already in effect). Record it in the disk-backed
                # state so EVERY future Python process refuses to
                # click again until the cooldown expires. This is the
                # hardest stop we have against re-locking the account.
                if self._is_manheim_account_locked_page(page):
                    page_text_head = ""
                    try:
                        page_text_head = (page.evaluate(
                            "() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 500)"
                        ) or "").strip()
                    except Exception:
                        pass
                    LOGGER.error(
                        "MANHEIM ACCOUNT LOCKED detected after auto-login click. url=%s text_head=%r",
                        page.url, page_text_head,
                    )
                    try:
                        auth_lockout.record_manheim_account_locked(
                            self.settings.artifact_dir,
                            port=self.settings.chrome_debug_port,
                            reason=(
                                f"detected on port {self.settings.chrome_debug_port} "
                                f"after auto-login click; first 500 chars of page: "
                                f"{page_text_head}"
                            ),
                        )
                    except Exception as record_exc:
                        LOGGER.error("Failed to record account-lock in lockout state: %s", record_exc)
                    if self._notifier is not None:
                        try:
                            self._notifier.notify_manheim_account_locked(
                                port=self.settings.chrome_debug_port,
                                reason=page_text_head or "Manheim account-locked page detected after auto-login click",
                                logger=LOGGER,
                            )
                        except Exception as alert_exc:
                            LOGGER.warning("Failed to fire account-locked alert: %s", alert_exc)
                    return False
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

    # Outcome tags for the structured KEEPALIVE_TICK log line. Kept as a
    # closed set (no string interpolation in the value) so a future
    # log-aggregation rule can match on `outcome=<exact tag>` without
    # accidentally swallowing a free-form message.
    _KEEPALIVE_OUTCOME_OK = "ok"
    _KEEPALIVE_OUTCOME_GOTO_TIMEOUT = "goto_timeout"
    _KEEPALIVE_OUTCOME_LOGIN_OK = "landed_on_login_then_clicked_ok"
    _KEEPALIVE_OUTCOME_LOGIN_BLOCKED = "landed_on_login_then_click_blocked_by_lockout"
    _KEEPALIVE_OUTCOME_LOGIN_FAILED = "landed_on_login_then_click_failed"
    _KEEPALIVE_OUTCOME_CONNECT_FAILED = "connect_failed"
    # 2026-05-04 keepalive durability fix outcomes. Emitted by the
    # persistent-tab keepalive when it actually verifies render and
    # observes a session that's accepting cookies but returning empty
    # (cards_empty) or hung (cards_did_not_render).
    _KEEPALIVE_OUTCOME_CARDS_EMPTY = "cards_empty"
    _KEEPALIVE_OUTCOME_CARDS_DID_NOT_RENDER = "cards_did_not_render"

    # Decay-detection thresholds. 3 of the last 5 ticks with
    # cards_render_ms > 12_000 indicates the session is degrading even
    # though individual ticks still complete. Triggers cookie-clear +
    # browser recovery via the existing SavedSearchPageEmpty handler in
    # main.py:run_browser_operation.
    _KEEPALIVE_DECAY_RENDER_MS_THRESHOLD = 12_000
    _KEEPALIVE_DECAY_WINDOW = 5
    _KEEPALIVE_DECAY_HITS_REQUIRED = 3
    _KEEPALIVE_RENDER_HISTORY_MAX = 10

    def touch_session(self) -> None:
        # Dispatch hierarchy (2026-05-06):
        #   keepalive_active=True (DEFAULT)
        #     → active keepalive: rotate through saved searches, click
        #       pagination, linger like a real user. Generates the
        #       GraphQL query traffic that keeps OAuth Bearer tokens
        #       warm. This is the response to the 401-on-graphql class
        #       of failure that produced partial CSVs and missing VINs.
        #   keepalive_active=False AND keepalive_persistent_tab=True
        #     → persistent-tab keepalive (legacy 2026-05-04 design).
        #       Fast and gentle but didn't simulate enough activity.
        #   keepalive_active=False AND keepalive_persistent_tab=False
        #     → worker-tab keepalive (oldest design, kept for
        #       defensive emergency rollback via KEEPALIVE_ACTIVE=false
        #       and KEEPALIVE_PERSISTENT_TAB=false env vars).
        use_active = bool(getattr(self.settings, "keepalive_active", True))
        use_persistent = bool(getattr(self.settings, "keepalive_persistent_tab", True))
        if use_active:
            self._touch_session_active()
        elif use_persistent:
            self._touch_session_persistent_tab()
        else:
            self._touch_session_worker_tab()

    # ------------------------------------------------------------------
    # Active keepalive (2026-05-06)
    # ------------------------------------------------------------------

    # Pagination button selectors, tried in order. OVE's UI may
    # vary; we try multiple before giving up.
    _PAGINATION_NEXT_SELECTORS: tuple[str, ...] = (
        "button[aria-label*='Next' i]:not([disabled])",
        "button[aria-label*='page next' i]:not([disabled])",
        "[data-test-id*='pagination-next']:not([disabled])",
        "[data-test-id='pagination-next-button']:not([disabled])",
        "button:has-text('Next'):not([disabled])",
        "a[aria-label*='Next' i]",
    )

    def _touch_session_active(self) -> None:
        """Active keepalive: rotate through saved searches, navigate
        into ONE search per tick, click through 2-3 pagination pages
        with realistic dwell time on each.

        Why this exists (2026-05-06): the previous persistent-tab
        keepalive just navigated to the saved-searches list page and
        waited 8 seconds. That wasn't generating the GraphQL query
        traffic that real user activity does, and OVE's data-layer
        auth (Bearer access tokens) was expiring silently while page-
        shell cookies stayed valid. Result: HTTP 401 from
        onesearch-api.manheim.com/graphql during exports, producing
        partial CSVs that lost specific VINs (operator caught this
        2026-05-06 evening: 4948-row CSV at 18:33, 4305-row CSV at
        18:57 on the same saved search).

        Active keepalive forces real GraphQL traffic each tick:
          1. Navigate to saved-searches list (cookies)
          2. Click the next saved-search card in the rotation
             (taxonomy + initial result-set GraphQL queries fire)
          3. Wait for vehicle cards to render (proves session works)
          4. Linger keepalive_page_linger_ms (60s default) — like a
             user reading the listings
          5. Click next-page pagination
             (paginated result-set GraphQL fires)
          6. Linger again
          7. Repeat for keepalive_pagination_clicks total clicks
             (default 2)
        Total tick: ~3-4 minutes inside the 5-min interval. Other
        operations (sync, hot-deal, deep-scrape poll) may briefly
        wait on the OveAutomationLock during long ticks; their
        retry/backoff logic absorbs this.

        Tab strategy: reuses the persistent keepalive page if it's
        on ove.com, opens a fresh worker tab otherwise. Shares the
        same `self._keepalive_page` slot as the persistent-tab
        keepalive so sweep_orphan_tabs identity-skip works.

        Recovery: if cards never render after navigating into the
        saved search, raises SavedSearchPageEmpty. With the surgical
        cookie clear shipped earlier today (preserves Manheim device-
        trust), this recovery is now CHEAP — fresh OAuth flow via
        saved credentials, no 2FA challenge.
        """
        t0 = time.monotonic()
        outcome = self._KEEPALIVE_OUTCOME_OK
        url_at_failure: str | None = None
        seed_url: str | None = None
        cards_render_ms: int | None = None
        decay_signal = "none"
        search_visited: str | None = None
        pagination_clicks_made = 0
        try:
            try:
                browser = self._connect_browser()
            except Exception as exc:
                outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
                self._browser = None
                raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc

            # Resolve which saved search to visit this tick. We rotate
            # through whatever names we last cached. If empty, populate
            # by visiting the saved-searches list once.
            search_names = self._cached_saved_search_names
            if not search_names:
                try:
                    search_names = self.list_saved_searches()
                    self._cached_saved_search_names = search_names
                except SavedSearchPageEmpty:
                    outcome = self._KEEPALIVE_OUTCOME_CARDS_EMPTY
                    LOGGER.warning(
                        "Active keepalive: saved-searches list returned empty on port %d; "
                        "session may be stale. Routing through cookie-clear recovery.",
                        self.settings.chrome_debug_port,
                    )
                    raise
                except BrowserSessionError:
                    outcome = self._KEEPALIVE_OUTCOME_CARDS_DID_NOT_RENDER
                    raise

            if not search_names:
                LOGGER.warning(
                    "Active keepalive: no saved searches to rotate through "
                    "(port %d); skipping tick", self.settings.chrome_debug_port,
                )
                return

            # Round-robin: pick the next search and advance the index.
            self._keepalive_search_index = self._keepalive_search_index % len(search_names)
            search_visited = search_names[self._keepalive_search_index]
            self._keepalive_search_index = (self._keepalive_search_index + 1) % len(search_names)
            LOGGER.info(
                "Active keepalive (port %d): visiting saved search %r "
                "(rotation index %d/%d)",
                self.settings.chrome_debug_port, search_visited,
                self._keepalive_search_index or len(search_names),
                len(search_names),
            )

            # Acquire / reuse the keepalive page.
            try:
                seed_url = self._snapshot_first_blank_or_auth_url(browser)
                page = self._get_or_create_keepalive_page(browser)
            except PlaywrightTimeoutError as exc:
                outcome = self._KEEPALIVE_OUTCOME_GOTO_TIMEOUT
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                LOGGER.warning(
                    "Active keepalive seed-page goto timed out (treating as transient): %s",
                    exc,
                )
                return
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                self._keepalive_page = None
                raise

            # Navigate into the chosen saved search.
            try:
                self._open_saved_search(page, search_visited)
            except SavedSearchPageEmpty:
                outcome = self._KEEPALIVE_OUTCOME_CARDS_EMPTY
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                self._close_page(page)  # 2026-05-06 fix: don't leak the orphan tab
                self._keepalive_page = None
                raise
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                self._close_page(page)
                self._keepalive_page = None
                raise

            # Render-verify: confirm VEHICLE cards actually appeared on
            # the search-results page. 2026-05-06 regression: I was
            # using _wait_for_saved_search_cards here, which waits for
            # the SAVED-SEARCH-LIST card selector — that selector
            # never matches on the post-click results page. Every tick
            # timed out and triggered recovery, which leaked tabs.
            # _wait_for_vehicle_results_cards uses the correct
            # vehicle-card selector, same as _trigger_export.
            cards_t0 = time.monotonic()
            try:
                self._wait_for_vehicle_results_cards(page, timeout_ms=60_000)
            except SavedSearchPageEmpty:
                outcome = self._KEEPALIVE_OUTCOME_CARDS_EMPTY
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                self._close_page(page)
                self._keepalive_page = None
                # User confirmed: all saved searches have thousands of
                # listings. If we see empty here, session went bad —
                # raise to trigger surgical cookie-clear recovery.
                raise
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_CARDS_DID_NOT_RENDER
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                self._close_page(page)
                self._keepalive_page = None
                raise
            cards_render_ms = int((time.monotonic() - cards_t0) * 1000)

            # Linger on page 1 like a user reading the listings.
            linger_ms = int(getattr(self.settings, "keepalive_page_linger_ms", 60_000))
            try:
                page.wait_for_timeout(linger_ms)
            except Exception:
                pass

            # Pagination: click "next page" N times with linger between.
            target_clicks = int(getattr(self.settings, "keepalive_pagination_clicks", 2))
            for _ in range(target_clicks):
                clicked = self._click_pagination_next(page)
                if not clicked:
                    LOGGER.info(
                        "Active keepalive: no next-page button reachable on '%s' "
                        "(probably last page or single-page result); stopping pagination",
                        search_visited,
                    )
                    break
                pagination_clicks_made += 1
                # Wait for the new page's vehicle cards to render
                # (same selector as initial render — VEHICLE cards on
                # results page, NOT saved-search-list cards). 30s
                # should be ample for a paginated GraphQL query.
                try:
                    self._wait_for_vehicle_results_cards(page, timeout_ms=30_000)
                except (SavedSearchPageEmpty, BrowserSessionError):
                    LOGGER.warning(
                        "Active keepalive: page %d after pagination click on '%s' "
                        "did not render cards; tick still counts as exercise",
                        pagination_clicks_made, search_visited,
                    )
                    # Stop paginating but don't trigger recovery — the
                    # earlier render already proved the session works.
                    break
                try:
                    page.wait_for_timeout(linger_ms)
                except Exception:
                    pass

            # Decay detection still applies — slow-render is a useful
            # observation even when the keepalive succeeds.
            self._keepalive_render_history.append(cards_render_ms)
            if len(self._keepalive_render_history) > self._KEEPALIVE_RENDER_HISTORY_MAX:
                self._keepalive_render_history = self._keepalive_render_history[
                    -self._KEEPALIVE_RENDER_HISTORY_MAX:
                ]
            window = self._keepalive_render_history[-self._KEEPALIVE_DECAY_WINDOW:]
            slow_hits = sum(
                1 for ms in window if ms > self._KEEPALIVE_DECAY_RENDER_MS_THRESHOLD
            )
            if slow_hits >= self._KEEPALIVE_DECAY_HITS_REQUIRED:
                sorted_window = sorted(window)
                p50 = sorted_window[len(sorted_window) // 2]
                p90_idx = max(0, int(len(sorted_window) * 0.9) - 1)
                p90 = sorted_window[p90_idx]
                LOGGER.error(
                    "KEEPALIVE_DECAY_DETECTED port=%d render_ms_p50=%d render_ms_p90=%d "
                    "action=observation_only",
                    self.settings.chrome_debug_port, p50, p90,
                )
                self._keepalive_render_history = []
                decay_signal = "slow_render"
        except SavedSearchPageEmpty:
            raise
        except BrowserSessionError:
            self._browser = None
            raise
        except Exception as exc:
            outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
            self._browser = None
            raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc
        finally:
            duration_ms = int((time.monotonic() - t0) * 1000)
            LOGGER.warning(
                "KEEPALIVE_TICK port=%d outcome=%s duration_ms=%d url_at_failure=%s "
                "seed_url=%s tab_strategy=active cards_render_ms=%s decay_signal=%s "
                "search_visited=%s pagination_clicks=%d",
                self.settings.chrome_debug_port,
                outcome,
                duration_ms,
                url_at_failure if url_at_failure is not None else "-",
                seed_url if seed_url is not None else "-",
                cards_render_ms if cards_render_ms is not None else "-",
                decay_signal,
                search_visited if search_visited is not None else "-",
                pagination_clicks_made,
            )

    def _click_pagination_next(self, page: Page) -> bool:
        """Try several next-page-button selectors in priority order.
        Returns True if a click landed (proxied by URL change OR by
        the click not raising), False if no clickable button was
        found. Doesn't raise on miss — pagination might not exist
        on the last page or for single-page result sets."""
        before_url = page.url
        for selector in self._PAGINATION_NEXT_SELECTORS:
            try:
                locator = page.locator(selector).first
                if not locator.count():
                    continue
                if not locator.is_visible(timeout=1000):
                    continue
                if not locator.is_enabled(timeout=1000):
                    continue
            except Exception:
                continue
            try:
                locator.click(timeout=5000)
                # Brief pause for the click to propagate before the
                # caller's wait_for_saved_search_cards picks up the
                # new render.
                page.wait_for_timeout(500)
                # URL change is the strongest signal that pagination
                # actually advanced; some OVE pagination updates the
                # hash fragment, others swap the in-memory state.
                # Either way, treating non-raise as success is fine.
                return True
            except Exception as exc:
                LOGGER.debug(
                    "Pagination click via %r failed: %s", selector, exc,
                )
                continue
        return False

    def _touch_session_persistent_tab(self) -> None:
        # 2026-05-04 keepalive durability fix. See module-level Settings
        # docstring for context. Key differences from the worker-tab
        # fallback:
        #   - Reuse self._keepalive_page across ticks (no churn)
        #   - Call _wait_for_saved_search_cards to verify the page
        #     actually rendered (cookie-accepted-but-empty backend
        #     would otherwise show outcome=ok)
        #   - Settle for keepalive_settle_ms so OVE's polling XHRs fire
        #   - Track cards_render_ms in a per-instance deque for decay
        #     detection
        #   - Do NOT close the page — leave it for the next tick
        t0 = time.monotonic()
        outcome = self._KEEPALIVE_OUTCOME_OK
        url_at_failure: str | None = None
        seed_url: str | None = None
        cards_render_ms: int | None = None
        decay_signal = "none"
        try:
            try:
                browser = self._connect_browser()
            except Exception as exc:
                outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
                self._browser = None
                raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc

            try:
                seed_url = self._snapshot_first_blank_or_auth_url(browser)
                page = self._get_or_create_keepalive_page(browser)
            except PlaywrightTimeoutError as exc:
                outcome = self._KEEPALIVE_OUTCOME_GOTO_TIMEOUT
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                LOGGER.warning(
                    "Keepalive seed-page goto timed out (treating as transient): %s",
                    exc,
                )
                return
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                # Forget the bad keepalive page so the next tick re-creates it.
                self._keepalive_page = None
                raise

            try:
                page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
            except PlaywrightTimeoutError as exc:
                outcome = self._KEEPALIVE_OUTCOME_GOTO_TIMEOUT
                try:
                    url_at_failure = page.url
                except Exception:
                    url_at_failure = None
                LOGGER.warning(
                    "Keepalive worker-page goto timed out (treating as transient): %s",
                    exc,
                )
                return

            if self._is_login_page(page):
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                if self._try_single_shot_login_click(page):
                    outcome = self._KEEPALIVE_OUTCOME_LOGIN_OK
                    url_at_failure = None
                else:
                    try:
                        lk = auth_lockout.get_state(
                            self.settings.artifact_dir,
                            port=self.settings.chrome_debug_port,
                        )
                    except Exception:
                        lk = None
                    if lk is not None and lk.blocked:
                        outcome = self._KEEPALIVE_OUTCOME_LOGIN_BLOCKED
                    else:
                        outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                    raise BrowserSessionError(
                        "OVE session is not authenticated; browser is on the login page"
                    )
                # Login click succeeded — we're on the post-login page now.
                # Skip the render-verify+settle this tick; next tick will
                # verify a fresh session.
                return

            # Render-verify: confirm auth + backend token + React all worked.
            # IMPORTANT (2026-05-04 fix): the keepalive does NOT propagate
            # cards_empty / cards_did_not_render upward. Those exceptions
            # would be caught by run_browser_operation's SavedSearchPageEmpty
            # handler which calls _clear_chrome_cookies — and even with
            # the surgical cookie clear, a transient empty-page observation
            # is too weak a signal to justify wiping OVE cookies. The
            # keepalive's job is to capture the observation in telemetry
            # so the operator can see decay; the next REAL operation
            # (sync, hot-deal export) will hit it through a path that
            # has proper retry semantics and IS allowed to escalate.
            #
            # Pre-fix this branch raised SavedSearchPageEmpty which fired
            # at 14:47 on 2026-05-04, wiped Login B's cookies (including
            # device-trust), and forced a fresh 2FA text. Now: outcome
            # tagged in telemetry, persistent page reset so next tick
            # tries fresh, return cleanly.
            #
            # Bumped timeout from 15_000 to 30_000 to match operator's
            # real-world observation that cold-start saved-searches
            # render takes 22–30s, not 3–6s.
            cards_t0 = time.monotonic()
            try:
                self._wait_for_saved_search_cards(page, timeout_ms=30_000)
            except SavedSearchPageEmpty:
                outcome = self._KEEPALIVE_OUTCOME_CARDS_EMPTY
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                LOGGER.warning(
                    "Keepalive observed empty saved-searches on port %d "
                    "(NOT triggering recovery — next real op will handle "
                    "if session is genuinely stale)",
                    self.settings.chrome_debug_port,
                )
                # Drop the persistent page so next tick recreates it
                # (gives the new tick a chance to recover via fresh nav).
                self._keepalive_page = None
                return
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_CARDS_DID_NOT_RENDER
                try:
                    url_at_failure = page.url
                except Exception:
                    pass
                LOGGER.warning(
                    "Keepalive saved-search cards did not render on port %d "
                    "within 30s (NOT triggering recovery — next real op "
                    "will handle if persistent)",
                    self.settings.chrome_debug_port,
                )
                self._keepalive_page = None
                return
            cards_render_ms = int((time.monotonic() - cards_t0) * 1000)

            # Settle window: let OVE's polling XHRs fire and refresh the
            # backend token. Matches A's natural 5–15s of "live" time
            # between operations. Configurable via KEEPALIVE_SETTLE_MS.
            settle_ms = int(getattr(self.settings, "keepalive_settle_ms", 8000))
            try:
                page.wait_for_timeout(settle_ms)
            except Exception:
                # Settle is best-effort. If wait_for_timeout fails (page
                # closed mid-settle, etc.) the tick is still
                # successful — we got the cards rendered.
                pass

            # Decay detection: append to history, check threshold.
            self._keepalive_render_history.append(cards_render_ms)
            if len(self._keepalive_render_history) > self._KEEPALIVE_RENDER_HISTORY_MAX:
                self._keepalive_render_history = self._keepalive_render_history[
                    -self._KEEPALIVE_RENDER_HISTORY_MAX:
                ]
            window = self._keepalive_render_history[-self._KEEPALIVE_DECAY_WINDOW:]
            slow_hits = sum(
                1 for ms in window if ms > self._KEEPALIVE_DECAY_RENDER_MS_THRESHOLD
            )
            if slow_hits >= self._KEEPALIVE_DECAY_HITS_REQUIRED:
                # 2026-05-04 fix: decay detection is now OBSERVATION-ONLY.
                # The original design (raise SavedSearchPageEmpty → cookie
                # clear) was too eager and contributed to unnecessary
                # device-trust cookie wipes / 2FA storms. Slow render
                # usually means OVE itself is slow, not that the session
                # is bad. The right response is to LOG the observation
                # so the operator can correlate against OVE outage
                # reports, but NOT to take destructive action from the
                # keepalive itself.
                #
                # If the session IS genuinely degrading, the next real
                # operation (sync, hot-deal export) will hit the failure
                # through a path that has proper retry semantics and IS
                # allowed to escalate to recovery.
                sorted_window = sorted(window)
                p50 = sorted_window[len(sorted_window) // 2]
                p90_idx = max(0, int(len(sorted_window) * 0.9) - 1)
                p90 = sorted_window[p90_idx]
                LOGGER.error(
                    "KEEPALIVE_DECAY_DETECTED port=%d render_ms_p50=%d render_ms_p90=%d "
                    "action=observation_only",
                    self.settings.chrome_debug_port, p50, p90,
                )
                # Reset history so we don't fire on every subsequent tick.
                self._keepalive_render_history = []
                decay_signal = "slow_render"
        except SavedSearchPageEmpty:
            # cards_empty / decay paths. Don't reset _browser — the CDP
            # connection itself is fine; only the OVE session is bad.
            # The caller (run_browser_operation) will route this through
            # the cookie-clear handler.
            raise
        except BrowserSessionError:
            self._browser = None
            raise
        except Exception as exc:
            outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
            self._browser = None
            raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc
        finally:
            duration_ms = int((time.monotonic() - t0) * 1000)
            LOGGER.warning(
                "KEEPALIVE_TICK port=%d outcome=%s duration_ms=%d url_at_failure=%s "
                "seed_url=%s tab_strategy=persistent cards_render_ms=%s decay_signal=%s",
                self.settings.chrome_debug_port,
                outcome,
                duration_ms,
                url_at_failure if url_at_failure is not None else "-",
                seed_url if seed_url is not None else "-",
                cards_render_ms if cards_render_ms is not None else "-",
                decay_signal,
            )

    def _get_or_create_keepalive_page(self, browser: Browser) -> Page:
        """Return the persistent keepalive page, opening one if needed.

        Reuses self._keepalive_page if it's still valid (not closed and
        URL still on ove.com). Otherwise opens a fresh worker tab in
        the seed page's context and stashes it.
        """
        existing = self._keepalive_page
        if existing is not None:
            try:
                if not existing.is_closed():
                    url = (existing.url or "").lower()
                    if "ove.com" in url:
                        return existing
            except Exception:
                # Existing page handle is broken (Chrome restart, GC, etc.).
                # Fall through to recreate.
                pass
            self._keepalive_page = None

        # Need a fresh keepalive page. Open one in the seed tab's
        # context so it shares cookies + storage with the seed.
        page = self._open_dedicated_ove_page(browser)
        self._keepalive_page = page
        return page

    def _touch_session_worker_tab(self) -> None:
        # Worker-tab fallback. Byte-identical to the pre-2026-05-04
        # behavior so KEEPALIVE_PERSISTENT_TAB=false is a safe
        # emergency rollback. Emits the same KEEPALIVE_TICK telemetry
        # shape with tab_strategy=worker.
        t0 = time.monotonic()
        outcome = self._KEEPALIVE_OUTCOME_OK
        url_at_failure: str | None = None
        seed_url: str | None = None
        page: Page | None = None
        try:
            try:
                browser = self._connect_browser()
            except Exception as exc:
                outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
                self._browser = None
                raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc

            try:
                seed_url = self._snapshot_first_blank_or_auth_url(browser)
                page = self._open_dedicated_ove_page(browser)
            except PlaywrightTimeoutError as exc:
                outcome = self._KEEPALIVE_OUTCOME_GOTO_TIMEOUT
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                LOGGER.warning(
                    "Keepalive seed-page goto timed out (treating as transient): %s",
                    exc,
                )
                return
            except BrowserSessionError:
                outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                url_at_failure = self._snapshot_first_blank_or_auth_url(browser) or seed_url
                self._close_stuck_blank_or_auth_tabs(browser)
                raise

            try:
                try:
                    page.goto(self._saved_searches_url(), wait_until="domcontentloaded", timeout=30000)
                except PlaywrightTimeoutError as exc:
                    outcome = self._KEEPALIVE_OUTCOME_GOTO_TIMEOUT
                    try:
                        url_at_failure = page.url
                    except Exception:
                        url_at_failure = None
                    LOGGER.warning(
                        "Keepalive worker-page goto timed out (treating as transient): %s",
                        exc,
                    )
                    return
                if self._is_login_page(page):
                    try:
                        url_at_failure = page.url
                    except Exception:
                        pass
                    if self._try_single_shot_login_click(page):
                        outcome = self._KEEPALIVE_OUTCOME_LOGIN_OK
                        url_at_failure = None
                    else:
                        try:
                            lk = auth_lockout.get_state(
                                self.settings.artifact_dir,
                                port=self.settings.chrome_debug_port,
                            )
                        except Exception:
                            lk = None
                        if lk is not None and lk.blocked:
                            outcome = self._KEEPALIVE_OUTCOME_LOGIN_BLOCKED
                        else:
                            outcome = self._KEEPALIVE_OUTCOME_LOGIN_FAILED
                        raise BrowserSessionError(
                            "OVE session is not authenticated; browser is on the login page"
                        )
            finally:
                self._close_page(page)
        except BrowserSessionError:
            self._browser = None
            raise
        except Exception as exc:
            outcome = self._KEEPALIVE_OUTCOME_CONNECT_FAILED
            self._browser = None
            raise BrowserSessionError(f"OVE browser keepalive failed: {exc}") from exc
        finally:
            duration_ms = int((time.monotonic() - t0) * 1000)
            LOGGER.warning(
                "KEEPALIVE_TICK port=%d outcome=%s duration_ms=%d url_at_failure=%s "
                "seed_url=%s tab_strategy=worker cards_render_ms=- decay_signal=none",
                self.settings.chrome_debug_port,
                outcome,
                duration_ms,
                url_at_failure if url_at_failure is not None else "-",
                seed_url if seed_url is not None else "-",
            )

    def _snapshot_first_blank_or_auth_url(self, browser: Browser) -> str | None:
        """Return the URL of the first about:blank or auth.* tab found
        across all contexts, or None. Used to capture what the user is
        seeing when a goto times out — the visible spinner is almost
        always one of these."""
        try:
            for context in browser.contexts:
                for page in context.pages:
                    try:
                        if page.is_closed():
                            continue
                        url = (page.url or "").lower()
                    except Exception:
                        continue
                    if not url:
                        continue
                    if (
                        url == "about:blank"
                        or any(pat in url for pat in self._ORPHAN_TAB_URL_PATTERNS)
                    ):
                        try:
                            return page.url
                        except Exception:
                            return url
        except Exception:
            return None
        return None

    def _close_stuck_blank_or_auth_tabs(self, browser: Browser) -> None:
        """Close every about:blank or auth-domain tab still open on this
        browser. Called from the keepalive timeout path because the
        existing try/finally only closed the worker page returned from
        _open_dedicated_ove_page; if _get_ove_page's seed-page goto
        timed out FIRST, the seed about:blank tab was left mid-
        navigation. This method is the cleanup for that leak path —
        safe even if no leak exists (no-op when nothing matches).
        Failures are swallowed; this is a safety net."""
        try:
            for context in browser.contexts:
                for page in list(context.pages):
                    try:
                        if page.is_closed():
                            continue
                        url = (page.url or "").lower()
                    except Exception:
                        continue
                    if not url:
                        continue
                    if (
                        url == "about:blank"
                        or any(pat in url for pat in self._ORPHAN_TAB_URL_PATTERNS)
                    ):
                        self._close_page(page)
        except Exception as exc:
            LOGGER.debug("close_stuck_blank_or_auth_tabs: enumeration failed: %s", exc)

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
                    # Row-count delta canary (2026-05-06). Detects silent
                    # data-loss bugs like the 4948 -> 4305 shrinkage seen
                    # on East-Hub-2025-2026 today between consecutive
                    # syncs. Fires an alert if today's row count drops
                    # below 75% of the recent max for the same search.
                    # Side-effect free if history is empty (first run).
                    try:
                        self._record_export_count_and_check_canary(
                            target_path, search_name,
                        )
                    except Exception as canary_exc:
                        # Canary failures are non-fatal; the export
                        # itself succeeded.
                        LOGGER.warning(
                            "Export row-count canary failed for '%s' "
                            "(non-fatal): %s",
                            search_name, canary_exc,
                        )
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
                autocheck_data = self._capture_autocheck_on_page(
                    detail_page, vin, artifact_dir, listing_json=listing_json,
                )
                LOGGER.info(
                    "Stage capture_autocheck: complete for VIN %s "
                    "(score=%s, status=%s, category=%s, stage=%s, message=%s)",
                    vin,
                    autocheck_data.get("autocheck_score") if autocheck_data else None,
                    autocheck_data.get("scrape_status", "unknown") if autocheck_data else "skipped",
                    autocheck_data.get("failure_category") if autocheck_data else None,
                    autocheck_data.get("failure_stage") if autocheck_data else None,
                    autocheck_data.get("failure_message") if autocheck_data else None,
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
            # Only feed CR popup body text into the structured CR parser. The
            # OVE detail page (payload.body_text) contains UI section labels
            # like "Structural Damage" / "Title Status" that the generic
            # parser regexes match as if they were CR field values, producing
            # false positives (e.g. cr.structural_damage=True from a listing
            # page that has no actual damage data). When the CR popup capture
            # fails, raw_text MUST be None so the structured parser exits
            # early instead of guessing fields from the wrong source.
            condition_report = normalize_condition_report(
                condition_report,
                raw_text=report_page_text,
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
            # Defense (2026-04-28): clear any stray asyncio state on this
            # thread BEFORE starting Playwright. Playwright's sync API
            # checks `asyncio.events._get_running_loop()` and refuses if
            # one is set. Some HTTP libraries (httpx 0.28 + anyio on
            # Python 3.12 Windows in particular) can leave a transient
            # event-loop reference attached to the calling thread, which
            # gets misclassified as "Sync API inside the asyncio loop".
            # Forcing the running-loop slot to None here makes Playwright
            # see a clean state regardless of upstream pollution.
            try:
                import asyncio as _asyncio
                _asyncio.events._set_running_loop(None)
            except Exception:
                pass
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

    # URL fragments that identify a tab as an orphaned auth/recovery
    # popup the scraper does NOT want to keep around. The sweeper closes
    # any tab whose URL contains one of these. Match is case-insensitive.
    # See sweep_orphan_tabs() for usage and the 2026-04-28 incident
    # write-up for why this defense exists.
    _ORPHAN_TAB_URL_PATTERNS: tuple[str, ...] = (
        "auth.manheim.com",
        "auth0.manheim.com",
        "signin.manheim.com",
        "/as/authorization",
        "/as/login",
        "accounts.manheim.com",
    )

    def sweep_orphan_tabs(self) -> int:
        """Close every orphan auth / login / stray-blank tab in the
        connected Chrome.

        Belt-and-suspenders defense (Fix B, 2026-04-28): the only
        guaranteed way to prevent tab pile-ups across multiple leak
        paths is to actively sweep them. Per-call leak fixes have been
        made four times and each missed a different code path. This
        sweeper does NOT trust per-operation cleanup: it walks every
        page in every context and closes:

          1. Tabs whose URL matches _ORPHAN_TAB_URL_PATTERNS (auth /
             login / signin endpoints) — always safe to close.
          2. Stray `about:blank` tabs, BUT ONLY when the same context
             also contains at least one OVE page (e.g. an active worker
             or seed tab). The "OVE page exists" gate is what makes
             this safe: during launcher boot the only tab is about:blank
             and we MUST NOT close it (or Python would have nothing to
             navigate). Once Python has navigated about:blank to OVE,
             a separate stray about:blank is a leak — usually from
             Chrome's "Continue where you left off" session-restore
             reopening tabs after an unclean shutdown (the 2026-04-29
             post-power-outage observation). Closing it doesn't break
             the seed.

        Safe to call from any context (keepalive tick, post-recovery,
        between operations):
          - Failures inside the sweep are swallowed — the sweep is a
            safety net, not a critical-path operation.
          - Per-context decisions: OVE-tab gating is evaluated against
            each context's pages independently, so a non-OVE context
            (rare) won't lose its only blank tab.
          - Counts and logs what it closed so the operator can see
            whether the sweep is doing real work.

        Returns the total number of pages closed. Returns 0 silently
        if the browser handle isn't connected.
        """
        if self._browser is None:
            return 0
        closed = 0
        try:
            contexts = self._browser.contexts
        except Exception as exc:
            LOGGER.debug("sweep_orphan_tabs: failed to enumerate contexts: %s", exc)
            return 0
        ove_base_url_lower = (self.settings.ove_base_url or "").lower()
        for context in contexts:
            try:
                pages = list(context.pages)
            except Exception:
                continue

            # Gate for the about:blank rule: only sweep stray blank
            # tabs when at least one OVE tab exists in this context.
            # This snapshot is computed BEFORE we close anything; we
            # don't want to flip the gate mid-loop by closing the only
            # OVE tab.
            has_ove_tab = False
            for page in pages:
                try:
                    if page.is_closed():
                        continue
                    page_url = (page.url or "").lower()
                except Exception:
                    continue
                if ove_base_url_lower and ove_base_url_lower in page_url:
                    has_ove_tab = True
                    break

            for page in pages:
                # Identity-skip the persistent keepalive page (2026-05-04
                # fix). It might transiently show about:blank during
                # navigation between ticks; the URL-based about:blank
                # rule below would otherwise kill it. The identity
                # check is the only safe way — never trust URL alone for
                # the keepalive page.
                if (
                    self._keepalive_page is not None
                    and page is self._keepalive_page
                ):
                    continue
                try:
                    if page.is_closed():
                        continue
                    url = (page.url or "").lower()
                except Exception:
                    continue
                if not url:
                    continue
                close_reason: str | None = None
                if any(pattern in url for pattern in self._ORPHAN_TAB_URL_PATTERNS):
                    close_reason = "auth orphan"
                elif has_ove_tab and url in ("about:blank", ""):
                    # url == "" handled above by `if not url: continue`,
                    # so this branch only fires for literal about:blank.
                    close_reason = "stray about:blank (OVE seed exists)"
                if close_reason is None:
                    continue
                LOGGER.warning(
                    "sweep_orphan_tabs: closing %s tab url=%s",
                    close_reason, page.url,
                )
                self._close_page(page)
                closed += 1
        if closed:
            LOGGER.warning(
                "sweep_orphan_tabs: closed %d orphan tab(s) on port %d",
                closed, self.settings.chrome_debug_port,
            )
        return closed

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

    def _wait_for_vehicle_results_cards(self, page: Page, *, timeout_ms: int = 60_000) -> None:
        """Wait for VEHICLE result cards to render on a search-results
        page (e.g. /search/results#/results/<saved-search-uuid>).

        2026-05-06: distinct from _wait_for_saved_search_cards, which
        waits for SAVED-SEARCH list cards on the /saved_searches list
        page. After the user (or active keepalive) clicks INTO a saved
        search, the page navigates to the results URL and renders
        vehicle-result cards using a totally different DOM structure.
        Using the wrong helper here was the regression that produced
        cards_did_not_render on every active-keepalive tick on
        2026-05-06 23:05.

        Distinguishes 'session expired, redirected to login' from
        'cards just haven't rendered yet'. Only raises on session
        problems; ordinary timeouts surface as a clear
        BrowserSessionError so the caller can decide to recover or
        continue.
        """
        if self._is_login_page(page):
            raise BrowserSessionError(
                "OVE session is not authenticated; browser is on the login page"
            )
        try:
            page.wait_for_selector(
                "[data-test-id*='vehicle'], [class*='VehicleCard'], tr[data-test-id*='vehicle']",
                state="visible",
                timeout=timeout_ms,
            )
        except PlaywrightTimeoutError:
            if self._is_login_page(page):
                raise BrowserSessionError(
                    "OVE session expired during wait; browser redirected to login page"
                )
            raise BrowserSessionError(
                f"Vehicle result cards did not render within {timeout_ms}ms — "
                "possible session/data-layer issue or OVE outage"
            )

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
            # Strategy 1: click the actual title div inside the card.
            # Important: do NOT use `.SavedSearchItem__title-container` here.
            # That wrapper appears before the title in DOM order, so a
            # `.first` locator can accidentally target the non-clickable
            # container and leave the browser on the saved-searches list.
            title_in_card = card.locator(".SavedSearchItem__title").first
            click_target = title_in_card if title_in_card.count() else card
            try:
                click_target.click(timeout=10000)
                # Verify the SPA actually moved into a results route.
                # A click can succeed visually while the page stays on the
                # saved-search index, so we wait explicitly for the hash
                # route to become a results URL before trusting the click.
                page.wait_for_url(re.compile(r"/results/"), timeout=15000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                LOGGER.info("Navigated to saved search '%s' results at %s", matched_name, page.url)
                return
            except PlaywrightTimeoutError:
                LOGGER.warning(
                    "Click on saved search '%s' did not reach results URL (url=%s)",
                    matched_name,
                    page.url,
                )
            except Exception as exc:
                LOGGER.warning("Click on saved search '%s' card failed: %s", matched_name, exc)

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
        #
        # 2026-05-11 fix: bumped from 60s → 180s. Operator confirmed
        # the largest east-hub searches (East-Hub-2015-2021 et al.,
        # 3k-4k rows each) now consistently take ~120s to render on
        # OVE's side — twice the previous 60s budget. With the silent
        # "proceed anyway" fallback, the export button isn't even on
        # the page yet when the timeout fires, so the trigger-locator
        # search at the bottom of this method fails and the whole
        # export raises BrowserSessionError. 180s gives ~50% margin
        # over the observed render time. Grid-settle also bumped
        # 1500ms → 3000ms since a slower render implies slower hydrate.
        #
        # Prior history: 2026-05-06 bumped 20s → 60s based on 22-30s
        # observed renders.
        try:
            page.wait_for_selector(
                "[data-test-id*='vehicle'], [class*='VehicleCard'], tr[data-test-id*='vehicle']",
                state="visible",
                timeout=180000,
            )
            page.wait_for_timeout(3000)  # let the rest of the grid catch up
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

        # OVE's saved-search results page hides the real export entry in a
        # menu item (#inventory_export). Clicking it navigates to the
        # Inventory Export page, where the actual CSV download is triggered
        # by the visible "Create Report" submit button.
        hidden_inventory_export = page.locator("a#inventory_export").first
        try:
            if hidden_inventory_export.count() > 0:
                try:
                    with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                        page.evaluate(
                            """
                            () => {
                              const el = document.querySelector('a#inventory_export');
                              if (!el) throw new Error('Missing inventory export link');
                              el.click();
                            }
                            """
                        )
                except Exception:
                    page.evaluate(
                        """
                        () => {
                          const el = document.querySelector('a#inventory_export');
                          if (!el) throw new Error('Missing inventory export link');
                          el.click();
                        }
                        """
                    )
                    page.wait_for_timeout(3000)
                page.wait_for_timeout(1000)
                report_download_locators = [
                    page.locator("input#generate_button").first,
                    page.get_by_role("button", name=re.compile(r"create\s*report", re.I)).first,
                    page.get_by_role("button", name=re.compile(r"generate", re.I)).first,
                ]
                for report_trigger in report_download_locators:
                    download = self._try_download_click(page, report_trigger)
                    if download is not None:
                        return download
        except Exception as exc:
            LOGGER.debug("Hidden inventory export flow did not complete cleanly: %s", exc)

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

    # Row-count delta canary thresholds (2026-05-06). Detects silent
    # data-loss bugs like the 4948 -> 4305 shrinkage seen on
    # East-Hub-2025-2026 today between consecutive syncs.
    _EXPORT_HISTORY_MAX = 10
    _EXPORT_HISTORY_FILENAME = "saved_search_export_history.json"
    # If current count is below this fraction of the recent max, fire
    # an alert. 0.75 = 25% drop is considered suspicious.
    _EXPORT_SHRINKAGE_THRESHOLD = 0.75
    # Don't alert on tiny absolute drops (e.g. legitimate 10-row churn
    # on a 50-row search). Only alert if the drop is meaningful.
    _EXPORT_SHRINKAGE_MIN_ABSOLUTE = 100

    def _record_export_count_and_check_canary(
        self,
        target_path: Path,
        search_name: str,
    ) -> None:
        """Record the row count of the just-exported CSV and alert if it
        dropped suspiciously vs recent exports of the same search.

        State file: artifacts/_state/saved_search_export_history.json
        Per-search history: last 10 (timestamp, row_count) pairs.

        Alerting: emits an EXPORT_SHRINKAGE_DETECTED log line and (if
        a notifier is wired in) sends an admin alert when the new
        count is below 75% of the recent max AND the absolute drop is
        > 100 rows. The 75% threshold tolerates normal auction churn
        (vehicles selling/expiring throughout the day) but catches
        the multi-hundred-row losses that indicate a partial export.
        """
        import json as _json
        # Count rows = total lines minus header. Tolerate trailing
        # blank line by counting non-empty lines only.
        try:
            with target_path.open("r", encoding="utf-8", errors="replace") as fh:
                row_count = max(0, sum(1 for line in fh if line.strip()) - 1)
        except Exception as exc:
            LOGGER.warning(
                "Row-count canary: failed to count rows in %s: %s",
                target_path, exc,
            )
            return

        history_path = (
            self.settings.artifact_dir / "_state" / self._EXPORT_HISTORY_FILENAME
        )
        history: dict[str, Any] = {}
        if history_path.exists():
            try:
                history = _json.loads(history_path.read_text(encoding="utf-8"))
            except Exception as exc:
                LOGGER.warning(
                    "Row-count canary: history file unreadable (%s); "
                    "starting fresh", exc,
                )
                history = {}

        per_search: list[dict[str, Any]] = list(
            history.get(search_name) or []
        )
        # Look at recent counts BEFORE appending the new one so we
        # don't compare against ourselves.
        recent_counts = [
            int(entry.get("row_count") or 0)
            for entry in per_search[-self._EXPORT_HISTORY_MAX:]
            if isinstance(entry, dict)
        ]
        recent_max = max(recent_counts) if recent_counts else 0

        # Append new entry and cap the history.
        per_search.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "row_count": row_count,
        })
        per_search = per_search[-self._EXPORT_HISTORY_MAX:]
        history[search_name] = per_search

        try:
            history_path.parent.mkdir(parents=True, exist_ok=True)
            history_path.write_text(
                _json.dumps(history, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning(
                "Row-count canary: failed to write history file %s: %s",
                history_path, exc,
            )

        # Canary check. Skip if no baseline (first run for this search).
        if recent_max == 0:
            LOGGER.info(
                "Export row-count baseline established for '%s': %d rows",
                search_name, row_count,
            )
            return

        absolute_drop = recent_max - row_count
        ratio = (row_count / recent_max) if recent_max > 0 else 1.0
        suspicious = (
            ratio < self._EXPORT_SHRINKAGE_THRESHOLD
            and absolute_drop >= self._EXPORT_SHRINKAGE_MIN_ABSOLUTE
        )

        if suspicious:
            LOGGER.error(
                "EXPORT_SHRINKAGE_DETECTED search=%r current_rows=%d "
                "recent_max=%d drop=%d ratio=%.2f threshold=%.2f",
                search_name, row_count, recent_max, absolute_drop,
                ratio, self._EXPORT_SHRINKAGE_THRESHOLD,
            )
            if self._notifier is not None:
                try:
                    notify_fn = getattr(
                        self._notifier, "notify_export_shrinkage", None,
                    )
                    if callable(notify_fn):
                        notify_fn(
                            search_name=search_name,
                            current_rows=row_count,
                            recent_max=recent_max,
                            absolute_drop=absolute_drop,
                            ratio=ratio,
                            threshold=self._EXPORT_SHRINKAGE_THRESHOLD,
                            logger=LOGGER,
                        )
                except Exception as alert_exc:
                    LOGGER.warning(
                        "Failed to fire export-shrinkage alert: %s",
                        alert_exc,
                    )
        else:
            LOGGER.info(
                "Export row-count OK for '%s': %d rows "
                "(recent_max=%d, ratio=%.2f)",
                search_name, row_count, recent_max, ratio,
            )

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
        self,
        detail_page: Page,
        vin: str,
        artifact_dir: Path,
        listing_json: dict[str, Any] | None = None,
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
        listing_fallback = self._parse_autocheck_listing_json(listing_json)
        if listing_fallback:
            LOGGER.info(
                "AutoCheck listing JSON fallback for VIN %s: score=%s owners=%s "
                "accidents=%s range=%s-%s",
                vin,
                listing_fallback.get("autocheck_score"),
                listing_fallback.get("owner_count"),
                listing_fallback.get("accident_count"),
                listing_fallback.get("score_range_low"),
                listing_fallback.get("score_range_high"),
            )

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
            result = self._merge_autocheck_fallback(result, listing_fallback)
        else:
            LOGGER.warning("AutoCheck: no inline section detected for VIN %s", vin)
            detail_page.screenshot(path=str(ac_artifact_dir / "autocheck-none.png"), full_page=True)
            (ac_artifact_dir / "autocheck-none.html").write_text(detail_page.content(), encoding="utf-8")
            if listing_fallback:
                LOGGER.warning(
                    "AutoCheck: inline section missing for VIN %s, continuing with "
                    "listing JSON fallback before Experian navigation",
                    vin,
                )
                result = {
                    **listing_fallback,
                    "raw_text": "",
                    "failure_stage": "inline",
                }
            else:
                return {
                    "scrape_status": "failed",
                    "failure_category": "not_found",
                    "failure_stage": "inline",
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
        if result["scrape_status"] == "success":
            result.pop("failure_category", None)
            result.pop("failure_message", None)
            result.pop("failure_stage", None)
        if experian_outcome.get("failure_category"):
            result["failure_category"] = experian_outcome["failure_category"]
        if experian_outcome.get("failure_stage"):
            result["failure_stage"] = experian_outcome["failure_stage"]
        if experian_outcome.get("failure_message"):
            result["failure_message"] = experian_outcome["failure_message"]
        return result

    def _attempt_inline_login_recovery(self, page: Page, context_label: str) -> bool:
        """Shared entry-point for inline single-shot login recovery from any
        mid-scrape auth redirect (AutoCheck popup, CR popup, CR direct
        navigation landing on auth).

        This delegates to _try_single_shot_login_click, which already
        enforces a process-wide click cooldown via the
        _auto_login_last_attempt_at timestamp + _AUTO_LOGIN_COOLDOWN
        (default 6h). Per feedback_ove_auto_login_account_lock.md,
        auto-login must never retry in a tight loop — calling this
        from multiple sites is SAFE because they all share that
        timestamp: the first one to reach the click consumes the
        cooldown window, and every subsequent call (including from a
        different subsystem) short-circuits to False until the
        cooldown elapses. The cross-process click ledger
        (auth_lockout) provides a second rate-limit layer so even
        process restarts cannot create a click storm.

        Returns True iff the page is no longer on the auth screen
        after the click. Authorized caller behavior: on True, re-check
        the target page for real content and proceed. On False, treat
        the auth failure as terminal for this attempt.

        context_label is purely for log attribution so a post-incident
        audit can tell which subsystem triggered the click.
        """
        if not self._is_manheim_auth_page(page):
            # Defensive: only burn the single-shot attempt on pages
            # that are actually on auth. The caller should have
            # checked already, but we double-check here so a stray
            # call from a non-auth context doesn't consume the budget.
            return False
        LOGGER.info(
            "Inline auth recovery attempt for %s at url=%s",
            context_label, page.url,
        )
        return self._try_single_shot_login_click(page)

    def _recover_cr_auth_inline(
        self,
        source_page: Page,
        popups: list[Page],
        context_label: str,
    ) -> bool:
        """CR-click-specific recovery helper. A ManheimAuthRedirectError
        inside _open_via_ove_internal_viewer could have come from the
        source_page itself (React-routed to auth) OR from one of the
        collected popups. We don't know which from the exception alone,
        so we iterate the candidates and try recovery on the first one
        that's actually on a Manheim auth page.

        Only one candidate ever gets an actual click because the
        process-wide single-shot flag is consumed on the first attempt.
        We break after the first candidate that was on auth (whether
        recovery succeeded or not) because the flag is spent either way.
        """
        candidates: list[Page] = []
        try:
            if self._is_manheim_auth_page(source_page):
                candidates.append(source_page)
        except Exception:
            pass
        for popup in popups:
            try:
                if popup.is_closed():
                    continue
            except Exception:
                continue
            try:
                if self._is_manheim_auth_page(popup):
                    candidates.append(popup)
            except Exception:
                continue
        for cand in candidates:
            return self._attempt_inline_login_recovery(cand, context_label)
        # No candidate is actually on auth — nothing to recover.
        return False

    # Process-wide short-circuit: once we've seen this many consecutive
    # Experian login redirects in a single scraper process, stop opening
    # new tabs for AutoCheck entirely. The vehiclehistservice.manheim.com
    # session cookie has clearly expired and every popup will redirect
    # to Manheim login. Observed 2026-04-23: one stale-Experian run
    # leaked 25 tabs before the user had to intervene. A restart of the
    # scraper (which refreshes the Experian cookie on next hit) clears
    # this counter.
    _EXPERIAN_MAX_CONSECUTIVE_LOGIN_REDIRECTS = 3
    _experian_consecutive_login_redirects: int = 0

    @classmethod
    def _experian_short_circuit_tripped(cls) -> bool:
        return cls._experian_consecutive_login_redirects >= cls._EXPERIAN_MAX_CONSECUTIVE_LOGIN_REDIRECTS

    @classmethod
    def _record_experian_login_redirect(cls) -> None:
        cls._experian_consecutive_login_redirects += 1

    @classmethod
    def _reset_experian_login_redirect_counter(cls) -> None:
        cls._experian_consecutive_login_redirects = 0

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
        scrape_status and optional failure_category/failure_message.

        Every tab/popup created here is wrapped in try/finally so a mid-
        flight exception (goto timeout, navigation error, etc.) still
        closes the tab. Without this the 2026-04-23 run leaked 25 tabs
        when the Experian session expired mid-run.

        Additionally: after N consecutive login redirects, the class-level
        short-circuit skips further attempts for the rest of the process
        lifetime. Restart the scraper (or fix the Experian cookie) to
        reset."""
        # Short-circuit: Experian session is known stale for this process.
        # Return partial immediately without opening any tabs.
        if self._experian_short_circuit_tripped():
            return {
                "scrape_status": "partial",
                "failure_category": "experian_session_expired",
                "failure_stage": "experian_short_circuit",
                "failure_message": (
                    f"Experian/VehicleHistService session expired for this "
                    f"scraper process (>= {self._EXPERIAN_MAX_CONSECUTIVE_LOGIN_REDIRECTS} "
                    "consecutive login redirects). Skipping Experian navigation "
                    "for remaining VINs; restart the scraper to recover."
                ),
            }

        failure_reasons: list[str] = []
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
        if not view_href:
            failure_reasons.append("missing_href")
            LOGGER.warning(
                "AutoCheck Experian report href missing for VIN %s; popup fallback will be attempted",
                vin,
            )

        # Path A: direct navigation to the report URL in a new tab. This is
        # the most reliable path — same context inherits Manheim SSO cookies
        # and Playwright controls the page fully.
        if view_href:
            report_page = None
            try:
                report_page = detail_page.context.new_page()
                report_page.goto(view_href, wait_until="domcontentloaded", timeout=20000)

                # Fast URL-based auth detection BEFORE waiting for content.
                # If the URL redirected to auth.manheim.com the content
                # will never arrive and waiting wastes 12s per VIN.
                skip_capture_auth = False
                if self._url_is_auth_redirect(report_page.url):
                    # Try inline single-shot login recovery. Per user
                    # 2026-04-24: Chrome typically has credentials
                    # pre-filled on the Manheim auth form, so a single
                    # click on Sign In is usually enough. The recovery
                    # helper is gated by the process-wide single-shot
                    # flag, so this is safe.
                    if self._attempt_inline_login_recovery(report_page, f"autocheck-direct-{vin}"):
                        # Click succeeded; the post-login SSO bounce
                        # should redirect us back to the AutoCheck URL.
                        # Fall through to the content-wait + capture.
                        LOGGER.info(
                            "AutoCheck direct nav: auth recovery succeeded for VIN %s, resuming capture",
                            vin,
                        )
                    else:
                        # Recovery unavailable (flag consumed, no pre-filled
                        # credentials, or click didn't land us off login).
                        # Record the redirect; the finally block closes the tab.
                        self._record_experian_login_redirect()
                        LOGGER.warning(
                            "AutoCheck direct nav URL redirected to auth for VIN %s: %s (consecutive=%s)",
                            vin, report_page.url, self._experian_consecutive_login_redirects,
                        )
                        failure_reasons.append(f"direct_login_redirect:{report_page.url}")
                        skip_capture_auth = True

                if not skip_capture_auth:
                    # Wait for real content to populate. Experian reports
                    # take 5-8s to fully render per user 2026-04-24;
                    # the 3s wait we had before was catching shells.
                    try:
                        report_page.wait_for_function(
                            self._AUTOCHECK_CONTENT_READY_SIGNAL,
                            timeout=self._AUTOCHECK_POST_LOAD_TIMEOUT_MS,
                        )
                    except Exception:
                        # Content never crossed 500 chars in 12s — either
                        # very slow or genuinely broken. Fall through and
                        # capture whatever's there; content check below
                        # will decide.
                        pass
                    report_page.screenshot(path=str(ac_artifact_dir / "autocheck-report.png"), full_page=True)
                    (ac_artifact_dir / "autocheck-report.html").write_text(report_page.content(), encoding="utf-8")
                    full_text = report_page.inner_text("body")

                    # Final URL check + content check — URL could have
                    # redirected during the 12s wait window.
                    if self._url_is_auth_redirect(report_page.url) or self._looks_like_login_page(full_text):
                        self._record_experian_login_redirect()
                        LOGGER.warning(
                            "AutoCheck direct nav landed on login for VIN %s: url=%s (consecutive=%s)",
                            vin, report_page.url, self._experian_consecutive_login_redirects,
                        )
                        failure_reasons.append(f"direct_login_page:{report_page.url}")
                    elif self._is_experian_error_page(full_text):
                        LOGGER.warning(
                            "AutoCheck direct nav landed on Experian error page for VIN %s "
                            "('Your request cannot be processed'); treating as partial",
                            vin,
                        )
                        return {
                            "scrape_status": "partial",
                            "failure_category": "experian_rate_limited",
                            "failure_stage": "direct_nav",
                            "failure_message": (
                                "Experian returned 'Your request cannot be processed' "
                                "(typically a rate-limit or stale session-token response)."
                            ),
                        }
                    else:
                        self._reset_experian_login_redirect_counter()
                        self._merge_full_experian_report(result, full_text)
                        LOGGER.info("AutoCheck full report captured via direct nav for VIN %s (%d chars)", vin, len(full_text))
                        return {"scrape_status": "success"}
            except PlaywrightTimeoutError as report_exc:
                failure_reasons.append(f"direct_timeout:{report_exc}")
                LOGGER.warning("AutoCheck direct navigation timed out for VIN %s: %s", vin, report_exc)
            except Exception as report_exc:
                failure_reasons.append(f"direct_error:{type(report_exc).__name__}:{report_exc}")
                LOGGER.warning("AutoCheck direct navigation failed for VIN %s: %s", vin, report_exc)
            finally:
                # Tab cleanup — runs even if goto/screenshot/inner_text
                # raised. This is the 2026-04-23 leak fix.
                if report_page is not None:
                    try:
                        report_page.close()
                    except Exception:
                        pass

        # If the short-circuit tripped on Path A, don't try Path B.
        if self._experian_short_circuit_tripped():
            return {
                "scrape_status": "partial",
                "failure_category": "experian_session_expired",
                "failure_stage": "direct_nav",
                "failure_message": (
                    f"Experian session expired during this VIN (hit "
                    f"{self._EXPERIAN_MAX_CONSECUTIVE_LOGIN_REDIRECTS}-redirect cap)."
                ),
            }

        # Path B: click the branded autocheck-link image with expect_popup.
        # Per user 2026-04-21: this is the universal trigger across CR types
        # — the Tracker__container wrapper fires a JS click delegate that
        # opens the Experian report in a popup window.
        for selector in ("[data-test-id='autocheck-link']", "a.Autocheck__fullreport-link"):
            popup = None
            try:
                trigger = detail_page.locator(selector).first
                if not trigger.is_visible(timeout=1500):
                    failure_reasons.append(f"popup_selector_not_visible:{selector}")
                    LOGGER.info("AutoCheck popup selector not visible for VIN %s: %s", vin, selector)
                    continue
                LOGGER.info("AutoCheck: attempting popup click via %s for VIN %s", selector, vin)
                # Popup spawn can take 3-5s on a busy session; give it 12s.
                with detail_page.expect_popup(timeout=12000) as popup_info:
                    trigger.click(timeout=5000)
                popup = popup_info.value
                popup.wait_for_load_state("domcontentloaded", timeout=20000)

                # Same URL-first auth detection as Path A, with the
                # inline single-shot login recovery attempt. The flag
                # is shared across Path A and Path B (and with CR-click
                # and startup/keepalive paths), so net click volume
                # stays bounded to 1 per process.
                if self._url_is_auth_redirect(popup.url):
                    if self._attempt_inline_login_recovery(popup, f"autocheck-popup-{vin}"):
                        LOGGER.info(
                            "AutoCheck popup: auth recovery succeeded for VIN %s, resuming capture",
                            vin,
                        )
                        # Fall through to content wait + capture below.
                    else:
                        self._record_experian_login_redirect()
                        LOGGER.warning(
                            "AutoCheck popup URL redirected to auth for VIN %s: %s (consecutive=%s)",
                            vin, popup.url, self._experian_consecutive_login_redirects,
                        )
                        failure_reasons.append(f"popup_login_redirect:{popup.url}")
                        if self._experian_short_circuit_tripped():
                            break
                        continue

                # Wait up to 12s for real content (Experian takes 5-8s to
                # populate per user 2026-04-24). Fall through if it never
                # crosses the threshold; content check below will decide.
                try:
                    popup.wait_for_function(
                        self._AUTOCHECK_CONTENT_READY_SIGNAL,
                        timeout=self._AUTOCHECK_POST_LOAD_TIMEOUT_MS,
                    )
                except Exception:
                    pass
                popup.screenshot(path=str(ac_artifact_dir / "autocheck-popup.png"), full_page=True)
                (ac_artifact_dir / "autocheck-popup.html").write_text(popup.content(), encoding="utf-8")
                popup_text = popup.inner_text("body")
                if self._url_is_auth_redirect(popup.url) or self._looks_like_login_page(popup_text):
                    self._record_experian_login_redirect()
                    LOGGER.warning(
                        "AutoCheck popup landed on login for VIN %s: url=%s (consecutive=%s)",
                        vin, popup.url, self._experian_consecutive_login_redirects,
                    )
                    failure_reasons.append(f"popup_login_page:{popup.url}")
                    if self._experian_short_circuit_tripped():
                        break
                    continue
                if self._is_experian_error_page(popup_text):
                    LOGGER.warning(
                        "AutoCheck popup landed on Experian error page for VIN %s "
                        "('Your request cannot be processed'); treating as partial",
                        vin,
                    )
                    return {
                        "scrape_status": "partial",
                        "failure_category": "experian_rate_limited",
                        "failure_stage": "popup",
                        "failure_message": (
                            "Experian returned 'Your request cannot be processed' "
                            "(typically a rate-limit or stale session-token response)."
                        ),
                    }
                self._reset_experian_login_redirect_counter()
                self._merge_full_experian_report(result, popup_text)
                LOGGER.info("AutoCheck full report captured via popup click for VIN %s (%d chars)", vin, len(popup_text))
                return {"scrape_status": "success"}
            except PlaywrightTimeoutError as popup_exc:
                failure_reasons.append(f"popup_timeout:{selector}:{popup_exc}")
                LOGGER.warning("AutoCheck popup click timed out via %s for VIN %s: %s", selector, vin, popup_exc)
            except Exception as popup_exc:
                failure_reasons.append(f"popup_error:{selector}:{type(popup_exc).__name__}:{popup_exc}")
                LOGGER.warning("AutoCheck popup click via %s failed for VIN %s: %s", selector, vin, popup_exc)
            finally:
                # Popup cleanup — runs even if wait_for_load_state /
                # screenshot / inner_text raised. Critical for the
                # 2026-04-23 leak fix.
                if popup is not None:
                    try:
                        popup.close()
                    except Exception:
                        pass

        # Could not reach the Experian report. Keep the inline indicators
        # but mark partial so consumers know title-brand / odometer-brand
        # coverage is incomplete for this VIN.
        detail_page.screenshot(path=str(ac_artifact_dir / "autocheck-experian-missing.png"), full_page=True)
        if self._experian_short_circuit_tripped():
            return {
                "scrape_status": "partial",
                "failure_category": "experian_session_expired",
                "failure_stage": "popup",
                "failure_message": (
                    f"Experian session expired (hit "
                    f"{self._EXPERIAN_MAX_CONSECUTIVE_LOGIN_REDIRECTS}-redirect cap); "
                    "restart scraper to recover."
                ),
            }
        detail = "; ".join(failure_reasons[-8:])
        if not detail:
            detail = "no detail; no direct href and no popup produced a report"
        LOGGER.warning(
            "AutoCheck Experian report unreachable for VIN %s: %s",
            vin,
            detail,
        )
        category = "experian_report_unreachable"
        if any(reason.startswith("direct_timeout") or reason.startswith("popup_timeout") for reason in failure_reasons):
            category = "experian_timeout"
        elif any("login_" in reason for reason in failure_reasons):
            category = "experian_login_redirect"
        elif any(reason == "missing_href" for reason in failure_reasons):
            category = "experian_href_missing"
        elif any(reason.startswith("popup_") for reason in failure_reasons):
            category = "experian_popup_failed"
        return {
            "scrape_status": "partial",
            "failure_category": category,
            "failure_stage": "experian_report",
            "failure_message": (
                "Inline AutoCheck indicators captured but the full Experian "
                "report could not be opened; title-brand and odometer-brand "
                f"checks may be incomplete. Attempts: {detail}"
            ),
        }

    @staticmethod
    def _looks_like_login_page(text: str) -> bool:
        head = (text or "").lower()[:400]
        return ("sign in" in head) or ("username" in head) or ("password" in head and "log in" in head)

    @staticmethod
    def _is_experian_error_page(text: str) -> bool:
        """Detect Experian's 'Your request cannot be processed' rejection
        page.

        Observed on 5 of 188 reports captured 2026-04-25 — these are
        ~3KB stub pages Experian returns when the cs= session token in
        the AutoCheck URL has expired or when the VIN is not in their
        database. Without this detector the captures are stored as
        scrape_status='success' with empty AutoCheck fields, masking
        the real failure. Detected pages should be downgraded to
        scrape_status='partial' with a clear failure_category so
        downstream consumers know the data is missing.
        """
        head = (text or "").strip()[:400].lower()
        return "your request cannot be processed" in head

    @staticmethod
    def _url_is_auth_redirect(url: str) -> bool:
        """Definitive: the URL itself says this is a login page.

        Unlike text scanning, URL doesn't partially render — a page is
        either on auth.manheim.com or it isn't. Used to fast-fail
        auth-redirect detection without waiting the full content timeout,
        AND to avoid the 2026-04-24 false-positive class where a real
        Experian report mid-render contained 'Sign In' in its header nav.
        """
        if not url:
            return False
        lowered = url.lower()
        auth_hosts = ("auth.manheim.com", "signin.manheim", "login.manheim", "oauth.manheim")
        auth_paths = ("/as/authorization", "/signin", "/oauth2/authorize", "/pingfederate")
        return (
            any(host in lowered for host in auth_hosts)
            or any(path in lowered for path in auth_paths)
        )

    # Observed 2026-04-24: user reported Experian AutoCheck popups take
    # 5-8 seconds to actually populate, CR views 8-10 seconds. The prior
    # 3-second post-load wait was capturing partially-rendered shells
    # and the text-based login detector would false-positive on header
    # nav content containing "Sign In" during those partial renders —
    # triggering the short-circuit and losing AutoCheck for the rest of
    # the run. New timing budget: wait up to 12s for real content to
    # appear (body innerText > 500 chars is a cheap content-ready
    # signal), then fall back to a content-length check. URL-based
    # auth detection runs first and short-circuits in ~100ms on a real
    # auth redirect.
    _AUTOCHECK_POST_LOAD_TIMEOUT_MS = 12000
    _AUTOCHECK_CONTENT_READY_SIGNAL = "() => document.body && document.body.innerText && document.body.innerText.length > 500"

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
            "other_title_brand_specific_event_check",
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
        for key in (
            "score_range_low",
            "score_range_high",
            "comparable_vehicle_year",
            "comparable_vehicle_class",
            "historical_event_count",
            "owner_count",
            "accident_count",
            "last_reported_event_date",
            "last_reported_mileage",
        ):
            full_value = full_parsed.get(key)
            if full_value is not None and full_value != "":
                result[key] = full_value

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
    def _merge_autocheck_fallback(
        result: dict[str, Any],
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        """Fill missing AutoCheck fields from the OVE listing JSON.

        The embedded OVE listing object often has score/range/owner/accident
        facts even when the inline rendered AutoCheck widget is late or absent.
        It is not authoritative enough to mark a scrape successful, but it is
        valuable fallback data for partial captures and VPS rendering.
        """
        if not fallback:
            return result
        merged = dict(result)
        for key, value in fallback.items():
            if value is None or value == "":
                continue
            if merged.get(key) in (None, ""):
                merged[key] = value
        return merged

    @staticmethod
    def _parse_autocheck_listing_json(listing_json: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(listing_json, dict):
            return {}
        autocheck = listing_json.get("autocheck")
        if not isinstance(autocheck, dict):
            return {}

        result: dict[str, Any] = {}

        field_map = {
            "score": "autocheck_score",
            "ownerCount": "owner_count",
            "numberOfAccidents": "accident_count",
            "compareScoreRangeLow": "score_range_low",
            "compareScoreRangeHigh": "score_range_high",
        }
        for source_key, target_key in field_map.items():
            value = autocheck.get(source_key)
            if isinstance(value, int):
                result[target_key] = value

        def ok_value(value: object, ok_label: str = "OK", problem_label: str = "Problem Reported") -> str | None:
            if value is True:
                return ok_label
            if value is False:
                return problem_label
            return None

        title_status = ok_value(autocheck.get("titleAndProblemCheckOK"))
        if title_status:
            result["title_brand_check"] = title_status
            result["other_title_brand_specific_event_check"] = title_status

        odometer_status = ok_value(autocheck.get("odometerCheckOK"))
        if odometer_status:
            result["odometer_check"] = odometer_status

        use_event_status = ok_value(
            autocheck.get("vehicleUseAndEventCheckOK"),
            ok_label="OK",
            problem_label="Other Use Reported",
        )
        if use_event_status:
            result["vehicle_use"] = use_event_status

        accidents = result.get("accident_count")
        if isinstance(accidents, int):
            result["accident_check"] = (
                f"Information Reported({accidents})" if accidents > 0 else "OK"
            )

        return result

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
    def _clean_check_value(raw: str) -> str:
        """Strip the AutoCheck Snapshot Report's repeated 'More info'
        tooltip text from an extracted check value.

        Snapshots render every check section with two tooltip elements:
        ``OK \\n More info \\n More info``. The non-greedy regex match
        captures all of it. Truncating at the first 'More info' gives a
        clean value (``OK``, ``Problem Reported``, ``Personal Use``,
        ``Qualifies``, etc.) suitable for both DB persistence and VPS
        template rendering.
        """
        if not raw:
            return ""
        # Cut everything from the first "More info" onward.
        cleaned = re.split(r'\s*More\s+info\b', raw, maxsplit=1)[0]
        # Collapse internal whitespace (newlines, tabs) to a single space.
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    @staticmethod
    def _parse_autocheck_content(raw_text: str) -> dict[str, Any]:
        """Parse raw AutoCheck modal text into structured sections.

        Tuned for the AutoCheck Snapshot Report (the only tier the
        OVE-internal viewer returns — the Full Vehicle History Report
        requires Manheim Dealer Elite membership). The Snapshot is
        sufficient for screening: it includes the AutoCheck Score, owner
        count, accident count, and OK/Problem-Reported verdicts on the
        title-brand / accident / damage / odometer / vehicle-use /
        buyback-protection checks.
        """
        result: dict[str, Any] = {"raw_text": raw_text}

        sections = {
            "title_brand_check": r"(?:Major\s*)?(?:State\s*)?Title\s*Brand\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "accident_check": r"Accident\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "damage_check": r"Damage\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "odometer_check": r"Odometer\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            "vehicle_use": r"Vehicle\s*Usage?\s*Check\s*[-–—]?\s*(.*?)(?=\n[A-Z]|\Z)",
            # Anchored on the literal phrase "AutoCheck Buyback Protection"
            # with a REQUIRED hyphen separator, NOT on bare
            # "Buyback Protection". The Snapshot's title-brand explainer
            # block contains "...not qualified for Buyback Protection
            # Program. Fire brand..." which the prior regex matched first,
            # extracting "Program." as the value on 178 of 182 reports.
            "buyback_protection": r"AutoCheck\s+Buyback\s+Protection\s*[-–—]\s*(.*?)(?=\n[A-Z]|\Z)",
        }

        for key, pattern in sections.items():
            match = re.search(pattern, raw_text, re.IGNORECASE | re.DOTALL)
            # _clean_check_value strips the "More info \n More info"
            # tooltip noise the Snapshot renders on every check.
            result[key] = (
                PlaywrightCdpBrowserSession._clean_check_value(match.group(1))
                if match else ""
            )

        other_title_match = re.search(
            r"Other\s+Title\s+Brand\s+and\s+Specific\s+Event\s+Check\s*[-\u2013\u2014]?\s*(.*?)(?=\n[A-Z]|\Z)",
            raw_text,
            re.IGNORECASE | re.DOTALL,
        )
        result["other_title_brand_specific_event_check"] = (
            PlaywrightCdpBrowserSession._clean_check_value(other_title_match.group(1))
            if other_title_match else ""
        )

        # Extract score if present
        score_match = re.search(r"(?:AutoCheck\s*Score|score)\s*:?\s*(\d+)", raw_text, re.IGNORECASE)
        if score_match:
            result["autocheck_score"] = int(score_match.group(1))

        comparable_match = re.search(
            r"Other\s+comparable\s+(\d{4})\s+vehicles\s+in\s+the\s+(.+?)\s+"
            r"typically\s+score\s+between\s+(\d+)\s*[-\u2013\u2014]\s*(\d+)",
            raw_text,
            re.IGNORECASE | re.DOTALL,
        )
        if comparable_match:
            result["comparable_vehicle_year"] = int(comparable_match.group(1))
            result["comparable_vehicle_class"] = re.sub(r"\s+", " ", comparable_match.group(2)).strip()
            result["score_range_low"] = int(comparable_match.group(3))
            result["score_range_high"] = int(comparable_match.group(4))

        event_count_match = re.search(
            r"No\.\s*of\s*Historical\s*Events\s*:?\s*(\d+)",
            raw_text,
            re.IGNORECASE,
        )
        if event_count_match:
            result["historical_event_count"] = int(event_count_match.group(1))

        last_event_match = re.search(
            r"Last\s+Reported\s+Event\s+Date\s*:?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            raw_text,
            re.IGNORECASE,
        )
        if last_event_match:
            result["last_reported_event_date"] = last_event_match.group(1)

        last_mileage_match = re.search(
            r"Last\s+Reported\s+Mileage\s*:?\s*([0-9][0-9,]*)",
            raw_text,
            re.IGNORECASE,
        )
        if last_mileage_match:
            result["last_reported_mileage"] = last_mileage_match.group(1)

        # Extract accident count
        accident_match = re.search(r"Number\s*of\s*Accidents?\s*:?\s*(\d+)", raw_text, re.IGNORECASE)
        if accident_match:
            result["accident_count"] = int(accident_match.group(1))

        # Extract owner count. Note: 18% of Snapshot reports legitimately
        # OMIT this line entirely (typical of low-mileage / new vehicles
        # where Experian has not yet computed an owner count). Returning
        # None is the correct outcome in that case.
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
        # Tab-leak fix (2026-04-28): the original loop fell through to a
        # bare `raise` at the bottom without closing `page` if every
        # candidate URL failed to render the search input. Each failure
        # left the worker page navigated to one of the candidate URLs
        # (most often `/buy#/`), so a sustained auth-failure storm
        # accumulated `/buy#/` tabs in the visible Chrome window. We now
        # close the page on the failure path before re-raising so the
        # caller doesn't have to reason about page ownership across
        # exception boundaries.
        try:
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
        except BaseException:
            # Catch BaseException (not just Exception) so we still close
            # the leaked page even on KeyboardInterrupt / SystemExit
            # during recovery storms. The page is fresh-allocated by the
            # caller per call, so closing it here is always safe.
            self._close_page(page)
            raise

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
        if "/buy#/" in url or "/saved_searches#/" in url:
            return False
        if "/#/results" in url or "#/results" in url:
            return False
        detail_route = f"#/details/{vin_lower}/"
        if detail_route in url:
            return True
        try:
            return bool(
                page.evaluate(
                    """(needle) => {
                        const text = (document.body?.innerText || '').toLowerCase();
                        if (!text.includes(needle.toLowerCase())) return false;
                        const href = location.href.toLowerCase();
                        if (href.includes('/buy#/') || href.includes('/saved_searches#/')) return false;
                        if (href.includes('#/results') || href.includes('/#/results')) return false;
                        if (document.querySelector(
                            ".SearchResultsView__container, [data-test-id='search-results'], [data-test-id='search-results-view']"
                        )) return false;
                        if (href.includes(`#/details/${needle.toLowerCase()}/`)) return true;
                        return Boolean(
                            document.querySelector(
                                ".VehicleDetailsView__container, [data-test-id='vehicle-details-view-container'], [data-test-id='vehicle-details'], [class*='VehicleDetailsView']"
                            )
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
        #
        # Tab-leak fix (2026-04-28): the previous version called
        # _close_page() in the except branch but BEFORE the
        # artifact_dir.mkdir/write block. If artifact_dir.mkdir raised
        # (filesystem error, permission denied), the close would have
        # run, but if anything between new_page() and the close raised
        # while page was already assigned... actually the existing flow
        # is mostly OK. The real protection is to use try/finally so
        # the close ALWAYS runs on the failure path regardless of what
        # comes after, and to set `page = None` once we want to hand it
        # off so the finally can tell success from failure.
        page: Page | None = None
        success = False
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
            success = True
            return page
        except Exception as exc:
            LOGGER.warning("Direct-goto CR open failed for %s: %s", href, exc)
            try:
                artifact_dir.mkdir(parents=True, exist_ok=True)
                (artifact_dir / "cr-direct-goto-failed.txt").write_text(
                    f"href={href}\nerror={exc!r}\n", encoding="utf-8"
                )
            except Exception:
                pass
            return None
        finally:
            # Always close the page on the failure path, no matter how we
            # got there (catch handler, BaseException, etc.).
            if not success and page is not None:
                self._close_page(page)

    # Regex extracting the VIN segment from an OVE detail URL like
    # https://www.ove.com/search/results#/details/{VIN}/OVE  — used by
    # _open_via_hash_route to construct the conditionInformation hash.
    _OVE_DETAIL_VIN_RE = re.compile(r"#/details/([A-HJ-NPR-Z0-9]+)/", re.IGNORECASE)

    def _extract_vin_from_source_page(self, source_page: Page) -> str | None:
        """Extract the VIN from the OVE detail page URL hash.

        The OVE webapp uses fragment routing
        `https://www.ove.com/search/results#/details/{VIN}/OVE` for detail
        pages. The VIN is the only piece we need to construct a CR hash
        route — the rest is fixed structure.
        """
        try:
            url = source_page.url or ""
        except Exception:
            return None
        match = self._OVE_DETAIL_VIN_RE.search(url)
        return match.group(1).upper() if match else None

    def _open_via_hash_route(
        self,
        source_page: Page,
        vin: str,
        intended_href: str,
        artifact_dir: Path,
    ) -> Page | None:
        """Trigger the OVE React app's CR view by mutating the URL hash
        directly, bypassing the missing CR anchor.

        Background (2026-04-25 forensic): for listings whose
        condition_report_link.href points at insightcr.manheim.com, the
        OVE React app does NOT render the
        `<a data-test-id="condition-report">` anchor in the detail page
        DOM — apparently because the conditionReportUrl in OVE's listing
        JSON is protocol-relative (`//insightcr.manheim.com/...`) which
        the VehicleReportLink component does not handle. The click flow
        below therefore can never find a target. 100% of insightcr
        listings fail at "Condition report locator not found on OVE
        detail page" — confirmed across 200+ historical attempts (zero
        successes via _click_condition_report_locator).

        However, the OVE webapp's React Router DOES respond to direct
        hash mutation. Setting window.location.hash to
        `#/details/{VIN}/OVE/conditionInformation` triggers the same
        route handler the click would have, which performs the
        server-side SSO bounce and renders the CR view.

        Returns source_page on success (now displaying the CR view), or
        None on failure (caller should fall back to the click flow).
        Raises ManheimAuthRedirectError if the route lands on a
        Manheim auth or "CR not available" page.
        """
        target_hash = f"#/details/{vin}/OVE/conditionInformation"
        LOGGER.info(
            "Attempting CR hash-route navigation for VIN %s (intended_href=%s)",
            vin, intended_href,
        )
        try:
            # Plain `window.location.hash = ...` triggers React Router's
            # popstate/hashchange listener. We use it explicitly rather
            # than goto(url + hash) because Playwright's goto can be
            # confused by hash-only navigations.
            source_page.evaluate(
                "(hash) => { window.location.hash = hash; }",
                target_hash,
            )
        except Exception as exc:
            LOGGER.warning("Hash-route evaluate failed for VIN %s: %s", vin, exc)
            return None

        # Poll up to 20s for the route to settle. Same budget as
        # _click_condition_report_locator's poll loop.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            try:
                current_url = source_page.url or ""
            except Exception:
                current_url = ""
            if self._CR_HASH_ROUTE_RE.search(current_url):
                # Route confirmed; let the React app finish mounting the
                # CR view (same wait_for_load_state networkidle the
                # click flow uses).
                try:
                    source_page.wait_for_load_state("networkidle", timeout=10000)
                except PlaywrightTimeoutError:
                    pass
                # Defense in depth: confirm we're not on auth or the
                # "CR not available right now" page. These raise
                # ManheimAuthRedirectError which the caller propagates.
                self._raise_if_auth_redirect(
                    source_page, artifact_dir, "hash_route_post_nav", intended_href,
                )
                if self._is_cr_unavailable_page(source_page):
                    raise ManheimAuthRedirectError(
                        f"Manheim returned 'condition reports are not available' "
                        f"for hash-route nav to {target_hash} "
                        f"(intended_href={intended_href}); "
                        "treating as stale-session auth failure."
                    )
                LOGGER.info(
                    "CR hash-route navigation succeeded for VIN %s (url=%s)",
                    vin, current_url,
                )
                return source_page
            source_page.wait_for_timeout(250)

        LOGGER.info(
            "CR hash-route navigation did not produce expected route within 20s "
            "for VIN %s; will fall back to click flow",
            vin,
        )
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
        # 2026-04-25: for listings whose CR is hosted on
        # insightcr.manheim.com, the OVE detail page DOES NOT render the
        # CR anchor at all (React leaves a hollow
        # <span class="VehicleReportLink"></span>) — so the click flow's
        # _find_condition_report_locator search returns None for 100% of
        # those listings. We now attempt a direct hash-route navigation
        # FIRST, which produces the same React route change a click
        # would have. The click flow remains as a fallback for listings
        # whose anchor IS rendered (e.g., inspectionreport listings that
        # somehow get routed here despite the dispatcher).
        #
        # We do NOT pass `href` to a goto here. We use the href ONLY to
        # label debug artifacts on failure; navigation happens via
        # in-page hash mutation OR via the locator click.
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
        hash_route_attempted = False
        context = source_page.context

        def _on_popup(new_page: Page) -> None:
            collected_popups.append(new_page)

        context.on("page", _on_popup)
        try:
            # Strategy 1 (2026-04-25): direct hash-route navigation. Use
            # this first because for insightcr-hosted listings the click
            # target literally doesn't exist in the DOM, and the click
            # flow below has a 100% historical failure rate on those.
            # For listings whose anchor IS rendered the hash route still
            # works (it's the same destination the click would produce),
            # so we lose nothing by trying it first.
            vin = self._extract_vin_from_source_page(source_page)
            if vin:
                hash_route_attempted = True
                try:
                    hash_cr_page = self._open_via_hash_route(
                        source_page, vin, href, artifact_dir,
                    )
                except ManheimAuthRedirectError:
                    # Hash-route definitively landed on auth or "not
                    # available". Propagate; click flow won't recover
                    # from that either.
                    raise
                if hash_cr_page is not None:
                    LOGGER.info(
                        "Opened OVE-internal condition report view via hash-route "
                        "for VIN %s (url=%s)",
                        vin, hash_cr_page.url,
                    )
                    claimed_page = hash_cr_page
                    return hash_cr_page
                LOGGER.info(
                    "Hash-route did not produce CR view for VIN %s; "
                    "falling back to anchor-click flow",
                    vin,
                )
            else:
                LOGGER.info(
                    "Could not extract VIN from source_page url=%s; "
                    "skipping hash-route attempt",
                    source_page.url if hasattr(source_page, 'url') else '<unknown>',
                )

            # Strategy 2: click-loop fallback for listings whose CR
            # anchor IS in the DOM.
            for attempt in range(max_attempts):
                try:
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
                        # Re-raise so the outer ManheimAuthRedirectError
                        # handler below can attempt single-shot login
                        # recovery before giving up.
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
                except ManheimAuthRedirectError:
                    # Inline single-shot login recovery. Per user 2026-04-24:
                    # Chrome typically has credentials pre-filled on the
                    # Manheim auth form, so a single click on Sign In
                    # is usually enough to restore SSO. The recovery
                    # helper is gated by the process-wide
                    # _auto_login_last_attempt_at timestamp + 6h cooldown
                    # (Fix 2, 2026-04-30), so this shares its
                    # one-attempt-per-cooldown budget with startup,
                    # keepalive, and AutoCheck paths. No tight-loop
                    # retry is possible.
                    if attempt < max_attempts - 1 and self._recover_cr_auth_inline(
                        source_page, collected_popups, f"cr-click-recovery-{attempt+1}"
                    ):
                        LOGGER.info(
                            "CR-click: auth recovery succeeded on attempt %s/%s; retrying click",
                            attempt + 1,
                            max_attempts,
                        )
                        # Fresh slate for post-recovery attempt — old
                        # popups carry stale auth state.
                        # Tab-leak fix (Fix E, 2026-04-28): the prior
                        # version called .clear() WITHOUT closing the
                        # popups first. The orphaned auth tabs persisted
                        # in Chrome (no longer tracked, finally block
                        # had nothing to close) and accumulated across
                        # VINs — a contributor to the 29-tab incident.
                        # Close every popup before dropping it from the
                        # tracking list. claimed_page is None at this
                        # point in the flow (we wouldn't be in the
                        # except handler if a CR view had been claimed),
                        # so no risk of closing the page we want to keep.
                        for _stale_popup in collected_popups:
                            self._close_page(_stale_popup)
                        collected_popups.clear()
                        source_page.wait_for_timeout(1500)
                        continue
                    # Recovery failed, flag already consumed, or we're
                    # out of attempts. Re-raise so the outer caller
                    # routes this to auth_expired as designed.
                    raise

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
            try:
                final_url = source_page.url
            except Exception:
                final_url = "<unknown>"
            raise ConditionReportClickFailedError(
                f"Could not open OVE condition report; "
                f"intended_href={href}; "
                f"hash_route_attempted={hash_route_attempted}; "
                f"click_attempts={max_attempts}; "
                f"final_source_url={final_url}; "
                f"last_error={last_error}"
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

    # Phrases Manheim uses on its account-locked / too-many-attempts
    # error pages. Conservative: must match one of the strong phrases
    # OR (the word "locked" AND the word "account") to avoid false
    # positives on unrelated pages that happen to mention "locked".
    _MANHEIM_ACCOUNT_LOCKED_STRONG_PHRASES: tuple[str, ...] = (
        "your account has been locked",
        "your account is locked",
        "account has been temporarily locked",
        "account is temporarily locked",
        "too many failed login attempts",
        "too many login attempts",
        "too many sign in attempts",
        "too many sign-in attempts",
        "your account is currently locked",
    )

    def _is_manheim_account_locked_page(self, page: Page) -> bool:
        """Detect Manheim's account-locked error page so the scraper
        can record a long cooldown and stop attempting auth.

        Conservative on purpose: a false positive sets a 6-hour
        cooldown, which is annoying but recoverable; a false negative
        means we keep trying and trigger an actual Manheim lockout,
        which is much worse. Match strong phrases first; fall back to
        ('locked' AND 'account') only inside a narrow head-of-page
        window to limit scope."""
        try:
            text = page.evaluate(
                "() => (document.body && document.body.innerText ? document.body.innerText : '').toLowerCase()"
            )
        except Exception:
            return False
        if not isinstance(text, str):
            return False
        head = text[:3000]
        for phrase in self._MANHEIM_ACCOUNT_LOCKED_STRONG_PHRASES:
            if phrase in head:
                return True
        # Fuzzier conservative match: 'locked' and 'account' AND one of
        # the lockout-context cues, all near the top of the page.
        if "locked" in head and "account" in head:
            if any(cue in head for cue in ("contact", "support", "try again later", "try later", "wait")):
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

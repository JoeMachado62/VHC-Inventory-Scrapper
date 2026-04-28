from __future__ import annotations

import smtplib
import ssl
import time
from email.message import EmailMessage
from typing import Any


class AdminNotifier:
    def __init__(
        self,
        *,
        smtp_host: str = "",
        smtp_port: int = 587,
        smtp_username: str = "",
        smtp_password: str = "",
        smtp_use_tls: bool = True,
        from_email: str = "",
        admin_alert_email: str = "",
        cooldown_seconds: int = 3600,
    ) -> None:
        self.smtp_host = smtp_host.strip()
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username.strip()
        self.smtp_password = smtp_password
        self.smtp_use_tls = smtp_use_tls
        self.from_email = from_email.strip()
        self.admin_alert_email = admin_alert_email.strip()
        self.cooldown_seconds = max(60, cooldown_seconds)
        self._last_sent_by_key: dict[str, float] = {}

    def notify_browser_auth_lost(self, *, reason: str, context: dict[str, Any] | None = None, logger=None) -> bool:
        # Path 2 / two-Chrome architecture (2026-04-28): the alert subject
        # and dedupe key now disambiguate which Chrome lost its session.
        # Pre-fix the subject was "OVE scraper login required" with no
        # indication of which login — the user couldn't tell whether to
        # re-auth Login A (port 9222, hot-deal/deep-scrape) or Login B
        # (port 9223, sync). Subject now reads e.g.
        # "OVE scraper login required: Login A (port 9222)".
        port = (context or {}).get("chrome_debug_port")
        if port == 9223:
            login_label = "Login B (port 9223, sync)"
            cooldown_key = "browser-auth-lost:9223"
        elif port == 9222:
            login_label = "Login A (port 9222, hot-deal/deep-scrape)"
            cooldown_key = "browser-auth-lost:9222"
        else:
            login_label = f"Chrome on port {port}" if port else "OVE Chrome session"
            cooldown_key = f"browser-auth-lost:{port or 'unknown'}"

        subject = f"OVE scraper login required: {login_label}"
        body_lines = [
            f"The {login_label} lost its authenticated browser session and could not recover automatically.",
            "",
            f"Reason: {reason}",
        ]
        if context:
            body_lines.append("")
            body_lines.extend(f"{key}: {value}" for key, value in context.items())
        body_lines.append("")
        body_lines.append(
            f"Action required: switch to the {login_label} Chrome window "
            "and log back into OVE manually. Other Chrome sessions are unaffected."
        )
        return self._send_with_cooldown(
            key=cooldown_key,
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_snapshot_safety_gate_blocked(
        self,
        *,
        proposed_count: int,
        last_count: int,
        threshold_pct: int,
        context: dict[str, Any] | None = None,
        logger=None,
    ) -> bool:
        subject = "OVE scraper REFUSED to push undersized snapshot"
        body_lines = [
            "The OVE inventory sync produced a merged snapshot that fell below the safety threshold.",
            "The push to the VPS was BLOCKED to prevent the live inventory from being clobbered by",
            "a partial / broken OVE export. The current VPS inventory is unchanged.",
            "",
            f"Proposed row count:        {proposed_count}",
            f"Last successful row count: {last_count}",
            f"Required threshold:        {threshold_pct}% ({int(last_count * threshold_pct / 100)} rows minimum)",
        ]
        if context:
            body_lines.append("")
            body_lines.extend(f"{key}: {value}" for key, value in context.items())
        body_lines.append("")
        body_lines.append(
            "Action required: investigate why the OVE export shrank. Check the saved-search "
            "exports manually, look at logs/ove_scraper.log for selector / DOM warnings, and "
            "re-run sync-once after the cause is resolved."
        )
        return self._send_with_cooldown(
            key="snapshot-safety-gate-blocked",
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_export_failed(
        self,
        *,
        search_name: str,
        attempts: int,
        last_error: str,
        debug_artifact_dir: str,
        logger=None,
    ) -> bool:
        subject = f"OVE scraper export FAILED for saved search '{search_name}'"
        body_lines = [
            "The OVE inventory sync failed to export a saved search after exhausting all retries.",
            "The hourly sync has been aborted because the merged snapshot would be incomplete.",
            "",
            f"Saved search:   {search_name}",
            f"Attempts:       {attempts}",
            f"Last error:     {last_error}",
            f"Debug artifacts: {debug_artifact_dir}",
            "",
            "Action required: open the debug HTML / screenshot to see what the OVE saved-search",
            "page looked like at failure time. Likely causes: OVE UI selector change, the saved",
            "search has been deleted in OVE, the saved search is genuinely empty, or the dedicated",
            "OVE Chrome profile has been logged out.",
        ]
        return self._send_with_cooldown(
            key=f"export-failed:{search_name}",
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_sync_success(
        self,
        *,
        east_count: int,
        west_count: int,
        total_vehicles: int,
        duplicates_removed: int,
        searches_exported: list[str],
        logger=None,
    ) -> bool:
        subject = f"OVE inventory sync OK — {total_vehicles} vehicles pushed"
        body_lines = [
            "The OVE inventory sync completed successfully.",
            "",
            f"East Hub records:    {east_count}",
            f"West Hub records:    {west_count}",
            f"Duplicates removed:  {duplicates_removed}",
            f"Vehicles pushed:     {total_vehicles}",
            f"Searches exported:   {', '.join(searches_exported)}",
        ]
        return self._send_email_unchecked(
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_hot_deal_complete(
        self,
        *,
        run_summary: dict[str, Any],
        hot_deals: list[dict[str, Any]],
        logger=None,
    ) -> bool:
        from ove_scraper.hot_deal_report import format_hot_deal_email_html, format_hot_deal_summary
        found = run_summary.get("hot_deals", 0)
        total = run_summary.get("total_vins", 0)
        subject = f"Hot Deal Screening Complete: {found} candidates found ({total} screened)"
        body = format_hot_deal_summary(run_summary, hot_deals)
        return self._send_with_cooldown(key="hot-deal-complete", subject=subject, body=body, logger=logger)

    def notify_hot_deal_cluster_rejection(
        self,
        *,
        clusters: list[dict[str, Any]],
        run_id: str,
        total_screened: int,
        logger=None,
    ) -> bool:
        """Alert when many VINs in one run reject for the identical reason.

        Real-world precedent (2026-04-26): a parser regex matched a UI
        label and false-rejected 17 vehicles with "Structural damage
        reported". The pipeline finished cleanly; only the trailing
        WARNING lines hinted at the problem and they were easy to miss.
        This alert turns the bug pattern into an inbox event.
        """
        if not clusters:
            return False
        biggest = clusters[0]
        subject = (
            f"Hot Deal: {biggest['count']} VINs rejected for the same reason "
            f"({biggest['reason'][:60]})"
        )
        body_lines = [
            "The Hot Deal pipeline detected one or more rejection-reason clusters.",
            "A cluster means many VINs failed step 1, 2, or 3 with the IDENTICAL",
            "rejection reason — strongly suggesting a screener bug rather than",
            "many independently-bad vehicles. Investigate before relying on the",
            "hot deal list this run produced.",
            "",
            f"Run ID:         {run_id}",
            f"Total screened: {total_screened}",
            "",
            "Clusters detected:",
        ]
        for c in clusters:
            body_lines.append(f"  - {c['count']:>4} VINs  |  {c['reason']}")
            body_lines.append(f"           sample: {', '.join(c['sample_vins'][:5])}")
        body_lines.append("")
        body_lines.append(
            "Action: pull one of the sample VINs' artifacts (artifacts/<VIN>/) "
            "and verify the rejection is real. If not, the screener regex or "
            "data-source extraction needs fixing."
        )
        return self._send_with_cooldown(
            key=f"hot-deal-cluster-reject:{biggest['reason'][:80]}",
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_hot_deal_push_zero(
        self,
        *,
        hot_deal_rows_count: int,
        missing_payload_count: int,
        skipped_at_build_count: int,
        skipped_sample: list[str],
        logger=None,
    ) -> bool:
        """Alert when the curated batch is empty but DB has hot_deal rows.

        Real-world precedent (2026-04-26): the MMR extractor only knew
        the legacy ``priceRange`` schema but production listings used
        ``mmrPrice``. Every VIN was skipped at batch-build time, the
        batch came out empty, and the VPS push silently sent nothing.
        The user only noticed when the marketing area was still empty
        the next day.
        """
        subject = (
            f"Hot Deal VPS push SKIPPED — batch empty despite "
            f"{hot_deal_rows_count} hot_deal rows in DB"
        )
        body_lines = [
            "The Hot Deal pipeline finished with hot_deal-status rows in the DB,",
            "but the VPS push was SKIPPED because the curated batch came out empty.",
            "",
            "This almost always means a payload-builder bug or a schema drift in",
            "the OVE listing JSON. Symptom: the marketing list won't update.",
            "",
            f"Hot deal rows in DB:        {hot_deal_rows_count}",
            f"Missing payload-data.json:  {missing_payload_count}",
            f"Skipped at batch build:     {skipped_at_build_count}",
        ]
        if skipped_sample:
            body_lines.append(f"Sample skipped VINs:        {', '.join(skipped_sample[:5])}")
        body_lines.append("")
        body_lines.append(
            "Action: pull one skipped VIN's artifacts/hot-deal/<VIN>/payload-data.json "
            "and step through ove_scraper.hot_deal_payload.build_deal_entry to find "
            "which required field is missing. Most likely culprits: MMR extraction, "
            "auction_end_at, or year/make/model on the listing JSON."
        )
        return self._send_with_cooldown(
            key="hot-deal-push-zero",
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def notify_hot_deal_pipeline_failed(
        self,
        *,
        attempts: int,
        last_error: str,
        logger=None,
    ) -> bool:
        subject = f"Hot Deal Pipeline FAILED after {attempts} attempts today"
        body_lines = [
            "The daily Hot Deal screening pipeline exhausted its retry budget for today.",
            "Qualified-VIN marketing list was NOT refreshed today.",
            "",
            f"Attempts exhausted: {attempts}",
            f"Last error:         {last_error}",
            "",
            "Action required: inspect logs/ove_scraper.log for the Hot Deal pipeline entries,",
            "confirm the OVE browser session is authenticated, verify the 'VCH Marketing List'",
            "saved search still exists in OVE, and run `python -m ove_scraper.main hot-deal`",
            "manually to reproduce. The daily auto-run will try again at tomorrow's scheduled slot.",
        ]
        return self._send_with_cooldown(
            key="hot-deal-pipeline-failed",
            subject=subject,
            body="\n".join(body_lines),
            logger=logger,
        )

    def _send_email_unchecked(self, *, subject: str, body: str, logger=None) -> bool:
        """Send an email without cooldown gating. Use for notifications
        that should always be delivered (e.g. sync success)."""
        if not self.is_configured():
            if logger:
                logger.warning("Admin notifier is not configured; skipping notification")
            return False
        try:
            self._send_email(subject=subject, body=body)
            if logger:
                logger.warning("Sent notification '%s' to %s", subject, self.admin_alert_email)
            return True
        except Exception as exc:
            if logger:
                logger.warning("Failed to send notification: %s", exc)
            return False

    def _send_with_cooldown(self, *, key: str, subject: str, body: str, logger=None) -> bool:
        if not self.is_configured():
            if logger:
                logger.warning("Admin notifier is not configured; skipping alert '%s'", key)
            return False

        now = time.monotonic()
        last_sent = self._last_sent_by_key.get(key, 0.0)
        if now - last_sent < self.cooldown_seconds:
            if logger:
                logger.info("Skipping alert '%s'; cooldown active", key)
            return False

        self._send_email(subject=subject, body=body)
        self._last_sent_by_key[key] = now
        if logger:
            logger.warning("Sent admin alert '%s' to %s", key, self.admin_alert_email)
        return True

    def is_configured(self) -> bool:
        return bool(
            self.smtp_host
            and self.from_email
            and self.admin_alert_email
            and (self.smtp_username or not self.smtp_password)
        )

    def _send_email(self, *, subject: str, body: str) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.from_email
        message["To"] = self.admin_alert_email
        message.set_content(body)

        if self.smtp_use_tls:
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as smtp:
                smtp.starttls(context=context)
                if self.smtp_username:
                    smtp.login(self.smtp_username, self.smtp_password)
                smtp.send_message(message)
            return

        with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=30) as smtp:
            if self.smtp_username:
                smtp.login(self.smtp_username, self.smtp_password)
            smtp.send_message(message)

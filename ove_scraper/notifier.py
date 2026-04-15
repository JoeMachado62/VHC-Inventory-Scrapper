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
        subject = "OVE scraper login required"
        body_lines = [
            "The OVE scraper lost its authenticated browser session and could not recover automatically.",
            "",
            f"Reason: {reason}",
        ]
        if context:
            body_lines.append("")
            body_lines.extend(f"{key}: {value}" for key, value in context.items())
        body_lines.append("")
        body_lines.append("Action required: log back into the dedicated OVE Chrome session.")
        return self._send_with_cooldown(
            key="browser-auth-lost",
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

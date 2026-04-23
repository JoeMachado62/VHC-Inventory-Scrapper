from __future__ import annotations

import logging
import os
from dataclasses import dataclass
import socket
from pathlib import Path

from ove_scraper.resource_utils import recommend_deep_scrape_workers


def load_env_file(path: str = ".env") -> None:
    candidates = [Path(path)]
    project_root = Path(__file__).resolve().parent.parent
    candidates.append(project_root / path)

    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())
        return


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


@dataclass(slots=True)
class Settings:
    vch_api_base_url: str
    vch_service_token: str
    chrome_debug_host: str = "127.0.0.1"
    chrome_debug_port: int = 9222
    sync_interval_seconds: int = 3600
    deep_scrape_poll_interval_seconds: int = 30
    # Reduced from 900s (15 min) to 300s (5 min) per the 2026-04-11
    # investigation: the 15-min interval was too slow to detect Chrome CDP
    # drops and left the OVE session cold overnight, causing 9 AM React
    # hydration failures. 5-min keepalives keep the session warm and detect
    # dead CDP within one cycle. Tunable via BROWSER_KEEPALIVE_INTERVAL_SECONDS.
    browser_keepalive_interval_seconds: int = 300
    deep_scrape_max_workers: int = 1
    deep_scrape_lease_seconds: int = 900
    deep_scrape_retry_delay_seconds: int = 300
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    alert_from_email: str = ""
    admin_alert_email: str = ""
    admin_alert_cooldown_seconds: int = 3600
    not_found_confirm_attempts: int = 3
    ims_refresh_start_hour_eastern: int = 16
    ims_refresh_end_hour_eastern: int = 17
    # Sync window in Eastern time. Per the 2026-04-08 VPS handoff, extended
    # from 9 AM – 9 PM (12h) to 6 AM – 11 PM (17h) so the daemon spends
    # less of the day idle and vehicles.updated_at stays fresher for the
    # /health endpoint. Tunable via SYNC_WINDOW_START_HOUR_EASTERN and
    # SYNC_WINDOW_END_HOUR_EASTERN env vars.
    sync_window_start_hour_eastern: int = 6
    sync_window_end_hour_eastern: int = 23
    # Fixed wall-clock slots (Eastern Time) at which the saved-searches sync
    # fires. Replaces the legacy interval-based cadence (sync_interval_seconds
    # is retained as a no-op compat field). The 17:30 slot is shifted from
    # 17:00 to clear the Manheim IMS refresh window (~4-5 PM ET) which causes
    # transient "not found" results. Tunable via SYNC_SCHEDULE_EASTERN env
    # var as a comma-separated list of HH or HH:MM entries.
    sync_schedule_eastern: tuple[tuple[int, int], ...] = (
        (9, 0), (11, 0), (13, 0), (15, 0), (17, 30), (19, 0), (21, 0), (23, 0),
    )
    ove_required_search_count: int = 6
    scraper_node_id: str = "unknown-node"
    scraper_profile_slug: str = "default"
    scraper_version: str = "0.1.0"
    export_dir: Path = Path("./exports")
    artifact_dir: Path = Path("./artifacts")
    data_dir: Path = Path("./data")
    # Snapshot safety gate: refuse to push the new merged snapshot if it has
    # fewer than this percentage of the rows in the last successfully pushed
    # snapshot. Prevents a partial OVE export from clobbering the live VPS DB.
    # Set to 0 to disable the gate entirely (NOT recommended).
    ove_ingest_size_threshold_pct: int = 75
    ove_export_max_attempts: int = 5
    log_level: str = "INFO"
    log_file_path: Path = Path("./logs/ove_scraper.log")
    enabled: bool = True
    ove_base_url: str = "https://www.ove.com"
    ove_listings_url: str = "https://www.ove.com"
    ove_source_platform: str = "manheim"
    ove_east_searches: tuple[str, ...] = (
        "East Hub 2022-2023",
        "East Hub 2024",
        "East Hub 2025 or Newer",
    )
    ove_west_searches: tuple[str, ...] = (
        "West Hub 2015-2023",
        "West Hub 2024 or Newer",
    )
    ove_search_input_selector: str = "input[type='search'], input[placeholder*='VIN'], input[name*='search']"
    ove_result_link_selector: str = "a[href*='/vehicle/'], a[href*='/listing/']"
    ove_export_button_selector: str = "button:has-text('Export'), a:has-text('Export')"
    ove_saved_search_link_selector: str = "a:has-text('{search_name}'), button:has-text('{search_name}')"
    ove_section_root_selector: str = "main, [role='main'], body"
    # Hot Deal pipeline settings
    hot_deal_searches: tuple[str, ...] = (
        "Factory Warranty Active",
        "VCH Marketing List",
    )
    hot_deal_db_path: Path = Path("./data/hot_deal.db")
    # Daily Hot Deal auto-run integration. When enabled, the main run loop
    # fires HotDealPipelineRunner once per Eastern calendar day at the
    # first unmet slot in hot_deal_daily_schedule_eastern. State is
    # persisted at artifacts/_state/hot_deal_daily_state.json so a
    # machine reboot doesn't lose the fact that today already ran (or
    # that today still needs to catch up). Default slot is 07:00 ET —
    # before the 09:00 inventory sync and well clear of the 16:00-17:00
    # Manheim IMS refresh window.
    hot_deal_enabled: bool = True
    hot_deal_daily_schedule_eastern: tuple[tuple[int, int], ...] = ((7, 0),)
    hot_deal_retry_delay_seconds: int = 1800
    hot_deal_max_daily_attempts: int = 3
    # If a state file shows status="started" but last_run_at is older
    # than this many seconds, the previous run is assumed crashed and
    # eligible for retry. Conservative default: 2 hours (pipeline runs
    # observed at ~50 min for 100 VINs; doubled for headroom).
    hot_deal_stale_start_seconds: int = 7200
    openai_api_key: str = ""
    openai_model: str = "gpt-5.4"

    @classmethod
    def from_env(cls) -> "Settings":
        base_url = os.getenv("VCH_API_BASE_URL", "").rstrip("/")
        service_token = os.getenv("VCH_SERVICE_TOKEN", "")
        if not base_url:
            raise ValueError("VCH_API_BASE_URL is required")
        if not service_token:
            raise ValueError("VCH_SERVICE_TOKEN is required")

        return cls(
            vch_api_base_url=base_url,
            vch_service_token=service_token,
            chrome_debug_host=os.getenv("CHROME_DEBUG_HOST", "127.0.0.1"),
            chrome_debug_port=_get_int("CHROME_DEBUG_PORT", 9222),
            sync_interval_seconds=_get_int("SYNC_INTERVAL_SECONDS", 3600),
            deep_scrape_poll_interval_seconds=_get_int("DEEP_SCRAPE_POLL_INTERVAL_SECONDS", 30),
            browser_keepalive_interval_seconds=_get_int("BROWSER_KEEPALIVE_INTERVAL_SECONDS", 300),
            deep_scrape_max_workers=_get_int("DEEP_SCRAPE_MAX_WORKERS", recommend_deep_scrape_workers()),
            deep_scrape_lease_seconds=_get_int("DEEP_SCRAPE_LEASE_SECONDS", 900),
            deep_scrape_retry_delay_seconds=_get_int("DEEP_SCRAPE_RETRY_DELAY_SECONDS", 300),
            smtp_host=os.getenv("SMTP_HOST", ""),
            smtp_port=_get_int("SMTP_PORT", 587),
            smtp_username=os.getenv("SMTP_USERNAME", ""),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            smtp_use_tls=_get_bool("SMTP_USE_TLS", True),
            alert_from_email=os.getenv("ALERT_FROM_EMAIL", ""),
            admin_alert_email=os.getenv("ADMIN_ALERT_EMAIL", ""),
            admin_alert_cooldown_seconds=_get_int("ADMIN_ALERT_COOLDOWN_SECONDS", 3600),
            not_found_confirm_attempts=_get_int("NOT_FOUND_CONFIRM_ATTEMPTS", 3),
            ims_refresh_start_hour_eastern=_get_int("IMS_REFRESH_START_HOUR_EASTERN", 16),
            ims_refresh_end_hour_eastern=_get_int("IMS_REFRESH_END_HOUR_EASTERN", 17),
            sync_window_start_hour_eastern=_get_int("SYNC_WINDOW_START_HOUR_EASTERN", 6),
            sync_window_end_hour_eastern=_get_int("SYNC_WINDOW_END_HOUR_EASTERN", 23),
            sync_schedule_eastern=_get_schedule_slots(
                "SYNC_SCHEDULE_EASTERN",
                ((9, 0), (11, 0), (13, 0), (15, 0), (17, 30), (19, 0), (21, 0), (23, 0)),
            ),
            ove_required_search_count=_get_int("OVE_REQUIRED_SEARCH_COUNT", 6),
            scraper_node_id=os.getenv("SCRAPER_NODE_ID", socket.gethostname().lower() or "unknown-node"),
            scraper_profile_slug=os.getenv("SCRAPER_PROFILE_SLUG", "default"),
            scraper_version=os.getenv("SCRAPER_VERSION", "0.1.0"),
            export_dir=Path(os.getenv("EXPORT_DIR", "./exports")),
            artifact_dir=Path(os.getenv("ARTIFACT_DIR", "./artifacts")),
            data_dir=Path(os.getenv("DATA_DIR", "./data")),
            ove_ingest_size_threshold_pct=_get_int("OVE_INGEST_SIZE_THRESHOLD_PCT", 75),
            ove_export_max_attempts=_get_int("OVE_EXPORT_MAX_ATTEMPTS", 5),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_file_path=Path(os.getenv("LOG_FILE_PATH", "./logs/ove_scraper.log")),
            enabled=_get_bool("OVE_SYNC_ENABLED", True),
            ove_base_url=os.getenv("OVE_BASE_URL", "https://www.ove.com").rstrip("/"),
            ove_listings_url=os.getenv("OVE_LISTINGS_URL", "https://www.ove.com"),
            ove_source_platform=os.getenv("OVE_SOURCE_PLATFORM", "manheim"),
            ove_east_searches=_get_list(
                "OVE_EAST_SEARCHES",
                (
                    "East Hub 2022-2023",
                    "East Hub 2024",
                    "East Hub 2025 or Newer",
                ),
            ),
            ove_west_searches=_get_list(
                "OVE_WEST_SEARCHES",
                (
                    "West Hub 2015-2023",
                    "West Hub 2024 or Newer",
                ),
            ),
            ove_search_input_selector=os.getenv(
                "OVE_SEARCH_INPUT_SELECTOR",
                "input[type='search'], input[placeholder*='VIN'], input[name*='search']",
            ),
            ove_result_link_selector=os.getenv(
                "OVE_RESULT_LINK_SELECTOR",
                "a[href*='/vehicle/'], a[href*='/listing/']",
            ),
            ove_export_button_selector=os.getenv(
                "OVE_EXPORT_BUTTON_SELECTOR",
                "button:has-text('Export'), a:has-text('Export')",
            ),
            ove_saved_search_link_selector=os.getenv(
                "OVE_SAVE_SEARCH_LINK_SELECTOR",
                "a:has-text('{search_name}'), button:has-text('{search_name}')",
            ),
            ove_section_root_selector=os.getenv("OVE_SECTION_ROOT_SELECTOR", "main, [role='main'], body"),
            hot_deal_searches=_get_list(
                "HOT_DEAL_SEARCHES",
                ("Factory Warranty Active", "VCH Marketing List"),
            ),
            hot_deal_db_path=Path(os.getenv("HOT_DEAL_DB_PATH", "./data/hot_deal.db")),
            hot_deal_enabled=_get_bool("HOT_DEAL_ENABLED", True),
            hot_deal_daily_schedule_eastern=_get_schedule_slots(
                "HOT_DEAL_DAILY_SCHEDULE_EASTERN",
                ((7, 0),),
            ),
            hot_deal_retry_delay_seconds=_get_int("HOT_DEAL_RETRY_DELAY_SECONDS", 1800),
            hot_deal_max_daily_attempts=_get_int("HOT_DEAL_MAX_DAILY_ATTEMPTS", 3),
            hot_deal_stale_start_seconds=_get_int("HOT_DEAL_STALE_START_SECONDS", 7200),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4"),
        )

    @property
    def detail_worker_id(self) -> str:
        return f"scraper-{self.scraper_node_id}-{self.scraper_profile_slug}"


def _get_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    items = tuple(part.strip() for part in value.split("|") if part.strip())
    return items or default


def _get_schedule_slots(
    name: str,
    default: tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    """Parse a comma-separated list of HH or HH:MM entries (Eastern time)
    into a sorted tuple of (hour, minute) pairs. On any parse error, log a
    warning and fall back to the supplied default rather than crashing —
    a malformed schedule env var should not take down the scraper.
    """
    value = os.getenv(name)
    if value is None:
        return default
    parsed: list[tuple[int, int]] = []
    try:
        for raw in value.split(","):
            entry = raw.strip()
            if not entry:
                continue
            if ":" in entry:
                hour_str, minute_str = entry.split(":", 1)
                hour = int(hour_str)
                minute = int(minute_str)
            else:
                hour = int(entry)
                minute = 0
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError(f"slot out of range: {entry}")
            parsed.append((hour, minute))
    except (ValueError, IndexError) as exc:
        logging.getLogger(__name__).warning(
            "Failed to parse %s=%r (%s); falling back to default schedule %s",
            name, value, exc, default,
        )
        return default
    if not parsed:
        return default
    return tuple(sorted(set(parsed)))

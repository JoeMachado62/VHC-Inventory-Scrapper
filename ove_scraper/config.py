from __future__ import annotations

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
    browser_keepalive_interval_seconds: int = 900
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
    sync_window_start_hour_eastern: int = 9
    sync_window_end_hour_eastern: int = 21
    ove_required_search_count: int = 6
    scraper_node_id: str = "unknown-node"
    scraper_profile_slug: str = "default"
    scraper_version: str = "0.1.0"
    export_dir: Path = Path("./exports")
    artifact_dir: Path = Path("./artifacts")
    log_level: str = "INFO"
    log_file_path: Path = Path("./logs/ove_scraper.log")
    enabled: bool = True
    ove_base_url: str = "https://www.ove.com"
    ove_listings_url: str = "https://www.ove.com"
    ove_source_platform: str = "manheim"
    ove_east_searches: tuple[str, ...] = (
        "East Hub 2022-2024",
        "East Hub 2024 or Newer",
    )
    ove_west_searches: tuple[str, ...] = (
        "West Hub 2015 - 2021",
        "West Hub 2015-2023",
        "West Hub 2022-2024",
        "West Hub 2024 or Newer",
    )
    ove_search_input_selector: str = "input[type='search'], input[placeholder*='VIN'], input[name*='search']"
    ove_result_link_selector: str = "a[href*='/vehicle/'], a[href*='/listing/']"
    ove_export_button_selector: str = "button:has-text('Export'), a:has-text('Export')"
    ove_saved_search_link_selector: str = "a:has-text('{search_name}'), button:has-text('{search_name}')"
    ove_section_root_selector: str = "main, [role='main'], body"

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
            browser_keepalive_interval_seconds=_get_int("BROWSER_KEEPALIVE_INTERVAL_SECONDS", 900),
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
            sync_window_start_hour_eastern=_get_int("SYNC_WINDOW_START_HOUR_EASTERN", 9),
            sync_window_end_hour_eastern=_get_int("SYNC_WINDOW_END_HOUR_EASTERN", 21),
            ove_required_search_count=_get_int("OVE_REQUIRED_SEARCH_COUNT", 6),
            scraper_node_id=os.getenv("SCRAPER_NODE_ID", socket.gethostname().lower() or "unknown-node"),
            scraper_profile_slug=os.getenv("SCRAPER_PROFILE_SLUG", "default"),
            scraper_version=os.getenv("SCRAPER_VERSION", "0.1.0"),
            export_dir=Path(os.getenv("EXPORT_DIR", "./exports")),
            artifact_dir=Path(os.getenv("ARTIFACT_DIR", "./artifacts")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_file_path=Path(os.getenv("LOG_FILE_PATH", "./logs/ove_scraper.log")),
            enabled=_get_bool("OVE_SYNC_ENABLED", True),
            ove_base_url=os.getenv("OVE_BASE_URL", "https://www.ove.com").rstrip("/"),
            ove_listings_url=os.getenv("OVE_LISTINGS_URL", "https://www.ove.com"),
            ove_source_platform=os.getenv("OVE_SOURCE_PLATFORM", "manheim"),
            ove_east_searches=_get_list(
                "OVE_EAST_SEARCHES",
                (
                    "East Hub 2022-2024",
                    "East Hub 2024 or Newer",
                ),
            ),
            ove_west_searches=_get_list(
                "OVE_WEST_SEARCHES",
                (
                    "West Hub 2015 - 2021",
                    "West Hub 2015-2023",
                    "West Hub 2022-2024",
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

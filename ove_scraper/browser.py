from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from ove_scraper.schemas import ConditionReport, ListingSnapshot


class BrowserSessionError(RuntimeError):
    """Raised when the live OVE session cannot fulfill a request."""


class ListingNotFoundError(BrowserSessionError):
    """Raised when a VIN cannot be found in the live OVE search results."""


class SavedSearchPageEmpty(BrowserSessionError):
    """Raised when the OVE saved-searches page loads successfully but shows
    'No Saved Searches'.  Despite looking like a data issue, this is an
    authentication problem: OVE renders the page shell but the session token
    is stale, so the backend returns zero searches instead of a redirect.
    Repeated requests on the same broken session trigger OVE rate-limiting
    and 500 errors, so this exception must propagate immediately to trigger
    a full browser session recovery (kill Chrome, relaunch) — never retry
    in-page."""


class ManheimAuthRedirectError(BrowserSessionError):
    """Raised when a Manheim navigation lands on auth.manheim.com instead of the
    expected condition-report page. Indicates the OAuth handshake failed and
    the captured DOM is the login screen, not vehicle data. Treat as a hard
    failure — pushing the captured payload would corrupt the VPS."""


class ConditionReportClickFailedError(BrowserSessionError):
    """Raised when the OVE-internal CR link could not be clicked into the
    expected #/details/{vin}/OVE/conditionInformation hash route after every
    retry. The CR is reachable ONLY through this in-page click — there is no
    valid direct-goto fallback for the raw insightcr.manheim.com URL because
    Manheim requires an OVE-side SSO bounce that only fires from a click."""


@dataclass(slots=True)
class DeepScrapeResult:
    images: list[str] = field(default_factory=list)
    condition_report: ConditionReport | None = None
    seller_comments: str | None = None
    listing_snapshot: ListingSnapshot | None = None


class BrowserSession(Protocol):
    def ensure_session(self) -> None:
        """Validate that the live browser session is reachable and authenticated."""

    def list_saved_searches(self) -> tuple[str, ...]:
        """Return the current saved-search names visible to the live OVE session."""

    def export_saved_search(self, search_name: str, export_dir: Path) -> Path:
        """Export a saved search to CSV and return the local file path."""

    def deep_scrape_vin(self, vin: str) -> DeepScrapeResult:
        """Load the OVE listing for a VIN and return redacted detail data."""


class UnimplementedBrowserSession:
    def ensure_session(self) -> None:
        raise BrowserSessionError(
            "Browser session is not implemented. "
            "Attach a real CDP/browser adapter before running the scraper."
        )

    def list_saved_searches(self) -> tuple[str, ...]:
        raise BrowserSessionError(
            "Saved-search discovery is not implemented. "
            "Attach a real CDP/browser adapter before running sync."
        )

    def export_saved_search(self, search_name: str, export_dir: Path) -> Path:
        raise BrowserSessionError(
            f"Browser export for '{search_name}' is not implemented. "
            "Attach a real CDP/browser adapter before running sync."
        )

    def deep_scrape_vin(self, vin: str) -> DeepScrapeResult:
        raise BrowserSessionError(
            f"Deep scrape for VIN {vin} is not implemented. "
            "Attach a real CDP/browser adapter before polling."
        )

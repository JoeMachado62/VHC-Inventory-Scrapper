from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class VehiclePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    vin: str
    year: int
    make: str
    model: str
    trim: str | None = None
    body_type: str | None = None
    sub_body_type: str | None = None
    engine_type: str | None = None
    cylinders: int | None = None
    forced_induction: str | None = None
    drivetrain: str | None = None
    mpg_combined: float | None = None
    ev_range: int | None = None
    towing_capacity_lbs: int | None = None
    odometer: int | None = None
    condition_grade: str | None = None
    price_asking: float
    price_wholesale_est: float | None = None
    location_zip: str | None = None
    location_state: str | None = None
    listing_id: str | None = None
    source_type: str = "ove"
    source_platform: str
    source_url: str | None = None
    images: list[str] = Field(default_factory=list)
    features_raw: list[str] = Field(default_factory=list)
    features_normalized: dict[str, Any] = Field(default_factory=dict)
    available: bool = True
    ove_listing_timestamp: datetime | None = None
    last_seen_active: datetime | None = None
    quality_firewall_pass: bool = True

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, value: str) -> str:
        vin = value.strip().upper()
        if len(vin) != 17:
            raise ValueError("vin must be 17 characters")
        return vin

    @field_validator("location_state")
    @classmethod
    def normalize_state(cls, value: str | None) -> str | None:
        if value is None:
            return None
        state = value.strip().upper()
        return state or None


class SyncMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    east_hub_record_count: int = 0
    west_hub_record_count: int = 0
    duplicates_removed: int = 0
    skipped_no_vin: int = 0
    scraper_node_id: str
    scraper_version: str
    source_platform: str | None = None
    saved_search_names: list[str] = Field(default_factory=list)
    completed_saved_search_names: list[str] = Field(default_factory=list)
    expected_saved_search_count: int = 0
    completed_saved_search_count: int = 0
    missing_saved_search_names: list[str] = Field(default_factory=list)
    full_snapshot: bool = False
    snapshot_mode: str | None = None
    verified_complete_snapshot: bool = False
    upload_mode: str | None = None
    replace_existing_snapshot: bool = False
    single_batch_upload: bool = False


class IngestPayload(BaseModel):
    vehicles: list[VehiclePayload]
    sync_metadata: SyncMetadata


class AutoCheckReport(BaseModel):
    """AutoCheck Vehicle History Report data scraped from OVE listing."""
    model_config = ConfigDict(extra="ignore")

    scrape_status: str = "not_attempted"  # success | partial | failed | not_attempted
    autocheck_score: int | None = None
    owner_count: int | None = None
    accident_count: int | None = None
    title_brand_check: str | None = None  # "OK" | "Problem Reported"
    odometer_check: str | None = None     # "OK" | "Problem Reported"
    accident_check: str | None = None
    damage_check: str | None = None
    vehicle_use: str | None = None
    buyback_protection: str | None = None
    view_report_href: str | None = None
    full_report_text: str | None = None
    raw_text: str | None = None
    failure_category: str | None = None
    failure_message: str | None = None


class ConditionReport(BaseModel):
    overall_grade: str | None = None
    structural_damage: bool | None = None
    paint_condition: str | None = None
    interior_condition: str | None = None
    tire_condition: str | None = None
    announcements: list[str] = Field(default_factory=list)
    remarks: list[str] = Field(default_factory=list)
    seller_comments_items: list[str] = Field(default_factory=list)
    title_status: str | None = None
    title_state: str | None = None
    title_branding: str | None = None
    tire_depths: dict[str, Any] = Field(default_factory=dict)
    damage_items: list[dict[str, Any]] = Field(default_factory=list)
    mechanical_findings: list[dict[str, Any]] = Field(default_factory=list)
    diagnostic_codes: list[str] = Field(default_factory=list)
    damage_summary: dict[str, Any] = Field(default_factory=dict)
    vehicle_history: dict[str, Any] = Field(default_factory=dict)
    problem_highlights: list[str] = Field(default_factory=list)
    severity_summary: str | None = None
    ai_summary: str | None = None
    raw_text: str | None = None
    # Paint color / code lifted from the OVE listing JSON's
    # designatedDescriptionEnrichment.designatedDescription.colors.exterior[]
    # block. The normalized name (e.g. "Red") is the short label; oem_name is
    # the full manufacturer description (e.g. "Rapid Red Metallic Tinted
    # Clearcoat"); paint_code is the OEM option code (e.g. "D4"); rgb_hex is
    # the swatch (e.g. "A0222D"). All optional — populated only when the
    # listing JSON included the colors.exterior block.
    exterior_color: str | None = None
    exterior_color_oem_name: str | None = None
    exterior_paint_code: str | None = None
    exterior_color_rgb: str | None = None
    interior_color: str | None = None
    has_prior_paint: bool | None = None
    # Full normalized list of installed options/equipment lifted from the OVE
    # listing JSON's installedEquipment array. Each entry is a dict with the
    # keys: id, primary_description, extended_description, classification,
    # installed_reason, oem_option_code, msrp, invoice, generics. Empty when
    # the listing JSON did not include installedEquipment (rare).
    #
    # SOURCE: OVE listing JSON. Populated for Manheim CRs and any other
    # listing that exposes the structured equipment data. NOT populated by
    # the Liquid Motors text parser (no MSRP data available there).
    installed_equipment: list[dict[str, Any]] = Field(default_factory=list)
    # Filtered + sorted view of installed_equipment: items the user is most
    # likely to care about for enriching listing display. Selection rule:
    # installed_reason in {"Build Data", "Optional"} AND msrp > 0, sorted by
    # msrp descending. Mandatory standard equipment ($0 build data) is
    # excluded — it adds noise without adding value.
    #
    # SOURCE: derived from installed_equipment. Same source caveat — only
    # populated for vehicles with structured OVE listing JSON equipment.
    high_value_options: list[dict[str, Any]] = Field(default_factory=list)
    # Flat ordered list of free-text vehicle features. Per the user's
    # 2026-04-09 feedback, the Liquid Motors CR HTML has a VEHICLE
    # INFORMATION section listing 50–80+ dealer-supplied features per
    # vehicle (e.g. "BLUETOOTH HANDS FREE MOBILE", "NAVIGATION SYSTEM",
    # "POWER MOONROOF") with NO pricing data, NO classification, NO option
    # codes — just the dealer's marketing feature list. This field
    # captures that list verbatim for the VPS template to render as a
    # "Vehicle Features" or "Standard Equipment" section.
    #
    # SOURCE: Liquid Motors CR text parser. NOT populated for Manheim CRs
    # (which don't have an equivalent free-text section — they use
    # installed_equipment / high_value_options instead). Both lists may
    # coexist when both source formats are available; the VPS template
    # renders whichever is populated.
    equipment_features: list[str] = Field(default_factory=list)
    autocheck: AutoCheckReport | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DetailImage(BaseModel):
    url: str
    role: str = "gallery"
    display_order: int = 0
    is_primary: bool = False
    source_image_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ListingSnapshot(BaseModel):
    title: str | None = None
    subtitle: str | None = None
    badges: list[dict[str, Any]] = Field(default_factory=list)
    hero_facts: list[dict[str, Any]] = Field(default_factory=list)
    sections: list[dict[str, Any]] = Field(default_factory=list)
    icons: list[dict[str, Any]] = Field(default_factory=list)
    page_url: str | None = None
    screenshot_refs: list[str] = Field(default_factory=list)
    raw_html_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DetailPayload(BaseModel):
    source_platform: str
    images: list[DetailImage] = Field(default_factory=list)
    condition_report: ConditionReport | None = None
    seller_comments: str | None = None
    listing_snapshot: ListingSnapshot | None = None
    sync_metadata: dict[str, Any] = Field(default_factory=dict)


class PendingDetailRequest(BaseModel):
    request_id: str
    vin: str
    source_platform: str
    status: str = "CLAIMED"
    priority: int = 100
    attempts: int = 0
    requested_at: datetime
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    last_polled_at: datetime | None = None
    request_source: str | None = None
    requested_by: str | None = None
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SyncExecutionLog(BaseModel):
    timestamp: datetime = Field(default_factory=utc_now)
    execution_status: str
    east_hub_record_count: int = 0
    west_hub_record_count: int = 0
    duplicates_removed: int = 0
    skipped_no_vin: int = 0
    api_push_status: str = "Skipped"
    api_response: dict[str, Any] = Field(default_factory=dict)
    error_details: list[str] = Field(default_factory=list)

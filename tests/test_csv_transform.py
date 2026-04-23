from __future__ import annotations

from ove_scraper.browser import DeepScrapeResult
from ove_scraper.condition_report_normalizer import normalize_condition_report
from ove_scraper.csv_transform import redact_row, transform_rows
from ove_scraper.deep_scrape import build_not_found_payload, redact_detail
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession, build_condition_report
from ove_scraper.cr_parsers import identify_report_family, normalize_severity_color, parse_condition_report_text
from ove_scraper.config import Settings
from ove_scraper.schemas import ConditionReport, ListingSnapshot, PendingDetailRequest
import ove_scraper.location_zip_lookup as location_zip_lookup


def base_row(**overrides: str) -> dict[str, str]:
    row = {
        "VIN": "1HGCM82633A004352",
        "Year": "2021",
        "Make": "Honda",
        "Model": "Accord",
        "Trim": "EX-L",
        "Body Style": "Sedan",
        "Engine": "Gasoline",
        "Drive": "FWD",
        "Mileage": "34,521",
        "Grade": "3.5",
        "Buy Now": "$22,150",
        "MMR": "$19,800",
        "Location ZIP": "30301",
        "State": "ga",
        "OVE Listing ID": "abc123",
        "Last Updated": "2025-07-15T14:30:00+0000",
        "Features": "Leather, Sunroof",
    }
    row.update(overrides)
    return row


def test_transform_rows_deduplicates_by_newest_timestamp() -> None:
    older = base_row(**{"Last Updated": "2025-07-15T14:30:00+0000", "Buy Now": "$21,000"})
    newer = base_row(**{"Last Updated": "2025-07-16T14:30:00+0000", "Buy Now": "$22,000"})

    result = transform_rows([older, newer])

    assert len(result.vehicles) == 1
    assert result.duplicates_removed == 1
    assert result.vehicles[0].price_asking == 22000.0


def test_transform_rows_skips_missing_vin() -> None:
    result = transform_rows([base_row(VIN="")])
    assert result.skipped_no_vin == 1
    assert result.vehicles == []


def test_redact_row_removes_sensitive_columns() -> None:
    row = base_row(**{"Listing Seller": "ABC Motors", "Current Bid": "$15,000"})
    cleaned = redact_row(row)
    assert "Listing Seller" not in cleaned
    assert "Current Bid" not in cleaned


def test_redact_detail_rejects_sensitive_text() -> None:
    detail = DeepScrapeResult(
        images=["https://cdn.example.com/1.jpg"],
        condition_report=ConditionReport(overall_grade="3.5"),
        seller_comments="Listing Seller: ABC Motors",
    )
    request = PendingDetailRequest.model_validate(
        {
            "request_id": "manual",
            "vin": "1HGCM82633A004352",
            "source_platform": "manheim",
            "status": "PENDING",
            "priority": 100,
            "attempts": 0,
            "requested_at": "2026-03-08T00:00:00+00:00",
            "request_source": "manual",
            "requested_by": "test",
            "reason": None,
            "metadata": {},
        }
    )
    settings = Settings(vch_api_base_url="https://example.com/v1", vch_service_token="token")
    try:
        redact_detail(detail, request, settings)
    except ValueError as exc:
        assert "Redacted auction data" in str(exc)
    else:
        raise AssertionError("Expected redaction validation failure")


def test_pickup_location_override_used_before_lookup(monkeypatch) -> None:
    monkeypatch.setattr(
        location_zip_lookup,
        "load_override_db",
        lambda: {
            location_zip_lookup.build_override_key(
                "NY - State Line Auto Auction",
                "State Line Auto Auction",
                "NY",
            ): {
                "pickup_location": "NY - State Line Auto Auction",
                "auction_house": "State Line Auto Auction",
                "state": "NY",
                "zip": "14895",
            }
        },
    )

    row = base_row(
        **{
            "Location ZIP": "",
            "Pickup Location": "NY - State Line Auto Auction",
            "Auction House": "State Line Auto Auction",
            "State": "",
        }
    )
    result = transform_rows([row])
    assert result.vehicles[0].location_zip == "14895"


def test_build_not_found_payload_marks_listing_unavailable() -> None:
    request = PendingDetailRequest.model_validate(
        {
            "request_id": "manual",
            "vin": "WBA7U2C02NCG81181",
            "source_platform": "manheim",
            "status": "PENDING",
            "priority": 100,
            "attempts": 0,
            "requested_at": "2026-03-08T00:00:00+00:00",
            "request_source": "manual",
            "requested_by": "test",
            "reason": "vehicle sold",
            "metadata": {},
        }
    )
    settings = Settings(vch_api_base_url="https://example.com/v1", vch_service_token="token")

    payload = build_not_found_payload(request, settings, "VIN WBA7U2C02NCG81181 is not available in OVE search results")

    assert payload["images"] == []
    assert payload["condition_report"] == {}
    assert payload["sync_metadata"]["scrape_status"] == "not_found"
    assert payload["sync_metadata"]["listing_available"] is False
    assert payload["listing_snapshot"]["metadata"]["failure_category"] == "vin_not_found"


def test_build_condition_report_keeps_report_link_metadata() -> None:
    snapshot = ListingSnapshot(
        hero_facts=[{"label": "Grade", "value": "4.7"}],
        sections=[],
    )

    report = build_condition_report(
        snapshot,
        {
            "href": "https://www.ove.com/reports/example",
            "text": "4.7",
            "title": "Condition Report",
            "reason": "score-node-parent",
            "score": 12,
        },
    )

    assert report is not None
    assert report.overall_grade == "4.7"
    assert report.metadata["report_link"]["href"] == "https://www.ove.com/reports/example"


def test_identify_report_family_handles_known_hosts() -> None:
    descriptor = identify_report_family(
        "https://inspectionreport.manheim.com/?CLIENT=SIMUC&channel=OVE&disclosureid=abc&listingID=123"
    )
    assert descriptor is not None
    assert descriptor.family == "manheim_inspectionreport"
    assert descriptor.supports_structured_damage is True


def test_normalize_severity_color_maps_report_palette() -> None:
    assert normalize_severity_color("yellow") == ("moderate", 1)
    assert normalize_severity_color("orange") == ("major", 2)
    assert normalize_severity_color("red") == ("severe", 3)


def test_normalize_condition_report_promotes_raw_text_fields() -> None:
    report = normalize_condition_report(
        ConditionReport(
            raw_text=(
                "Owners: Owners2 AccidentsACDNT0 Title StatusTitle Absent "
                "Title StateFL Title BrandingNone Announcements CAUTION TRANSMISSION PROB "
                "Remarks No Remarks Seller Comments  No Comments"
            )
        ),
        raw_text=(
            "Owners: Owners2 AccidentsACDNT0 Title StatusTitle Absent "
            "Title StateFL Title BrandingNone Announcements CAUTION TRANSMISSION PROB "
            "Remarks No Remarks Seller Comments  No Comments"
        ),
        report_link={"href": "https://inspectionreport.manheim.com/?listingID=123"},
    )

    assert report is not None
    assert report.title_status == "Title Absent"
    assert report.title_state == "FL"
    assert report.vehicle_history["owners"] == 2
    assert report.vehicle_history["accidents"] == 0
    assert "CAUTION TRANSMISSION PROB" in report.problem_highlights
    assert report.metadata["report_family"] == "manheim_inspectionreport"


def test_parse_condition_report_text_extracts_manheim_ecr_tires_and_damage() -> None:
    raw_text = """
    GRADING
    Grade 4.7 Clean
    Engine Starts-Yes
    Drivable-Yes
    INTERIOR
    Int Odor: OK
    TIRES AND WHEELS
    Wheels:\tAlloy
    Tire\tTread Depth\tBrand\tSize
    Left Front:\t6/32"\tPirelli\t245/40R20
    Left Rear:\t4/32"\tPirelli\t275/35R20
    Right Front:\t6/32"\tPirelli\t245/40R20
    Right Rear:\t4/32"\tPirelli\t275/35R20
    Spare:\tN/A\tN/A\tN/A
    ADDITIONAL INFORMATION
    *EXECUTIVE&MSPORT PKGS/SHDOWLNE/20"WHLS
    Common Abbreviations
    DAMAGE SUMMARY AND ADDITIONAL IMAGES
    Paint and Body Requires Conventional Repair-[1 Items]
    IMAGE\tDESCRIPTION\tCONDITION\tSEVERITY
    \tFront Bumper Cover\tCurb Rash Bug damage\t4" to 5"
    Interior-[1 Items]
    IMAGE\tDESCRIPTION\tCONDITION\tSEVERITY
    \tDash\tWorn\tAcceptable
    VIN:\tWBA7U2C02NCG81181
    """

    parsed = parse_condition_report_text(
        "https://mmsc400.manheim.com/MABEL/ECR2I.htm?listingID=123",
        raw_text,
    )

    assert parsed["overall_grade"] == "4.7"
    assert parsed["engine_starts"] is True
    assert parsed["drivable"] is True
    assert parsed["interior_condition"] == "OK"
    assert parsed["tire_depths"]["left_front"]["tread_depth"] == '6/32"'
    assert parsed["damage_summary"]["total_items"] == 2
    assert parsed["damage_items"][0]["panel"] == "Front Bumper Cover"
    assert parsed["severity_summary"] == "moderate"


def test_normalize_condition_report_uses_structured_manheim_ecr_parse() -> None:
    raw_text = """
    GRADING
    Grade 4.7 Clean
    Engine Starts-Yes
    Drivable-Yes
    INTERIOR
    Int Odor: OK
    TIRES AND WHEELS
    Wheels:\tAlloy
    Tire\tTread Depth\tBrand\tSize
    Left Front:\t6/32"\tPirelli\t245/40R20
    Right Front:\t6/32"\tPirelli\t245/40R20
    DAMAGE SUMMARY AND ADDITIONAL IMAGES
    Miscellaneous-[1 Items]
    IMAGE\tDESCRIPTION\tCONDITION\tSEVERITY
    \tWindshield\tChipped\t< 1/8"
    VIN:\tWBA7U2C02NCG81181
    """

    report = normalize_condition_report(
        ConditionReport(),
        raw_text=raw_text,
        report_link={"href": "https://mmsc400.manheim.com/MABEL/ECR2I.htm?listingID=123"},
    )

    assert report is not None
    assert report.overall_grade == "4.7"
    assert report.tire_depths["left_front"]["brand"] == "Pirelli"
    assert report.damage_items[0]["panel"] == "Windshield"
    assert report.vehicle_history["engine_starts"] is True
    assert report.vehicle_history["drivable"] is True


def test_parse_condition_report_text_extracts_inspectionreport_damage_and_tires() -> None:
    raw_text = """
    Condition Report
    2020 Audi A6 2.0T Premium Plus Sedan
    4.6
    Clean
    Announcements
    Open Recall
    Remarks/Comments
    --
    Title
    TITLE STATE
    --
    TITLE STATUS
    NOT SPECIFIED
    Drivability, Keys, & History
    VEHICLE STARTS
    Yes - Starts
    VEHICLE DRIVES
    Yes - Drives
    Exterior
    FRONT EXTERIOR
    FRONT WINDSHIELD
    Severe Damage
    DRIVER EXTERIOR
    No Damage
    Interior
    INTERIOR COSMETIC DAMAGE
    HEADLINER
    Stained
    Mechanical & Diagnostic Trouble Codes
    DIAGNOSTIC TROUBLE CODES
    Scan Not Available
    WARNING LIGHTS & GAUGE CLUSTER
    OBDII Codes
    NOTE:
    P0236, P0238
    Tires & Wheels
    DRIVER FRONT TIRE DEPTH
    6/32" or Above
    DRIVER FRONT TIRE & WHEEL ISSUE
    No Issues
    PASSENGER FRONT TIRE DEPTH
    5/32"
    PASSENGER FRONT TIRE & WHEEL ISSUE
    No Issues
    """

    parsed = parse_condition_report_text(
        "https://inspectionreport.manheim.com/?CLIENT=SIMUC&listingID=123",
        raw_text,
    )

    assert parsed["overall_grade"] == "4.6"
    assert parsed["announcements"] == ["Open Recall"]
    assert parsed["damage_items"][0]["panel"] == "FRONT WINDSHIELD"
    assert parsed["damage_items"][0]["severity_color"] == "red"
    assert parsed["tire_depths"]["driver_front"]["tread_depth"] == '6/32" or Above'
    assert parsed["mechanical_findings"][0]["system"] == "DIAGNOSTIC TROUBLE CODES"
    assert parsed["diagnostic_codes"] == ["P0236", "P0238"]
    assert "Diagnostic trouble code scan not available" in parsed["problem_highlights"]
    assert parsed["title_status"] == "NOT SPECIFIED"
    assert parsed["ai_summary"] is not None


def test_parse_condition_report_text_extracts_insightcr_fields() -> None:
    raw_text = """
    2023 Ford F-150 Raptor Crew Cab Short Bed
    5.0
    Extra
    Clean
    Condition Details
    Not Specified
    No Structural Damage
    No Prior
    Paint
    Vehicle History
    Owners
    1
    ACDNT
    0
    ANNOUNCEMENTS & COMMENTS
    Announcements
    CLN CFX, 1 OWNER, PANO,BEDLINER,RPD RED
    Remarks
    No Remarks
    Additional Announcements
    Green Light.
    Seller Comments
    No Comments
    Title
    Title Status
    Title Absent
    Title State
    --
    Title Branding
    --
    CONDITION DETAILS
    Inspection Date
    03/18/2026
    Work Order
    5590646
    Location
    Manheim Orlando
    Exterior (0)
    No exterior condition items were reported
    Interior (0)
    No interior condition items were reported
    Structure (0)
    No structure condition items were reported
    Other (0)
    No other condition items were reported
    TIRES AND WHEELS
    Wheels
    Aluminum
    Left Front
    BFGoodrich
    9/32”
    315/70R17
    Right Front
    BFGoodrich
    9/32”
    315/70R17
    """

    parsed = parse_condition_report_text(
        "https://insightcr.manheim.com/cr-display?CLIENT=SIMUC&listingID=123",
        raw_text,
    )

    assert parsed["overall_grade"] == "5.0"
    assert parsed["grade_label"] == "Extra Clean"
    assert parsed["structural_damage"] is False
    assert parsed["paint_condition"] == "No Prior Paint"
    assert parsed["announcements"] == ["CLN CFX, 1 OWNER, PANO,BEDLINER,RPD RED", "Green Light."]
    assert parsed["title_status"] == "Title Absent"
    assert parsed["owners"] == 1
    assert parsed["accidents"] == 0
    assert parsed["tire_depths"]["left_front"]["tread_depth"] == "9/32”"
    assert parsed["damage_summary"]["total_items"] == 0


def test_normalize_condition_report_prefers_structured_insightcr_fields() -> None:
    raw_text = """
    2023 Ford F-150 Raptor Crew Cab Short Bed
    5.0
    Extra
    Clean
    Condition Details
    Not Specified
    No Structural Damage
    No Prior
    Paint
    Vehicle History
    Owners
    1
    ACDNT
    0
    ANNOUNCEMENTS & COMMENTS
    Announcements
    CLN CFX, 1 OWNER, PANO,BEDLINER,RPD RED
    Remarks
    No Remarks
    Additional Announcements
    Green Light.
    Seller Comments
    No Comments
    Title
    Title Status
    Title Absent
    Title State
    --
    Title Branding
    --
    TIRES AND WHEELS
    Wheels
    Aluminum
    Left Front
    BFGoodrich
    9/32”
    315/70R17
    Right Front
    BFGoodrich
    9/32”
    315/70R17
    """

    report = normalize_condition_report(
        ConditionReport(),
        raw_text=raw_text,
        report_link={"href": "https://insightcr.manheim.com/cr-display?CLIENT=SIMUC&listingID=123"},
    )

    assert report is not None
    assert report.overall_grade == "5.0"
    assert report.paint_condition == "No Prior Paint"
    assert report.structural_damage is False
    assert report.title_status == "Title Absent"
    assert report.announcements == ["CLN CFX, 1 OWNER, PANO,BEDLINER,RPD RED", "Green Light."]
    assert report.tire_depths["left_front"]["brand"] == "BFGoodrich"


def test_parse_condition_report_text_extracts_liquidmotors_fields() -> None:
    raw_text = """
    Condition Report
    2021 Lexus RX 350
    Grade 4.4 Clean
    Announcements
    Open Recall
    Remarks
    Prior rental unit
    Seller Comments
    Non-smoker
    Title Status
    Clean
    Title State
    FL
    Title Branding
    --
    Owners
    2
    Accidents
    1
    Engine Starts-Yes
    Drivable-Yes
    """

    parsed = parse_condition_report_text(
        "https://content.liquidmotors.com/IR/123456.html",
        raw_text,
    )

    assert parsed["overall_grade"] == "4.4"
    assert parsed["grade_label"] == "Clean"
    assert parsed["announcements"] == ["Open Recall"]
    assert parsed["remarks"] == ["Prior rental unit"]
    assert parsed["seller_comments"] == ["Non-smoker"]
    assert parsed["title_status"] == "Clean"
    assert parsed["title_state"] == "FL"
    assert parsed["owners"] == 2
    assert parsed["accidents"] == 1
    assert parsed["engine_starts"] is True
    assert parsed["drivable"] is True


def test_normalize_condition_report_uses_structured_liquidmotors_parse() -> None:
    raw_text = """
    Grade 4.4 Clean
    Announcements
    Open Recall
    Remarks
    Prior rental unit
    Seller Comments
    Non-smoker
    Title Status
    Clean
    Title State
    FL
    Owners
    2
    Accidents
    1
    Engine Starts-Yes
    Drivable-Yes
    """

    report = normalize_condition_report(
        ConditionReport(),
        raw_text=raw_text,
        report_link={"href": "https://content.liquidmotors.com/IR/123456.html"},
    )

    assert report is not None
    assert report.overall_grade == "4.4"
    assert report.announcements == ["Open Recall"]
    assert report.remarks == ["Prior rental unit"]
    assert report.seller_comments_items == ["Non-smoker"]
    assert report.title_status == "Clean"
    assert report.title_state == "FL"
    assert report.vehicle_history["owners"] == 2
    assert report.vehicle_history["accidents"] == 1
    assert report.vehicle_history["engine_starts"] is True
    assert report.vehicle_history["drivable"] is True
    assert report.metadata["report_family"] == "liquidmotors_ir"


def test_normalize_condition_report_prefers_structured_inspection_report_fields() -> None:
    raw_text = """
    Condition Report
    2020 Audi A6 2.0T Premium Plus Sedan
    4.6
    Clean
    Announcements
    Open Recall
    Remarks/Comments
    --
    Title
    TITLE STATE
    --
    TITLE STATUS
    NOT SPECIFIED
    VEHICLE STARTS
    Yes - Starts
    VEHICLE DRIVES
    Yes - Drives
    Exterior
    FRONT EXTERIOR
    FRONT WINDSHIELD
    Severe Damage
    Interior
    INTERIOR COSMETIC DAMAGE
    HEADLINER
    Stained
    Mechanical & Diagnostic Trouble Codes
    DIAGNOSTIC TROUBLE CODES
    Scan Not Available
    WARNING LIGHTS & GAUGE CLUSTER
    OBDII Codes
    NOTE:
    P0236, P0238
    Tires & Wheels
    DRIVER FRONT TIRE DEPTH
    6/32" or Above
    DRIVER FRONT TIRE & WHEEL ISSUE
    No Issues
    """

    report = normalize_condition_report(
        ConditionReport(),
        raw_text=raw_text,
        report_link={"href": "https://inspectionreport.manheim.com/?CLIENT=SIMUC&listingID=123"},
    )

    assert report is not None
    assert report.title_status == "NOT SPECIFIED"
    assert report.remarks == []
    assert report.damage_items[0]["panel"] == "FRONT WINDSHIELD"
    assert report.mechanical_findings[0]["condition"] == "Scan Not Available"
    assert report.diagnostic_codes == ["P0236", "P0238"]
    assert "/Comments -- Title" not in report.problem_highlights


def test_report_link_selector_rejects_generic_ove_condition_order_page() -> None:
    settings = Settings(vch_api_base_url="https://example.com/v1", vch_service_token="token")
    browser = PlaywrightCdpBrowserSession(settings)
    result = browser._select_valid_condition_report_link(
        {"href": "https://www.ove.com/order_condition_reports/new#/", "text": "Order Condition Report", "score": 99},
        {
            "href": "https://inspectionreport.manheim.com/?CLIENT=SIMUC&channel=OVE&disclosureid=abc&listingID=123&username=CIAplatform",
            "text": "",
            "labelText": "CR",
            "valueText": "4.6",
            "score": 10,
        },
    )

    assert result is not None
    assert result["href"].startswith("https://inspectionreport.manheim.com/")


def test_report_link_selector_returns_none_for_only_generic_ove_link() -> None:
    settings = Settings(vch_api_base_url="https://example.com/v1", vch_service_token="token")
    browser = PlaywrightCdpBrowserSession(settings)

    result = browser._select_valid_condition_report_link(
        {"href": "https://www.ove.com/order_condition_reports/new#/", "text": "Order Condition Report", "score": 99}
    )

    assert result is None


def test_create_worker_page_uses_new_tab_and_validates_session() -> None:
    class FakePage:
        def __init__(self, url: str = "https://www.ove.com/saved_searches#/") -> None:
            self.url = url
            self.goto_calls: list[str] = []
            self.wait_calls: list[int] = []
            self.closed = False

        def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int | None = None) -> None:
            self.url = url
            self.goto_calls.append(url)

        def wait_for_timeout(self, timeout_ms: int) -> None:
            self.wait_calls.append(timeout_ms)

        def close(self) -> None:
            self.closed = True

        def title(self) -> str:
            return "OVE"

    class FakeContext:
        def __init__(self, page: FakePage) -> None:
            self._page = page
            self.new_page_calls = 0

        def new_page(self) -> FakePage:
            self.new_page_calls += 1
            return self._page

    settings = Settings(vch_api_base_url="https://example.com/v1", vch_service_token="token")
    browser = PlaywrightCdpBrowserSession(settings)
    page = FakePage()
    context = FakeContext(page)

    result = browser._create_worker_page(context)  # type: ignore[arg-type]

    assert result is page
    assert context.new_page_calls == 1
    assert page.goto_calls == [f"{settings.ove_base_url}/saved_searches#/"]
    assert page.wait_calls == [1500]
    assert page.closed is False

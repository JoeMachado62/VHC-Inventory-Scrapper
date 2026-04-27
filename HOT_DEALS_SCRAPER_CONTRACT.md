# Hot Deals Scraper Contract

## Audience

This document is for the scraper agent/process that builds the OVE
`VHC Marketing List` output.

The scraper should continue its normal OVE inventory push. In addition,
after the VHC Marketing List deep scrape completes, it should push the
curated Hot Deals batch to the VPS using the endpoint below.

## Endpoint

```text
POST /api/v1/inventory/ove/hot-deals/ingest
```

Headers:

```text
Authorization: Bearer <SERVICE_TOKEN>
Content-Type: application/json
```

## Batch Requirements

Send one batch per completed VHC Marketing List run.

Use `snapshot_mode = "full_replace"` when the batch represents the complete
current Hot Deals list. This lets the VPS deactivate Hot Deals that are no
longer in the list.

Use `snapshot_mode = "append"` only for manual recovery or a small
supplemental push.

Every included VIN must have already passed the scraper-side negative CR
filter.

## Payload Shape

```jsonc
{
  "source_list_name": "VHC Marketing List",
  "source_platform": "manheim",
  "batch_id": "vhc-marketing-2026-04-26-0900Z",
  "snapshot_mode": "full_replace",
  "scraped_at": "2026-04-26T09:00:00Z",
  "filter_rules": {
    "minimum_delta_below_mmr": 1000,
    "negative_cr_filter_version": "2026-04-26",
    "excluded_if": [
      "structural damage",
      "frame damage",
      "airbag issue",
      "true mileage unknown",
      "flood",
      "branded title",
      "major mechanical warning"
    ]
  },
  "deals": [
    {
      "vin": "1FT8W2BN0PEC12345",
      "listing_id": "ove-123456",
      "listing_url": "https://www.ove.com/...",
      "source_platform": "manheim",
      "auction_start_at": "2026-04-26T14:00:00Z",
      "auction_end_at": "2026-04-26T20:00:00Z",
      "vehicle": {
        "year": 2025,
        "make": "Ford",
        "model": "F-250",
        "trim": "XL",
        "body_type": "Truck",
        "odometer": 22140,
        "condition_grade": "4.2",
        "price_asking": 56049,
        "location_state": "FL",
        "location_zip": "33101",
        "source_url": "https://www.ove.com/...",
        "images": [
          "https://..."
        ],
        "features_raw": [
          "4WD",
          "Crew Cab"
        ],
        "features_normalized": {
          "exterior_color": "Black",
          "interior_color": "Gray",
          "fuel_type": "Diesel",
          "transmission": "Automatic",
          "drivetrain": "4WD",
          "pickup_location": "Orlando, FL",
          "mmr": 58500
        }
      },
      "pricing": {
        "mmr_value": 58500,
        "asking_price": 56049,
        "deal_delta": 2451,
        "deal_delta_pct": 4.19,
        "deal_label": "Excellent",
        "deal_rank": 1
      },
      "cr_screen": {
        "status": "passed",
        "version": "2026-04-26",
        "reasons": [],
        "positive_highlights": [
          "No structural damage reported",
          "Runs and drives",
          "Clean title indicated"
        ],
        "excluded_signals_checked": [
          "structural_damage",
          "frame_damage",
          "flood",
          "airbag",
          "odometer",
          "branded_title",
          "major_mechanical"
        ]
      },
      "detail": {
        "images": [
          {
            "url": "https://...",
            "role": "hero",
            "display_order": 0,
            "is_primary": true
          }
        ],
        "condition_report": {
          "overall_grade": "4.2",
          "announcements": [],
          "remarks": [],
          "vehicle_history": {
            "owners": 1,
            "accidents": 0,
            "engine_starts": true,
            "drivable": true
          },
          "damage_items": [],
          "damage_summary": {
            "total_items": 0,
            "structural_issue": false
          },
          "tire_depths": {
            "lf": {"position_label": "LF", "tread_depth": "7/32"},
            "rf": {"position_label": "RF", "tread_depth": "7/32"},
            "lr": {"position_label": "LR", "tread_depth": "6/32"},
            "rr": {"position_label": "RR", "tread_depth": "6/32"}
          },
          "metadata": {
            "report_link": {
              "href": "http://content.liquidmotors.com/IR/15614/38020971.html",
              "title": "4.2"
            }
          }
        },
        "seller_comments": "Runs and drives well.",
        "listing_snapshot": {
          "title": "2025 Ford F-250 XL",
          "subtitle": "VHC Marketing List",
          "badges": [{"label": "Buy Now"}],
          "hero_facts": [
            {"label": "Odometer", "value": "22,140"},
            {"label": "MMR", "value": "$58,500"}
          ],
          "sections": [],
          "icons": [],
          "page_url": "https://www.ove.com/...",
          "screenshot_refs": [],
          "raw_html_ref": null
        }
      },
      "marketing": {
        "title": "Deal of the Hour: 2025 Ford F-250 XL",
        "summary": "$2,451 below MMR and condition-report screened.",
        "priority": 1,
        "featured_until": "2026-04-26T15:00:00Z",
        "channels": ["homepage", "facebook", "instagram", "email"]
      },
      "raw_refs": {
        "listing_json_ref": "s3://...",
        "condition_report_html_ref": "s3://...",
        "scraper_run_log_ref": "s3://..."
      }
    }
  ]
}
```

## Required Deal Fields

Each `deals[]` item must include:

- `vin`
- `auction_end_at` — required; drives the lower-left countdown clock on
  Hot Deal cards and detailed listing pages
- `vehicle.year`
- `vehicle.make`
- `vehicle.model`
- `vehicle.price_asking`
- `pricing.mmr_value`
- `pricing.asking_price`
- `pricing.deal_delta`
- `pricing.deal_label`
- `pricing.deal_rank`
- `cr_screen.status = "passed"`
- `detail.condition_report`
- `detail.listing_snapshot`

If a condition report could not be deeply scraped, do not include the VIN
in the Hot Deals batch. Send it through the normal OVE inventory flow only.

## Condition Report Requirements

The `detail.condition_report` object should follow
[CONDITION_REPORT_CONTRACT.md](./CONDITION_REPORT_CONTRACT.md).

Important fields:

- `overall_grade`
- `announcements`
- `remarks`
- `vehicle_history`
- `damage_items`
- `damage_summary`
- `tire_depths`
- `metadata.report_link.href`

The VPS stores this report, but the frontend must gate full report access
to admins and logged-in credit-approved buyers.

## Negative CR Filter

The scraper should exclude vehicles with negative signals before sending
the Hot Deals batch.

Recommended exclude signals:

- structural damage
- frame damage
- flood/water damage
- branded title
- true mileage unknown / odometer inconsistency
- airbag deployed or airbag warning
- engine does not start
- vehicle does not drive
- major mechanical warning
- severe damage item count above configured threshold
- missing CR HTML when the listing claims a CR exists

Include `cr_screen.excluded_signals_checked` so the VPS can show admins how
the vehicle passed.

## Response Shape

Expected response:

```jsonc
{
  "status": "ok",
  "data": {
    "source_list_name": "VHC Marketing List",
    "batch_id": "vhc-marketing-2026-04-26-0900Z",
    "snapshot_mode": "full_replace",
    "requested": 12,
    "upserted_vehicles": 12,
    "upserted_details": 12,
    "hot_deals_inserted": 3,
    "hot_deals_updated": 9,
    "hot_deals_deactivated": 4,
    "rejected": 0,
    "active_count": 12
  }
}
```

If any VIN is rejected, the response should include a per-VIN error list.

## Scraper Agent Prompt

Use this prompt for the scraper agent that runs after the VHC Marketing
List scrape completes:

```text
You are preparing the VirtualCarHub Hot Deals batch for the VPS.

Source list: OVE "VHC Marketing List".
Only include vehicles priced at least $1,000 below MMR that passed the
negative condition-report screen.

For every included VIN:
1. Include complete vehicle basics and current OVE listing URL.
2. Include auction/listing start and end time. auction_end_at is required.
   The VPS uses auction_end_at to render the lower-left countdown clock on
   Hot Deal cards and detailed listing pages.
3. Include MMR, asking price, dollar delta below MMR, percent delta when
   available, deal_label, and deal_rank.
4. Include the deep scrape condition_report payload following
   CONDITION_REPORT_CONTRACT.md.
5. Include listing_snapshot, seller_comments, images, and CR metadata links.
6. Include cr_screen.status="passed", the filter version, reasons=[], and
   the excluded signals that were checked.
7. Do not include vehicles with structural/frame/flood/title/odometer/airbag/
   non-running/non-driving/major-mechanical negative signals.
8. Do not include vehicles where the deep CR scrape failed or the CR HTML is
   missing. Send those only through the normal OVE inventory flow.

POST the batch to:
POST /api/v1/inventory/ove/hot-deals/ingest

Use snapshot_mode="full_replace" when this is the complete current VHC
Marketing List. Use a unique batch_id that includes the scrape date/time.

The VPS will upsert normal vehicle data, attach the condition report, create
or update hot_deals rows, and deactivate hot deals missing from the latest
full snapshot.
```

# Scraper → VPS Condition Report Payload Contract

**Authoritative source: the OVE scraper.** This document is owned by the
scraper repo and describes the exact shape of the payload that
`POST /api/v1/inventory/ove/detail/{vin}` delivers. The VPS template should
adapt its renderer to consume these fields. Earlier versions of this
contract (notably `CONDITION_REPORT_CONTRACT.md` written from the VPS side)
were written before the listing-JSON extraction landed and contain
out-of-date assumptions; treat this document as the source of truth and
treat `CONDITION_REPORT_CONTRACT.md` as superseded.

**Scope.** Three CR providers are in scope today:
- **Manheim Inspection Report** (`inspectionreport.manheim.com`)
- **Manheim Insight CR** (`insightcr.manheim.com`)
- **Manheim Express ECR** (`mmsc400.manheim.com/MABEL/ECR2I.htm`)
- **Liquid Motors IR** (`content.liquidmotors.com/IR/{dealer_id}/{cr_id}.html`)

The "Auto Grade only" saved-search filter does **not** actually exclude
liquidmotors-hosted CRs in practice — some OVE Partner Auction listings
slip through with `content.liquidmotors.com` CR links. Verified live
against VIN 1N4BL4EV2NN423240 on 2026-04-09. The scraper handles all
four providers via per-family parsers in [ove_scraper/cr_parsers.py](ove_scraper/cr_parsers.py).

---

## 1. Top-level payload

```jsonc
POST /api/v1/inventory/ove/detail/{vin}
{
  "source_platform": "manheim",
  "images": [ /* §3 */ ],
  "condition_report": { /* §2 */ },
  "seller_comments": "string | null",
  "listing_snapshot": { /* page-derived listing snapshot */ },
  "sync_metadata": { /* request_id, scraper_node_id, etc. */ }
}
```

`condition_report` and `images` are the two fields the CR template renders.
`condition_report` is an instance of `ConditionReport` from
[ove_scraper/schemas.py](ove_scraper/schemas.py); the schema is the contract.

---

## 2. `condition_report` fields

All fields are optional from a schema standpoint — the scraper omits
fields it can't populate rather than sending `null`/`{}`. Each field below
states (a) its source — OVE listing JSON or the per-family Manheim CR
parser — and (b) when it can legitimately be empty.

A canonical fully-populated example lives at
[tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json](tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json).
Diff your renderer against that file.

### 2.1 Identification / grade

| Field | Type | Source | Notes |
|---|---|---|---|
| `overall_grade` | `string` | listing JSON `conditionGrade`; CR parser as fallback | e.g. `"5.0"` |
| `structural_damage` | `bool \| null` | CR parser | `false` = "No Structural Damage" |
| `paint_condition` | `string \| null` | listing JSON `hasPriorPaint`; CR parser | `"No Prior Paint"` / `"Prior Paint"` |
| `interior_condition` | `string \| null` | CR parser | |
| `tire_condition` | `string \| null` | CR parser | summary string, distinct from `tire_depths` |

### 2.2 Paint color & code (NEW)

These are pulled from
`listing_json.designatedDescriptionEnrichment.designatedDescription.colors.exterior[]`.
The scraper picks the entry where `isPrimary == true` (or the first entry
if none are flagged primary).

| Field | Type | Example |
|---|---|---|
| `exterior_color` | `string \| null` | `"Red"` (normalized short name) |
| `exterior_color_oem_name` | `string \| null` | `"Rapid Red Metallic Tinted Clearcoat"` (full manufacturer name) |
| `exterior_paint_code` | `string \| null` | `"D4"` (OEM option code) |
| `exterior_color_rgb` | `string \| null` | `"A0222D"` (hex without `#`) |
| `interior_color` | `string \| null` | `"Black"` |
| `has_prior_paint` | `bool \| null` | `false` for no prior paint |

The VPS template should render the OEM full name as the primary label
and use the RGB hex as a swatch background where available.

### 2.3 Announcements & remarks

| Field | Type | Source |
|---|---|---|
| `announcements` | `string[]` | listing JSON `announcementsEnrichment.announcements` |
| `remarks` | `string[]` | CR parser remarks section |
| `seller_comments_items` | `string[]` | CR parser seller block |
| `problem_highlights` | `string[]` | derived (announcements + remarks + seller comments) |
| `severity_summary` | `string \| null` | derived (`"attention"` if any highlights) |
| `ai_summary` | `string \| null` | optional narrative summary |

### 2.4 `vehicle_history`

```jsonc
{
  "owners": 1,           // listing JSON autocheck.ownerCount
  "accidents": 0,        // listing JSON autocheck.numberOfAccidents
  "drivable": true,      // CR parser
  "engine_starts": true  // CR parser
}
```

### 2.5 `damage_items` + `damage_summary`

Populated from the CR parser when the CR view loads successfully. May be
empty for clean vehicles even when the CR did load.

```jsonc
"damage_items": [
  {
    "section": "exterior",
    "section_label": "Exterior",
    "panel": "FRONT WINDSHIELD",
    "condition": "Severe Damage",
    "reported_severity": "Severe Damage",
    "severity_color": "red",          // gray | green | yellow | orange | red
    "severity_label": "severe",
    "severity_rank": 3
  }
]
```

`damage_summary` is a roll-up dict (`total_items`, `by_color`,
`by_section`, `structural_issue`).

### 2.6 `tire_depths`

Keyed map. Position keys depend on the CR family:

- `manheim_inspectionreport` family uses `driver_front` / `driver_rear` / `passenger_front` / `passenger_rear` (and optional `spare`)
- `manheim_insightcr` family uses `left_front` / `left_rear` / `right_front` / `right_rear` / `spare`

Each entry: `{position_label, tread_depth, brand?, size?, wheel_type?, issue?}`.
The VPS template should accept either key set and treat them as equivalent.
Hidden entirely if `tire_depths` is missing or empty.

### 2.7 Title info

`title_status`, `title_state`, `title_branding` — all `string | null`,
populated from CR parser. Section renders only if at least one is present.

### 2.8 `installed_equipment` (NEW)

Full list of installed options/equipment from
`listing_json.designatedDescriptionEnrichment.installedEquipment`. Each
entry:

```jsonc
{
  "id": 42759581,
  "primary_description": "MOONROOF & TAILGATE",
  "extended_description": "-inc: Power Tailgate, Twin Panel Moonroof",
  "classification": "EXTERIOR",      // PACKAGE | INTERIOR | EXTERIOR | MECHANICAL | SAFETY | ...
  "installed_reason": "Build Data",  // Build Data | Standard | Optional
  "oem_option_code": "18Y",
  "msrp": 2195,
  "invoice": 1998,
  "generics": [{"id": 188, "name": "Panoramic Roof"}, ...]
}
```

This is the raw normalized list — typically 100+ items per vehicle, mostly
$0 standard equipment. Use `high_value_options` (§2.9) for display.

### 2.9 `high_value_options` (NEW — feature requested 2026-04-09)

Filtered + sorted view of `installed_equipment`. Selection rule:

- `installed_reason` is `"Build Data"` or `"Optional"`
- `msrp > 0`
- Sorted by `msrp` descending

This is what the VPS CR template should render in a "High Value Options"
section. Standard $0 equipment ("2 Seatback Storage Pockets",
"Front Center Armrest", etc.) is excluded as noise. Items with their own
MSRP carry information about what makes the vehicle desirable. Example
from the F-150 Raptor golden fixture:

```
$2195  EXTERIOR   MOONROOF & TAILGATE
$ 595  EXTERIOR   TOUGH BED SPRAY-IN BEDLINER
```

### 2.10 `metadata`

```jsonc
{
  "report_link": {
    "href": "https://insightcr.manheim.com/cr-display?...",
    "labelText": "CR",
    "valueText": "5.0",
    "score": 44
  },
  "report_family": "manheim_insightcr",  // manheim_inspectionreport | manheim_insightcr | manheim_ecr
  "report_page": {
    "url": "https://...",
    "title": "...",
    "body_text": "...",
    "html_ref": "artifacts/{vin}/condition-report.html",
    "screenshot_ref": "artifacts/{vin}/condition-report.png",
    "captured_from_iframe": true        // NEW: true when snapshot came from the Manheim iframe inside the OVE-internal viewer
  },
  "listing_json": { /* the entire OVE listing JSON */ }
}
```

**`metadata.report_link.href` is load-bearing.** The VPS backend extracts
the Manheim CR deep link from this exact path with `_extract_cr_url` and
returns it as `vehicle.condition_report_url`. The "See Original Condition
Report" button on the VPS template depends on this path.

**`metadata.listing_json` is the full OVE listing object.** It includes
115+ fields the scraper does not currently surface as named columns
(equipment array, MMR valuation, sale times, channel, etc.). The VPS
template can read any field from here without a scraper change.

### 2.11 `raw_text`

Capped at **16 KB**. The full body text the parser was run against, kept
for debugging and as a last-resort fallback. The VPS template should
**never** dump `raw_text` into the Announcements section — that produced
the JSON-blob symptom on the 04-08 push. If `condition_report.announcements`
is empty, render the announcements section as empty/hidden.

---

## 3. `images`

```jsonc
[
  {
    "url": "https://images.cdn.manheim.com/{id}.jpg",
    "role": "gallery",          // gallery | hero | inspection | disclosure
    "display_order": 0,
    "is_primary": true,
    "source_image_id": null,
    "metadata": {}
  }
]
```

Rules:
- Send the full OVE gallery (typically 10–40 images), not just the first thumbnail.
- Filtered out by the frontend: `.svg`, `.gif`, `ready_logistics.png` and similar non-photo assets.
- Mark exactly one image `is_primary: true`.
- `role: "inspection"` and `role: "disclosure"` images render in their own sub-galleries.

---

## 4. CR navigation map (how the per-family parser actually gets fed)

The scraper dispatches by CR-link host. This dispatch was added 2026-04-09
to fix a regression introduced by commit d6136d4 that broke
`inspectionreport.manheim.com` vehicles.

| CR link host | Path | Why |
|---|---|---|
| `inspectionreport.manheim.com` | `context.new_page().goto(href)` direct popup | Accepts OVE session cookies. No SSO bounce. CR HTML loads cleanly and the per-family parser sees structurally clean text. |
| `mmsc400.manheim.com` (Manheim Express) | Same — direct goto | Same — no SSO bounce. |
| `content.liquidmotors.com` | Same — direct goto | Third-party CR provider. The href carries `?username=CIAplatform` which authenticates the OVE session. No SSO bounce. Used by some OVE Partner Auction listings that slip through the Auto Grade filter. |
| `insightcr.manheim.com` | Click the OVE CR link, poll for `#/details/{vin}/OVE/conditionInformation` hash route, then snapshot the **iframe** inside the OVE webapp where Manheim CR content is rendered | Direct goto to insightcr always 302s to `auth.manheim.com` (no scraper credentials). The OVE webapp itself performs the SSO bounce server-side. |

The iframe snapshot (Bug B fix in 2026-04-09) means
[_snapshot_page](ove_scraper/cdp_browser.py) enumerates `page.frames`,
finds the frame whose URL contains `manheim.com` (excluding
`auth.manheim.com`), and reads `body.innerText` from THAT frame instead of
the outer OVE page. The existing per-family parsers in
[ove_scraper/cr_parsers.py](ove_scraper/cr_parsers.py)
(`_parse_manheim_inspectionreport`, `_parse_manheim_insightcr`,
`_parse_manheim_ecr`) then receive the input format they were written for
and populate damage / tires / paint / structural / title / etc.

---

## 5. Partial-data protocol

Manheim periodically changes parts of its CR HTML; individual fields will
break before the whole CR does. The scraper:

1. **Never** sends an empty section dict (`{}`). Missing data is omitted entirely so the VPS template hides the section instead of rendering an empty grid.
2. Adds `condition_report.metadata.scrape_warnings` (reserved — not yet wired) for per-field parse failures.
3. Routes via `/fail` with `error_category=auth_expired` when the CR view is captured as a Manheim auth page (the [_raise_if_auth_redirect](ove_scraper/cdp_browser.py) defense fires post-snapshot).
4. Routes via `/fail` for transient browser errors so the lease queue retries with backoff.
5. Uses the 75% snapshot safety gate to refuse pushes that look corrupt.

The scraper will not `/complete` a CR with a confirmed corruption signal.

---

## 6. Pre-flight checklist

Before the scraper POSTs a payload it should be true that:

- [ ] `images` has at least the OVE gallery (>= 10 entries for vehicles with photos)
- [ ] `condition_report.overall_grade` is non-null OR the vehicle legitimately has no grade
- [ ] `condition_report.metadata.report_link.href` is set when a CR link was present in the listing
- [ ] `len(condition_report.raw_text) <= 16 KB`
- [ ] `condition_report.metadata.report_page.url` does not contain `auth.manheim.com`

---

## 7. Versioning

When the scraper schema changes, bump
`ove_scraper.schemas.ConditionReport` AND update §2 of this file in the
same commit. The golden fixture
[tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json](tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json)
is the visual reference both sides diff against.

# [PARTIALLY SUPERSEDED] Condition Report Payload Contract

> **As of 2026-04-09 this document is partially superseded by
> [SCRAPER_CR_PAYLOAD.md](SCRAPER_CR_PAYLOAD.md), which is the
> scraper-authoritative version of this contract.** Both documents
> coexist: the VPS-side (this file) describes what the VPS expects;
> the scraper-side (SCRAPER_CR_PAYLOAD.md) describes what the scraper
> actually emits. They should agree, and where they don't, the
> annotations below mark which parts of THIS document are still
> correct vs. which are out of date.
>
> **§1 (Where the data lives on Manheim) — STILL ACCURATE.**
> Despite an earlier scraper-side note claiming liquidmotors was dead,
> empirical scrape of VIN 1N4BL4EV2NN423240 on 2026-04-09 confirmed that
> the "Auto Grade only" saved-search filter does NOT exclude
> liquidmotors-hosted CRs. OVE Partner Auction listings still arrive
> with `content.liquidmotors.com/IR/{dealer_id}/{cr_id}.html` CR links,
> and the scraper does (re)dispatch through this host via direct goto
> as of the 2026-04-09 fix. The two-source model in §1 is correct.
>
> Sections still out of date:
>
> - **§3.6 — `tire_depths` key naming.** This document specified `lf` /
>   `rf` / `lr` / `rr`. The scraper's per-family parsers natively emit
>   `driver_front` / `driver_rear` / `passenger_front` / `passenger_rear`
>   (for the `manheim_inspectionreport` family) or `left_front` /
>   `left_rear` / `right_front` / `right_rear` / `spare` (for the
>   `manheim_insightcr` family). As of 2026-04-09 the normalizer also
>   adds `lf` / `rf` / `lr` / `rr` aliases that point at the same dict
>   entries so all three naming schemes coexist on every payload. The
>   VPS template can read whichever it prefers. See the golden fixture
>   at
>   [tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json](tests/fixtures/golden_detail_payload_1FTFW1RG5PFD32361.json)
>   (note: that fixture predates the alias addition; regenerate it from
>   a fresh post-restart scrape to capture the aliases).
> - **§3.10 — `raw_text` fallback behavior.** The VPS template's
>   `raw_text → announcements` last-resort fallback is the proximate cause
>   of the "JSON blob in Announcements" symptom on the 2026-04-08 push.
>   That fallback should be removed; the announcements section should
>   render empty/hidden when `condition_report.announcements` is empty.
>   The scraper now caps `raw_text` at 16 KB defensively as well.
> - **§3 — missing fields.** The scraper now also emits
>   `exterior_color`, `exterior_color_oem_name`, `exterior_paint_code`,
>   `exterior_color_rgb`, `interior_color`, `has_prior_paint`,
>   `installed_equipment`, and `high_value_options` — all sourced from
>   the OVE listing JSON embedded in the detail page. See
>   [SCRAPER_CR_PAYLOAD.md §2.2 and §2.8–2.9](SCRAPER_CR_PAYLOAD.md).
>
> --- Original document below for historical reference. ---

# Condition Report Payload Contract

**Audience:** the coding agent / engineer maintaining the OVE scraper. Read
this together with [SCRAPER_CONTRACT.md](./SCRAPER_CONTRACT.md), which
specifies the lease queue lifecycle; *this* document specifies the **shape
of the `condition_report` dict** you POST to
`/api/v1/inventory/ove/detail/{vin}`.

It exists because the current payload schema
(`backend/app/schemas/ove_inventory.py::OveDetailPushRequest.condition_report`)
is typed as `dict[str, Any]` — the VPS accepts whatever the scraper sends
and stores it verbatim. There is no server-side extraction. If a field is
missing from the payload, the frontend template renders an empty section.

The "rich" condition report template was introduced in commit `c94faa5`
(2026-03-20) and lives at
`frontend/components/ConditionReportDocument.tsx`. Every field enumerated
below is a key that template reads. If you ship a payload that omits these
fields, the CR page degrades to a vehicle summary card with no tire grid,
no damage table, no announcements, and no vehicle history — which is the
exact regression we are debugging right now.

---

## 1. Where the data lives on Manheim

A correct CR scrape for a single VIN requires data from **two** Manheim
sources. Losing either source loses half the CR.

| Source | URL pattern | What it provides |
|---|---|---|
| **OVE listing JSON** | `https://www.ove.com/...` listing page (JSON embedded in `__NEXT_DATA__` or equivalent) | Announcements, autocheck (owners/accidents), arbitrationRating, conditionGrade, gallery images, seller comments, title status, `conditionReportUrl` pointer to the Manheim CR HTML page |
| **Manheim CR HTML page** | `http://content.liquidmotors.com/IR/{dealer_id}/{cr_id}.html` (value comes from `conditionReportUrl` on the listing JSON) | Tire tread depths, damage items (panels + severity), structural damage, paint condition, interior condition, remarks, severity summary |

**Tire depths and damage items do not exist on the listing JSON.** They
only exist on the `liquidmotors.com` CR page. If your scraper stops
fetching that page, tire + damage fields go empty on every vehicle. This
is the most common failure mode.

---

## 2. Top-level payload shape

```jsonc
POST /api/v1/inventory/ove/detail/{vin}
{
  "source_platform": "manheim",
  "images": [ /* see §4 */ ],
  "condition_report": {
    /* see §3 — all keys below are siblings of this object */
  },
  "seller_comments": "string | null",
  "listing_snapshot": { /* unchanged, see ove_inventory.py */ },
  "sync_metadata": { /* freeform, for scraper-side correlation */ }
}
```

`condition_report` and `images` are the two fields that matter for the
CR page. Everything else is orthogonal.

---

## 3. `condition_report` field-by-field spec

All keys are optional from the schema's perspective (the Pydantic type is
`dict[str, Any]`), but each key below is **required for its corresponding
section of the template to render non-empty.** Where a section becomes
empty, the template either hides it or shows a placeholder, which the
user reads as "the CR is broken."

### 3.1 Identification / grade

| Key | Type | Source | Notes |
|---|---|---|---|
| `overall_grade` | `string` | OVE listing JSON `conditionGrade` or CR HTML grade header | e.g. `"3.5"`. Also drives the `CONDITION DETAIL` section via `hasStructuredData()`. |
| `structural_damage` | `string \| null` | CR HTML | e.g. `"None Reported"` or a description. |
| `paint_condition` | `string` | CR HTML | e.g. `"Very Good"`. |
| `interior_condition` | `string` | CR HTML | e.g. `"Good"`. |
| `tire_condition` | `string` | CR HTML | Summary string, separate from `tire_depths`. |

### 3.2 `metadata` — required for the Manheim CR deep link

```jsonc
"metadata": {
  "report_link": {
    "href": "http://content.liquidmotors.com/IR/15614/38020971.html",
    "title": "3.5"   // optional; used as fallback grade label
  },
  "announcementsEnrichment": {
    "announcements": ["Open Recall", "Previous Canadian"]
  }
}
```

**This is load-bearing.** The backend extracts the Manheim CR URL from
`condition_report.metadata.report_link.href` in
`backend/app/api/v1/routers/inventory.py::_extract_cr_url` and returns it
as `vehicle.condition_report_url`. The template's "See Original Condition
Report" button depends on this path. If you put the URL anywhere else
(e.g. top-level `condition_report_url` or `raw_text`), the button will
not appear.

`metadata.announcementsEnrichment.announcements` is a secondary source
the template falls back to if `announcements` (see §3.3) is missing or
empty. Populate both when possible.

### 3.3 Announcements & remarks

| Key | Type | Source | Notes |
|---|---|---|---|
| `announcements` | `string[]` | OVE listing JSON `announcementsEnrichment.announcements` | Each item ≤ 400 chars. Longer items are dropped by the template as a guard against JSON blobs leaking in. |
| `remarks` | `string[]` | CR HTML remarks section | Free-text bullets. |
| `seller_comments_items` | `string[]` | OVE listing seller block (split) | If empty, template falls back to top-level `seller_comments`. |
| `problem_highlights` | `string[]` | Scraper-derived | Optional "top issues" summary rendered above announcements. |
| `ai_summary` | `string` | Scraper-derived (optional) | Narrative summary; rendered as its own section if present. |

### 3.4 `vehicle_history`

```jsonc
"vehicle_history": {
  "owners": 2,          // int, from autocheck.ownerCount
  "accidents": 0,       // int, from autocheck.numberOfAccidents
  "engine_starts": true,  // bool, from CR HTML
  "drivable": true        // bool, from CR HTML
}
```

Drives the `VEHICLE HISTORY` section and the Engine Starts / Drivable
bullets in the grade card.

### 3.5 `damage_items` + `damage_summary`

```jsonc
"damage_items": [
  {
    "section": "front_bumper",
    "section_label": "Front Bumper",
    "panel": "Lower",
    "condition": "Scratched",
    "reported_severity": "Minor",
    "severity_label": "Minor",
    "severity_color": "yellow",  // one of: green | yellow | orange | red | gray
    "severity_rank": 2
  }
],
"damage_summary": {
  "total_items": 7,
  "by_color": { "yellow": 5, "orange": 2 },
  "by_section": { "front_bumper": 2, "driver_door": 1 },
  "structural_issue": false
}
```

Source: CR HTML damage map (SVG overlay + tabular list).
`severity_color` drives the colored pill in the damage table — the
template classes are `cr-severity-{color}` so stick to the five values
above.

### 3.6 `tire_depths`

```jsonc
"tire_depths": {
  "lf": {
    "position_label": "LF",
    "tread_depth": "6/32",
    "brand": "Michelin",
    "size": "245/45R19",
    "wheel_type": "Alloy"
  },
  "rf": { /* ... */ },
  "lr": { /* ... */ },
  "rr": { /* ... */ }
}
```

Keyed map. Position keys are lowercase (`lf`, `rf`, `lr`, `rr`, or
`spare`). Source: CR HTML tire section. The `TIRE CONDITION` section is
hidden entirely if this object is missing or empty.

### 3.7 Title info

| Key | Type | Source |
|---|---|---|
| `title_status` | `string` | CR HTML |
| `title_state` | `string` (2-letter) | CR HTML |
| `title_branding` | `string` | CR HTML |

The `TITLE INFORMATION` section only renders if at least one of these is
present.

### 3.8 Color fallbacks

| Key | Type | Notes |
|---|---|---|
| `exterior_color` | `string` | Fallback only — the top-level vehicle record's `exterior_color` wins. |
| `interior_color` | `string` | Same. |

### 3.9 `severity_summary`

`string` — a one-line narrative (e.g. `"7 minor items, 2 moderate, no
structural."`) rendered below the damage table. Optional.

### 3.10 `raw_text`

`string` — the full listing/CR dump as a single blob. The template uses
this **only as a last-resort fallback** when structured fields are
missing (old scraper format). **Do not rely on this.** The template
explicitly does *not* regex announcements out of `raw_text` when
`raw_text` looks JSON-shaped, because that leaked a 51k-character JSON
blob into a single bullet — see the inline comment at
`ConditionReportDocument.tsx:124`.

If you send `raw_text`, cap it. Anything over ~16kb is almost certainly
a bug.

---

## 4. `images` — CR-grade gallery

This is a **separate** payload field from `condition_report`, but it is
part of the same regression (scraper sending 1 image instead of the full
set). Schema:

```jsonc
"images": [
  {
    "url": "https://cdn.manheim.com/.../photo1.jpg",
    "role": "gallery",          // gallery | inspection | disclosure | hero
    "display_order": 0,
    "is_primary": true,
    "source_image_id": "optional scraper-side id",
    "metadata": {}
  }
]
```

Rules:
- **Send the full OVE gallery**, not just the first thumbnail. The OVE
  listing JSON has the complete image array; use it.
- **Do not send** `.svg`, `.gif`, or `ready_logistics.png` — these are
  filtered out by the frontend and treated as non-photo assets.
  ([resolveReportImages](../frontend/components/ConditionReportDocument.tsx#L748))
- Strip any `?size=` query parameter or let the frontend strip it (it
  does, for deduplication). But try to send the original-resolution URL.
- Mark one image `is_primary: true`. This becomes the hero.
- `role: "inspection"` and `role: "disclosure"` images are rendered in
  their own sub-galleries on the CR page.

The OVE listing JSON is the authoritative source — do not fall back to
Imagin Studio / stock photos for auction inventory. Imagin belongs to
retail inventory only.

---

## 5. Minimal payload checklist

Before POSTing, verify the payload contains at least these keys or
expect a visibly broken CR page:

- [ ] `condition_report.metadata.report_link.href` — URL to liquidmotors CR page
- [ ] `condition_report.announcements` OR `condition_report.metadata.announcementsEnrichment.announcements`
- [ ] `condition_report.vehicle_history.owners` and `.accidents`
- [ ] `condition_report.damage_items` (array, possibly empty — but only empty if the CR page itself reports no damage)
- [ ] `condition_report.tire_depths` with 4 positions (lf/rf/lr/rr)
- [ ] `condition_report.overall_grade`
- [ ] `images` with **at least** the full OVE gallery (typically 10–40 items)

If `damage_items` is empty AND `tire_depths` is empty AND the listing was
scraped successfully, your scraper almost certainly failed to fetch the
liquidmotors CR HTML — that is the single most common cause of "CR
looks empty" incidents. Log the CR URL fetch separately so this failure
is visible.

---

## 6. Partial-data protocol

Manheim changes their HTML periodically and individual fields will break
before the whole page does. The scraper should:

1. **Never** silently drop a section. If tire depths fail to parse but
   damage items succeed, send the damage items and omit `tire_depths`
   (do not send `{}`). The template hides the tire section when the
   object is missing; sending `{}` renders nothing but costs a diff.
2. Add a `condition_report.metadata.scrape_warnings` array with one
   string per field that failed to parse, e.g.
   `["tire_depths: selector .tire-grid not found"]`. The template does
   not render this today, but the field is reserved for it and the
   backend will store it.
3. If the liquidmotors CR page itself 404s or times out, still POST the
   listing-JSON-derived fields (announcements, owners/accidents,
   overall_grade from listing, images, seller comments) and **use
   `/fail` with `error_category=page_structure_changed` or
   `transient_network`** so the request retries. Do not `/complete` a
   half-empty CR.
4. If the liquidmotors CR page is permanently missing (listing type that
   has no CR), use `/terminal` with
   `reason=unsupported_listing_type`.

---

## 7. Golden example

A canonical example payload belongs at
`backend/tests/fixtures/ove/condition_report_golden.json` with:

- 1 vehicle with `overall_grade=3.5`
- 4 tire positions with real depths
- 7–10 damage items across 3+ severity colors
- 2+ announcements
- `vehicle_history` with non-zero accidents
- 15+ gallery images
- Full `metadata.report_link.href`

Both sides can diff their output against that file. **This fixture does
not exist yet** — capturing one from a known-good snapshot (or a `c94faa5`-era
`raw_text` dump) is the next concrete step before we can regression-test
the scraper.

---

## 8. Schema evolution

Today `condition_report` is `dict[str, Any]`. Once the contract above is
stable, the backend should replace it with a typed
`ConditionReportPayload(BaseModel)` in `backend/app/schemas/ove_inventory.py`
that validates the shape at the boundary. Until then, the VPS accepts
anything, which is what allowed this regression to go unnoticed for a
week — the backend has no way to say "this payload is missing its tire
grid." Typing the schema is the long-term fix; this document is the
short-term fix.

---

## 9. Quick reference — canonical field list

These are every key the rich template reads, in the order they appear in
`ConditionReportDocument.tsx`:

```
condition_report.metadata
condition_report.metadata.report_link.href
condition_report.metadata.report_link.title
condition_report.metadata.announcementsEnrichment.announcements
condition_report.announcements
condition_report.raw_text                   (fallback only; bounded)
condition_report.vehicle_history.engine_starts
condition_report.vehicle_history.drivable
condition_report.vehicle_history.owners
condition_report.vehicle_history.accidents
condition_report.damage_items[].section
condition_report.damage_items[].section_label
condition_report.damage_items[].panel
condition_report.damage_items[].condition
condition_report.damage_items[].reported_severity
condition_report.damage_items[].severity_color
condition_report.damage_items[].severity_label
condition_report.damage_items[].severity_rank
condition_report.damage_summary.total_items
condition_report.damage_summary.by_color
condition_report.damage_summary.by_section
condition_report.damage_summary.structural_issue
condition_report.tire_depths[pos].position_label
condition_report.tire_depths[pos].tread_depth
condition_report.tire_depths[pos].brand
condition_report.tire_depths[pos].size
condition_report.tire_depths[pos].wheel_type
condition_report.problem_highlights[]
condition_report.remarks[]
condition_report.seller_comments_items[]
condition_report.severity_summary
condition_report.ai_summary
condition_report.title_status
condition_report.title_state
condition_report.title_branding
condition_report.overall_grade
condition_report.structural_damage
condition_report.paint_condition
condition_report.interior_condition
condition_report.tire_condition
condition_report.exterior_color             (fallback)
condition_report.interior_color             (fallback)
images[].url
images[].role
images[].display_order
images[].is_primary
```

That is the universe. If a field is not on this list, the template does
not read it and the scraper should not waste time populating it.

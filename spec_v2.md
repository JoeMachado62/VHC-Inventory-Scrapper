# Specification: OVE Auction Scraping Module

**Project ID:** d5fee6ca-9340-43f0-8013-ae6074d719d6  
**Version:** 2  
**Parent Project:** Virtual-CarHub (`github.com/JoeMachado62/Virtual-CarHub`)

## Project Summary

This specification defines the behavior of an autonomous AI agent responsible for executing an hourly cronjob to synchronize vehicle inventory from OVE.com to the Virtual CarHub (VCH) live database. The agent runs on a **separate local PC** (the "scraper node") that maintains a manually authenticated OVE.com browser session. It communicates with the main VCH backend server exclusively through the VCH REST API.

The agent will attach to an open browser session, download specific saved searches, merge and deduplicate the data, map it to the VCH `Vehicle` model schema (which aligns with the Marketcheck Cars API field conventions), and push the payload to the VCH API for safe, targeted database upserts. Additionally, it includes an on-demand deep-scraping module to retrieve specific vehicle details while strictly redacting sensitive auction information.

---

## Architecture Overview

```
┌──────────────────────────────────┐         HTTPS / REST API          ┌──────────────────────────────────┐
│         SCRAPER NODE             │ ──────────────────────────────────▶ │       VCH BACKEND SERVER         │
│   (Local PC - your desk)         │                                    │   (VPS / Docker Compose)          │
│                                  │                                    │                                  │
│  ┌──────────────────────────┐    │   POST /v1/inventory/ove-ingest   │  ┌──────────────────────────┐    │
│  │ Module A                 │    │   Header: X-Service-Token         │  │ FastAPI app (app/main.py)│    │
│  │ Browser Automation       │────│──────────────────────────────────▶│  │  ├─ /v1/inventory/*      │    │
│  │ (CDP attach, CSV export) │    │                                    │  │  ├─ /v1/webhooks/*       │    │
│  └──────────┬───────────────┘    │   POST /v1/inventory/ove-detail   │  │  ├─ /v1/admin/*          │    │
│             │ local CSV files    │   Header: X-Service-Token         │  │  └─ /v1/sourcing/*       │    │
│  ┌──────────▼───────────────┐    │──────────────────────────────────▶│  └──────────┬───────────────┘    │
│  │ Module B                 │    │                                    │             │                    │
│  │ CSV Transform & Merge    │    │   GET /v1/inventory/{vin}         │  ┌──────────▼───────────────┐    │
│  │ (Dedup, schema mapping)  │────│──────────────────────────────────▶│  │ PostgreSQL / SQLite      │    │
│  └──────────┬───────────────┘    │                                    │  │ (vehicles table)          │    │
│             │ JSON payload       │   Webhook callback (optional)      │  └──────────────────────────┘    │
│  ┌──────────▼───────────────┐    │◀──────────────────────────────────│                                  │
│  │ Module C                 │    │                                    │                                  │
│  │ API Client               │    │                                    │                                  │
│  │ (Push to VCH API)        │    │                                    │                                  │
│  └──────────────────────────┘    │                                    │                                  │
│                                  │                                    │                                  │
│  ┌──────────────────────────┐    │                                    │                                  │
│  │ Module D                 │    │                                    │                                  │
│  │ On-Demand Deep Scrape    │────│──── triggered by VCH API call ────│                                  │
│  │ (VIN detail, redaction)  │    │                                    │                                  │
│  └──────────────────────────┘    │                                    │                                  │
└──────────────────────────────────┘                                    └──────────────────────────────────┘
```

### Deployment Model

The scraper node and VCH backend are **separate machines**. The scraper node must be a local PC where OVE.com is authenticated in a persistent browser session (Chrome/Edge with `--remote-debugging-port`). The VCH backend runs on a VPS or cloud instance behind Docker Compose.

Communication is **unidirectional for the hourly sync** (scraper → VCH API) and **bidirectional for the on-demand deep scrape** (VCH triggers scraper, scraper returns data to VCH API).

---

## Problem Statement

Develop an autonomous agent that executes an hourly cronjob to synchronize vehicle inventory from OVE.com to the VCH live database, alongside an on-demand deep-scraping module.

The hourly sync must attach to an existing browser session, export two specific saved searches ('East Hub Official' and 'West Hub Official'), merge the CSVs, deduplicate by VIN (retaining the newest record), map the data to the VCH `Vehicle` model schema, and push the payload to the VCH API's dedicated OVE ingest endpoint for safe upserts.

The on-demand module must trigger when a buyer requests 'more info' on a vehicle (initiated from the VCH frontend), search the specific VIN on OVE to extract images, condition reports, and seller comments, while strictly redacting sensitive auction data ('Listing Seller' and 'current bid'), and push the enrichment data back to the VCH API.

---

## Integration with Virtual CarHub Backend

### Authentication: Service Token

All scraper-to-VCH API calls authenticate using the existing `X-Service-Token` header mechanism defined in `backend/app/api/deps.py:require_service_token`. The scraper node reads this token from its local `.env` file and includes it on every request.

```
X-Service-Token: <value matching VCH backend's SERVICE_TOKEN env var>
```

No JWT or user login is required. The service token is the same credential used by all internal/machine-to-machine calls in the VCH system.

### Required New Endpoints on VCH Backend

The following endpoints must be added to the VCH backend to receive data from the scraper node. They belong in a new router file: `backend/app/api/v1/routers/ove_sync.py`, registered in `backend/app/api/v1/router.py` with `prefix="/inventory/ove"`.

#### 1. `POST /v1/inventory/ove/ingest` — Hourly Bulk Upsert

Receives the merged, deduplicated, schema-mapped vehicle array from the scraper node.

**Request:**
```json
{
  "vehicles": [
    {
      "vin": "1HGCM82633A004352",
      "year": 2021,
      "make": "Honda",
      "model": "Accord",
      "trim": "EX-L",
      "body_type": "Sedan",
      "engine_type": "Gasoline",
      "drivetrain": "FWD",
      "odometer": 34521,
      "condition_grade": "3.5",
      "price_asking": 22150.0,
      "price_wholesale_est": 19800.0,
      "location_zip": "30301",
      "location_state": "GA",
      "source_type": "ove_auction",
      "source_url": "https://www.ove.com/vehicle/<listing_id>",
      "images": [],
      "features_raw": ["Leather", "Sunroof", "Honda Sensing"],
      "features_normalized": {},
      "available": true,
      "ove_listing_timestamp": "2025-07-15T14:30:00Z"
    }
  ],
  "sync_metadata": {
    "east_hub_record_count": 100,
    "west_hub_record_count": 50,
    "duplicates_removed": 5,
    "skipped_no_vin": 2,
    "scraper_node_id": "joes-desktop",
    "scraper_version": "1.0.0"
  }
}
```

**Behavior:**
- Iterates each vehicle in the array.
- Calls the existing `upsert_vehicle_with_source_priority()` function from `backend/app/services/inventory_service.py` with `incoming_source="ove_auction"`.
- Feeds images into the existing image pipeline via `sync_marketcheck_source_assets()` and `ensure_tier2_hero_job()` (these functions are source-agnostic despite the name).
- Logs an `AuditEvent` with `event_type="ove_sync"` and `actor="system"`.
- Returns the structured execution log (see Required Output Format).

**Response:**
```json
{
  "status": "ok",
  "data": {
    "timestamp": "2025-07-15T15:00:00Z",
    "execution_status": "Success",
    "east_hub_record_count": 100,
    "west_hub_record_count": 50,
    "duplicates_removed": 5,
    "db_records_added": 42,
    "db_records_updated": 103,
    "error_details": []
  }
}
```

#### 2. `POST /v1/inventory/ove/detail/{vin}` — On-Demand Deep Scrape Result Push

Receives the redacted enrichment data after the scraper node completes a deep scrape.

**Request:**
```json
{
  "images": [
    "https://cdn.ove.com/photos/abc123/1.jpg",
    "https://cdn.ove.com/photos/abc123/2.jpg"
  ],
  "condition_report": {
    "overall_grade": "3.5",
    "structural_damage": false,
    "paint_condition": "Good",
    "interior_condition": "Excellent",
    "tire_condition": "Good",
    "announcements": ["Prior Rental"],
    "raw_text": "..."
  },
  "seller_comments": "Vehicle runs and drives. Minor scratches on rear bumper."
}
```

**Behavior:**
- Updates the `Vehicle` row for the given VIN (images, condition_grade, features_normalized sub-fields).
- Feeds images into the image pipeline. OVE auction images should be tagged as `ImageTier.TIER4_INSPECTION` with `ImageContext.INSPECTION` and `source_kind="ove_auction"`, per the VCH Image Processing Pipeline Spec v2.
- Logs an `AuditEvent` with `event_type="ove_detail_scrape"`.
- Note: `Listing Seller` and `current bid` must already be stripped by the scraper node before this payload is sent. The VCH backend should additionally validate their absence as a safety net.

#### 3. `POST /v1/inventory/ove/detail/{vin}/request` — Trigger Deep Scrape (VCH → Scraper)

This endpoint is called by the VCH frontend/agent when a buyer clicks "More Info." It does not perform the scrape itself. Instead, it queues a request that the scraper node picks up.

**Implementation options (pick one during build):**

- **Option A — Polling:** The scraper node polls `GET /v1/inventory/ove/detail/pending` every 30 seconds for queued VIN requests. The VCH backend stores pending requests in a lightweight `ove_scrape_requests` table or Redis queue.
- **Option B — Webhook push:** The VCH backend sends an HTTP POST to a webhook URL exposed by the scraper node (e.g., `http://<scraper-ip>:9100/scrape`). Requires the scraper node to run a small HTTP server and the VPS to have network access to the scraper's IP (may require a tunnel like ngrok, Cloudflare Tunnel, or Tailscale).
- **Option C — Redis pub/sub:** Both the VCH backend and scraper node connect to the same Redis instance. The VCH backend publishes to a `ove:scrape_requests` channel. This requires the scraper node to have network access to the VCH Redis instance.

**Recommended: Option A (Polling)** for simplicity — it works through NAT/firewalls without tunneling, and the 30-second polling interval is acceptable for a "more info" request.

### Source Priority Registration

The existing `SOURCE_PRIORITY` dict in `backend/app/services/inventory_service.py` must be updated to include the new source type:

```python
SOURCE_PRIORITY: dict[str, int] = {
    "marketcheck": 1,
    "dealer_partner": 2,
    "dealer_wholesale": 2,
    "auction": 3,
    "ove_auction": 3,    # ← ADD: same priority as generic auction
}
```

This ensures that OVE auction data fills in gaps but does not overwrite higher-priority MarketCheck or dealer partner data if both exist for the same VIN.

### VCH Constants Update

Add `OVE` to the `AuctionPlatform` enum in `backend/app/core/constants.py`:

```python
class AuctionPlatform(StrEnum):
    MANHEIM = "manheim"
    OPENLANE = "openlane"
    ALLY_SMART_AUCTION = "ally-smart-auction"
    OVE = "ove"    # ← ADD
```

### VCH Config Update

Add the following to `backend/app/core/config.py` (class `Settings`):

```python
# OVE scraper integration
ove_sync_enabled: bool = Field(default=False, alias="OVE_SYNC_ENABLED")
ove_scraper_webhook_url: str = Field(default="", alias="OVE_SCRAPER_WEBHOOK_URL")
```

And a convenience property:

```python
@property
def has_ove_sync(self) -> bool:
    return self.ove_sync_enabled
```

### CSV-to-Vehicle Schema Field Mapping

The scraper's Module B must map OVE CSV columns to the VCH `Vehicle` model fields defined in `backend/app/models/entities.py`. Below is the reference mapping. The exact OVE CSV column names should be confirmed from a sample export, but the general mapping is:

| OVE CSV Column (expected) | VCH Vehicle Field | Notes |
|---|---|---|
| `VIN` | `vin` | Primary key. Must be 17 chars. Uppercase. |
| `Year` | `year` | Integer. Required. |
| `Make` | `make` | String. Required. |
| `Model` | `model` | String. Required. |
| `Trim` | `trim` | String. Optional. |
| `Body Style` | `body_type` | String. Optional. |
| `Engine` | `engine_type` | String. Optional. |
| `Drivetrain` / `Drive` | `drivetrain` | String. Optional. |
| `Mileage` / `Odometer` | `odometer` | Integer. Optional. |
| `Condition` / `Grade` | `condition_grade` | String. Optional. (e.g., "3.5") |
| `Asking Price` / `Buy Now` / `Floor Price` | `price_asking` | Float. Required. Use buy-now or floor price. |
| `MMR` / `Wholesale Value` | `price_wholesale_est` | Float. Optional. |
| `Location ZIP` / `Seller ZIP` | `location_zip` | String. Optional. |
| `State` | `location_state` | 2-char uppercase. Optional. |
| `OVE Listing ID` | `listing_id` | String. Optional. |
| N/A (constructed) | `source_type` | Always `"ove_auction"`. |
| N/A (constructed) | `source_url` | Construct from listing ID if available. |
| N/A | `images` | Empty list `[]` in hourly sync. Populated by deep scrape. |
| N/A (constructed) | `available` | Default `True`. |
| `Last Updated` / `List Date` | `last_seen_active` | Parse to ISO-8601 datetime. Used for dedup. |
| N/A | `features_raw` | Extract from CSV if available, else `[]`. |
| Various (color, transmission, etc.) | `features_normalized` | Dict. Map available columns. |
| N/A | `quality_firewall_pass` | Default `True`. |

**IMPORTANT:** The following OVE CSV columns must NEVER be included in the payload sent to the VCH API:
- `Listing Seller` / `Seller Name` — redacted
- `Current Bid` / `High Bid` — redacted

### Image Pipeline Integration

When the deep scrape returns images for a VIN, the VCH backend should process them through the existing four-tier image pipeline:

1. OVE auction photos are stored as `ImageTier.SOURCE_CACHE` with `source_kind="ove_auction"` and `source_platform=AuctionPlatform.OVE`.
2. The first image triggers a `ImageTier.TIER2_HERO` job via `ensure_tier2_hero_job()`.
3. If the vehicle is in `ACQUISITION_PENDING` or later deal stage, images with condition report context should be tagged as `ImageTier.TIER4_INSPECTION` with `ImageContext.INSPECTION`.
4. Per the VCH Image Processing Pipeline Spec v2: auction inspection images are preserved unmodified for dispute resolution.

### Audit Trail Integration

Every sync and deep scrape event must create an `AuditEvent` record in the VCH database using the existing `log_event()` utility from `backend/app/services/audit_service.py`:

- **Hourly sync success:** `event_type="ove_sync"`, `actor="system"`, `payload_json=<execution log>`
- **Hourly sync failure:** `event_type="ove_sync_failure"`, `actor="system"`, `payload_json=<error details>`
- **Deep scrape result:** `event_type="ove_detail_scrape"`, `actor="system"`, `payload_json=<redacted result summary>`
- **Deep scrape request queued:** `event_type="ove_detail_requested"`, `actor="agent"` or `"buyer"`, `payload_json={"vin": "..."}`

---

## Scraper Node Configuration

The scraper node requires a `.env` file with the following:

```env
# VCH API connection
VCH_API_BASE_URL=https://your-vch-server.com/v1
VCH_SERVICE_TOKEN=<must match the SERVICE_TOKEN in VCH backend .env>

# Browser automation
CHROME_DEBUG_PORT=9222
CHROME_DEBUG_HOST=localhost

# Scraper behavior
SYNC_INTERVAL_SECONDS=3600
DEEP_SCRAPE_POLL_INTERVAL_SECONDS=30
SCRAPER_NODE_ID=joes-desktop

# Logging
LOG_LEVEL=INFO
LOG_FILE_PATH=./logs/ove_scraper.log
```

### Scraper Node API Client

Module C must implement a thin HTTP client that wraps all VCH API calls. Pseudocode:

```python
class VCHApiClient:
    def __init__(self, base_url: str, service_token: str):
        self.base_url = base_url
        self.headers = {"X-Service-Token": service_token, "Content-Type": "application/json"}

    def push_ove_ingest(self, vehicles: list[dict], sync_metadata: dict) -> dict:
        """POST /v1/inventory/ove/ingest"""
        response = httpx.post(
            f"{self.base_url}/inventory/ove/ingest",
            json={"vehicles": vehicles, "sync_metadata": sync_metadata},
            headers=self.headers,
            timeout=120,
        )
        response.raise_for_status()
        return response.json()

    def push_ove_detail(self, vin: str, detail_payload: dict) -> dict:
        """POST /v1/inventory/ove/detail/{vin}"""
        response = httpx.post(
            f"{self.base_url}/inventory/ove/detail/{vin}",
            json=detail_payload,
            headers=self.headers,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def poll_pending_detail_requests(self) -> list[str]:
        """GET /v1/inventory/ove/detail/pending"""
        response = httpx.get(
            f"{self.base_url}/inventory/ove/detail/pending",
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("vins", [])

    def check_health(self) -> bool:
        """GET /health"""
        try:
            response = httpx.get(f"{self.base_url.rsplit('/v1', 1)[0]}/health", timeout=10)
            return response.status_code == 200
        except Exception:
            return False
```

---

## Acceptance Criteria

1. **Zero Duplicate VINs in Payload**
   * **Pass Condition:** Maximum frequency of any VIN is exactly 1 in the array sent to `/v1/inventory/ove/ingest`.
   * **Verification Method:** Count the frequency of each VIN in the final merged dataset prior to API push.

2. **Safe Database Operations**
   * **Pass Condition:** 100% of operations are INSERT or UPDATE (upserts). 0 instances of DROP, DELETE, or TRUNCATE commands. The VCH backend's existing `upsert_vehicle_with_source_priority()` function enforces this.
   * **Verification Method:** Monitor the SQL queries executed during the ingest endpoint handler.

3. **Strict Data Redaction on Deep Scrape**
   * **Pass Condition:** The keys or values for 'Listing Seller' and 'current bid' are entirely absent from the JSON payload sent to `/v1/inventory/ove/detail/{vin}`. The VCH backend additionally validates absence as a safety net.
   * **Verification Method:** Inspect the JSON payload at both the scraper outbound and VCH inbound.

4. **Structured Execution Logging**
   * **Pass Condition:** Log contains `timestamp`, `execution_status`, `east_hub_record_count`, `west_hub_record_count`, `duplicates_removed`, `db_records_added`, `db_records_updated`, and `error_details`.
   * **Verification Method:** Validate the response from `/v1/inventory/ove/ingest` against the required JSON schema. Also validate the local scraper log file.

5. **API Connectivity**
   * **Pass Condition:** Scraper node successfully calls `GET /health` on the VCH backend before each sync cycle. If health check fails, the sync halts and logs the failure without retrying destructively.
   * **Verification Method:** Check the scraper log for health check results before each sync.

6. **VCH Audit Trail**
   * **Pass Condition:** Every successful sync creates an `AuditEvent` with `event_type="ove_sync"` in the VCH database. Every failed sync creates one with `event_type="ove_sync_failure"`.
   * **Verification Method:** Query the `audit_events` table via `GET /v1/admin/audit-log`.

---

## Constraint Architecture

### Musts (Required Actions)

* **Session Management:** Attach to the existing open browser session debugging port rather than launching a new authenticated session.
* **Schema Enforcement:** Map all data to the VCH `Vehicle` model schema before pushing to the API. Required fields: `vin` (17 chars), `year`, `make`, `model`, `price_asking`. `source_type` is always `"ove_auction"`.
* **API-Only Database Access:** The scraper node must NEVER connect directly to the VCH PostgreSQL/SQLite database. All reads and writes go through the VCH REST API with `X-Service-Token` authentication.
* **Deduplication Logic:** Retain the vehicle record with the most recently updated timestamp when resolving duplicate VINs across the East Hub and West Hub CSVs.
* **Data Validation:** Skip any CSV row missing a VIN, omit it from the API payload, and append it to the `error_details` log and the `sync_metadata.skipped_no_vin` counter.
* **Rate Limiting:** Limit the automated sync execution to exactly once per hour.
* **Health Check Before Sync:** Call `GET /health` on the VCH backend before starting each sync. If unreachable, halt and log.
* **Idempotent Payloads:** The ingest payload must be safe to retry. If a network error occurs after the scraper sends the payload but before it receives the response, re-sending the same payload must not create duplicates (enforced by VIN-keyed upserts on the backend).

### Must Nots (Prohibited Actions)

* **No Authentication Guessing:** Never attempt to guess credentials or log in if the existing browser session is expired or logged out.
* **Strict Navigation:** Never click on or export any saved searches other than exactly 'East Hub Official' and 'West Hub Official'.
* **No Destructive DB Commands:** The scraper never touches the database directly. The VCH backend's ingest endpoint uses only upserts.
* **No Sensitive Data Exposure:** Never include 'Listing Seller' or 'current bid' in any payload sent to the VCH API, whether during hourly sync or on-demand deep scrape.
* **No Direct Database Connections:** The scraper must not import SQLAlchemy, establish database connections, or execute SQL. All persistence goes through the VCH API.

### Preferences

* **Autonomous Correction:** Automatically correct minor data formatting issues (e.g., trimming whitespace, standardizing date formats, uppercasing VINs and state codes) to match the VCH Vehicle schema without halting the workflow.
* **Batch Size:** If the total payload exceeds 500 vehicles, split into batches of 200 and send sequentially to avoid API timeouts.
* **Retry with Backoff:** On transient API errors (5xx, network timeout), retry up to 3 times with exponential backoff (2s, 4s, 8s) before marking the sync as failed.

### Escalation Triggers (Halt & Alert)

* The OVE.com session is logged out, expired, or requires multi-factor authentication.
* The UI layout of OVE.com changes, making 'My OVE', 'Saved Searches', or 'Export List' buttons unfindable.
* The VCH backend health check fails (API unreachable).
* The VCH API rejects the ingest payload with a 422 schema validation error.
* The VCH API returns a 401 (service token mismatch or expired).
* The on-demand VIN search for 'more info' returns no results or an error page on OVE.

---

## Break Patterns

To ensure modularity and safety, the agent must adhere to the following architectural break patterns:

* **Module A — Browser Automation** (Isolate from data transformation)
  * *Scope:* Hourly Sync Workflow
  * *Implementation:* Handles attaching to the Chrome DevTools Protocol (CDP) session, navigating to OVE.com saved searches, clicking export, and saving CSVs to a local directory. Has zero knowledge of the VCH schema or API.

* **Module B — CSV Transform & Merge** (Isolate from browser and API)
  * *Scope:* Data Processing
  * *Implementation:* Reads the local CSV files, merges East Hub and West Hub, deduplicates by VIN (keeping newest), maps columns to VCH `Vehicle` model fields per the field mapping table above, strips redacted fields, validates required fields, and produces a clean JSON array. Has zero knowledge of browser automation or HTTP.

* **Module C — VCH API Client** (Isolate from data processing)
  * *Scope:* Network Communication
  * *Implementation:* Receives a JSON payload from Module B and pushes it to the VCH API endpoint. Handles authentication, retries, health checks, and error handling. Returns the structured execution log from the API response.

* **Module D — On-Demand Deep Scrape** (Decouple from hourly sync)
  * *Scope:* On-Demand Detail Enrichment
  * *Implementation:* Operates as a standalone loop or event handler. Polls `GET /v1/inventory/ove/detail/pending` for queued VIN requests, uses Module A's browser session to search and scrape the VIN detail page on OVE, applies redaction, and pushes results to `POST /v1/inventory/ove/detail/{vin}` via Module C. Completely independent of the hourly cronjob.

---

## Evaluation Test Cases

### Test Case 1: Standard Merge and Deduplication

* **Input Scenario:** East Hub CSV has 100 records, West Hub CSV has 50 records. 5 VINs overlap. The overlapping West Hub records have a newer timestamp.
* **Pass Condition:** Final payload sent to `/v1/inventory/ove/ingest` contains 145 vehicles, 0 duplicate VINs, and newer timestamps are retained. `sync_metadata.duplicates_removed` equals 5.
* **Expected Output:** VCH API returns `db_records_added` + `db_records_updated` = 145 (or less if some VINs already existed from MarketCheck). An `AuditEvent` with `event_type="ove_sync"` is created.

### Test Case 2: Missing VIN Handling

* **Input Scenario:** The downloaded East Hub CSV contains 2 rows with an empty VIN column.
* **Pass Condition:** The 2 rows are excluded from the API payload. `sync_metadata.skipped_no_vin` equals 2. The API ingest succeeds for the remaining rows.
* **Expected Output:** The API response's `error_details` lists the skipped rows. The `AuditEvent` payload includes the skipped row info.

### Test Case 3: On-Demand Deep Scrape Redaction

* **Input Scenario:** User requests 'more info' for VIN `1HGCM82633A00000`. The OVE listing contains images, condition report, seller 'ABC Motors', and current bid '$15,000'.
* **Pass Condition:** The JSON sent to `/v1/inventory/ove/detail/1HGCM82633A00000` contains images and condition report, but 'Listing Seller' and 'current bid' keys/values are completely absent.
* **Expected Output:** The VCH backend stores the images in the image pipeline as `TIER4_INSPECTION` assets. An `AuditEvent` with `event_type="ove_detail_scrape"` is created.

### Test Case 4: Authentication Failure Handling

* **Input Scenario:** Agent attempts to attach to the browser, but the OVE.com page is sitting at the login screen.
* **Pass Condition:** Zero attempts made to input credentials; workflow halts and escalates safely.
* **Expected Output:** Agent immediately halts execution. No CSVs are downloaded. No API calls are made to VCH (except optionally logging the failure). Local JSON log reports 'Failure' with 'Authentication Error' in `error_details`.

### Test Case 5: VCH API Unreachable

* **Input Scenario:** The scraper node completes CSV download and merge successfully, but the VCH backend `/health` endpoint is unreachable.
* **Pass Condition:** The scraper halts before sending the ingest payload. CSVs are preserved locally for the next successful sync cycle. The local log reports 'Failure' with 'VCH API unreachable' in `error_details`.
* **Expected Output:** No data is lost. The next hourly sync will re-export from OVE with fresh data.

### Test Case 6: Service Token Mismatch

* **Input Scenario:** The scraper's `.env` has an outdated `VCH_SERVICE_TOKEN` value.
* **Pass Condition:** The VCH API returns HTTP 401. The scraper logs the authentication error and halts without retrying.
* **Expected Output:** Local log reports 'Failure' with 'VCH API authentication failed (401)' in `error_details`.

### Test Case 7: Source Priority Conflict

* **Input Scenario:** VIN `1HGCM82633A004352` already exists in the VCH database with `source_type="marketcheck"` (priority 1). The OVE sync sends the same VIN with `source_type="ove_auction"` (priority 3).
* **Pass Condition:** The VCH backend's `upsert_vehicle_with_source_priority()` uses the secondary fill logic — it fills in missing fields from OVE data but does not overwrite existing MarketCheck data. The vehicle's `source_type` remains `"marketcheck"`.
* **Expected Output:** The API response counts this VIN as `skipped_priority` (not `updated`).

---

## Required Output Format

### Scraper Node Local Log

Upon completion of each hourly sync attempt, the scraper writes a structured JSON log entry locally:

```json
{
  "timestamp": "ISO-8601 String",
  "execution_status": "Success | Failure",
  "east_hub_record_count": 0,
  "west_hub_record_count": 0,
  "duplicates_removed": 0,
  "skipped_no_vin": 0,
  "api_push_status": "Success | Failure | Skipped",
  "api_response": {},
  "error_details": []
}
```

### VCH API Ingest Response

The `/v1/inventory/ove/ingest` endpoint returns:

```json
{
  "status": "ok",
  "data": {
    "timestamp": "ISO-8601 String",
    "execution_status": "Success | Failure",
    "east_hub_record_count": 0,
    "west_hub_record_count": 0,
    "duplicates_removed": 0,
    "db_records_added": 0,
    "db_records_updated": 0,
    "db_records_skipped_priority": 0,
    "error_details": []
  }
}
```

This matches the VCH API's standard `ok()` response wrapper used throughout the codebase (`backend/app/core/responses.py`).

---

## Summary of VCH Codebase Changes Required

For the AI coding agent working on the VCH backend, here is the complete list of files to create or modify:

### New Files

1. **`backend/app/api/v1/routers/ove_sync.py`** — New router with three endpoints: `POST /ingest`, `POST /detail/{vin}`, `GET /detail/pending`, `POST /detail/{vin}/request`.
2. **`backend/app/schemas/ove.py`** — Pydantic request/response models for the OVE endpoints.
3. **`backend/app/services/ove_ingest_service.py`** — Service layer that wraps `upsert_vehicle_with_source_priority()`, image pipeline calls, and audit logging for OVE-sourced vehicles. (Optional — could be handled inline in the router if preferred.)

### Modified Files

4. **`backend/app/api/v1/router.py`** — Add: `from app.api.v1.routers import ove_sync` and `api_router.include_router(ove_sync.router, prefix="/inventory/ove", tags=["ove-sync"])`.
5. **`backend/app/services/inventory_service.py`** — Add `"ove_auction": 3` to the `SOURCE_PRIORITY` dict.
6. **`backend/app/core/constants.py`** — Add `OVE = "ove"` to the `AuctionPlatform` enum.
7. **`backend/app/core/config.py`** — Add `ove_sync_enabled` and `ove_scraper_webhook_url` fields to `Settings`, plus the `has_ove_sync` property.
8. **`backend/.env.example`** — Add `OVE_SYNC_ENABLED=false` and `OVE_SCRAPER_WEBHOOK_URL=` entries.

### No Changes Required

- **Database migrations:** The `vehicles` table schema does not change. The `source_type` column is a free-form `String(30)` that already accepts any value, so `"ove_auction"` works without a migration. The `AuctionPlatform` enum addition only affects the `vehicle_image_assets` table's `source_platform` column (Alembic migration needed for that enum if using PostgreSQL with native enums).
- **Frontend:** No immediate frontend changes are needed. OVE-sourced vehicles appear in search results alongside MarketCheck vehicles using the existing inventory endpoints.

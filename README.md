# OVE Scraper Module

Standalone local-PC scraper module for syncing OVE inventory into the Virtual CarHub backend through REST APIs.

## What is implemented

- Environment-driven configuration
- Structured JSON logging for hourly sync attempts
- VCH REST API client with health checks and retry/backoff on transient failures
- CSV load, normalization, deduplication, mapping, and redaction
- Hourly sync orchestration with batch submission
- Pending deep-scrape polling loop
- Browser automation interface and Playwright CDP adapter for attaching to an existing logged-in browser session
- Rich listing snapshot capture for rebuilding the OVE listing layout on your own site
- Windows keep-awake protection while the scraper process is running
- PowerShell scripts to launch the dedicated Chrome profile and install Task Scheduler jobs
- Local override database for unresolved auction-location to ZIP mappings
- Tests for deduplication, redaction, batching, and required-field handling

## What still needs live environment validation

The scraper now includes a Playwright CDP adapter, but it still needs validation against your live authenticated OVE session and exact DOM selectors. You will likely need to tune the selector env vars in `.env` after the first real run.

- attaches to the existing Chrome or Edge debugging session
- exports only `East Hub Official` and `West Hub Official`
- searches VINs for deep-scrape requests
- extracts images, condition data, seller comments, icons, and a structured page snapshot
- never captures or forwards seller or bid data

## Install

```bash
python -m pip install -e .[dev]
```

## Run

```bash
ove-scraper sync-once
ove-scraper poll-once
ove-scraper scrape-vin 1HGCM82633A004352
ove-scraper run
```

## Keep The PC Awake

The scraper now calls the Windows `SetThreadExecutionState` API while it is running, which prevents system sleep during the long-lived `ove-scraper run` process.

## Install Windows Tasks

These scripts are included:

- `scripts/start_ove_browser.ps1`
- `scripts/start_ove_scraper.ps1`
- `scripts/install_ove_tasks.ps1`

Install the scheduled tasks with:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_ove_tasks.ps1
```

That creates two logon tasks:

- `OVE Browser Session`: launches the dedicated Chrome CDP profile
- `OVE Scraper`: starts the long-running scraper loop

This is the recommended production setup for the Windows machine because:

- the scraper process keeps Windows awake while active
- the browser and scraper restart automatically after reboot/logon
- the hourly sync and 30-second detail polling happen inside one persistent process instead of relying on many separate hourly task invocations

## Location ZIP Overrides

Pickup-location ZIP resolution now works in three layers:

1. explicit local overrides in `data/auction_location_overrides.json`
2. offline city/state lookup via `pgeocode`
3. unresolved-location reporting for manual enrichment

Generate a report of unresolved pickup locations with:

```powershell
& "C:\Users\joema\AppData\Local\Programs\Python\Python312\python.exe" .\scripts\report_unresolved_locations.py
```

That writes:

- `artifacts/unresolved_pickup_locations.csv`

Add new exceptions to:

- `data/auction_location_overrides.json`

# Two-Chrome Architecture Setup (Path 2)

## What this is

The OVE scraper has historically driven a single Chrome session, with three
workflows competing for it via a Windows mutex:

1. Saved-search sync (the "OVE database update" — hourly snapshot push)
2. Hot-deal pipeline (daily curated marketing list)
3. Deep-scrape worker (live VPS-driven condition-report requests)

Workflow #1 (sync) is the simplest, most-proven, and least-changed. Workflow #2
and #3 are the complex ones that received most of the recent fixes (CR-click
hash route, AutoCheck Snapshot capture, structural_damage parser, MMR
extraction). When all three share one browser, a long-running #2 or #3
operation can timeout the #1 sync — observed live on 2026-04-26.

Path 2 splits the workload across two Chromes:

- **Login A** (primary Chrome, port 9222) keeps hot-deal + deep-scrape on
  its battle-tested session.
- **Login B** (secondary Chrome, port 9223) runs only the sync.

Each Chrome has its own Windows mutex, so they never contend.

## Code-side enablement

Already shipped on the `two-chrome-arch` branch:

- New config field `chrome_debug_port_sync` (read from env
  `CHROME_DEBUG_PORT_SYNC`). When 0 (default), behavior is byte-identical
  to the historical single-Chrome mode. When > 0, `build_runtime` creates
  a second `PlaywrightCdpBrowserSession` pointed at that port and hands
  it to `HourlySyncRunner`.
- `lock_name_for_port()` in `automation_lock.py` derives a port-suffixed
  mutex name (`Local\OVE_Browser_Automation_9223`) so the sync and the
  primary Chrome's hot-deal/deep-scrape never collide.
- `run_browser_operation()` accepts an optional `lock_name` so the sync
  caller can pass the secondary mutex.

## One-time manual setup

### 1. Spin up second Chrome

Open PowerShell and start a second Chrome instance with its own profile
directory and debug port:

```powershell
$secondaryProfile = "$env:LOCALAPPDATA\OveScraperProfile2"
& "C:\Program Files\Google\Chrome\Application\chrome.exe" `
    --remote-debugging-port=9223 `
    --user-data-dir="$secondaryProfile" `
    --no-first-run `
    --no-default-browser-check
```

The new Chrome window opens with a blank profile. Verify CDP is listening:

```powershell
curl http://127.0.0.1:9223/json/version
```

### 2. Log in as Login B

In the new Chrome window:

1. Navigate to https://www.ove.com/
2. Sign in with the second OVE account credentials.
3. Complete the Manheim 2FA challenge if prompted.
4. **Check "trust this device"** when 2FA accepts the code, so the
   device-trust cookie is saved. (Per the project memory, clearing
   device trust triggers 2FA on every reconnect; we don't want that.)

### 3. Configure the sync saved searches on Login B

The sync runs the saved searches listed in `OVE_EAST_SEARCHES` and
`OVE_WEST_SEARCHES` in `.env`. Each one needs to exist on Login B's
account. Recreate them in the OVE UI under Login B — same names, same
filter criteria as Login A's:

- East Hub 2022-2023
- East Hub 2024
- East Hub 2025 or Newer
- West Hub 2015-2023
- West Hub 2024 or Newer

(Update this list if `.env`'s search names diverge.)

### 4. Enable the secondary port in `.env`

Add this line to `C:\Users\joema\Auction Module\.env`:

```
CHROME_DEBUG_PORT_SYNC=9223
```

### 5. Restart the scraper

```powershell
# Stop the current `python -m ove_scraper.main run` process (Ctrl-C in
# its terminal, or kill via Task Manager).
# Then relaunch — it'll pick up the new env var and create the
# two-browser runtime.
python -m ove_scraper.main run
```

On startup you should see this log line:

```
INFO Two-Chrome mode: sync runner -> port 9223 (Login B), hot-deal/deep-scrape -> port 9222 (Login A)
```

## What changed and what didn't

**Changed:** The sync now drives Login B's Chrome. The sync's mutex is
`Local\OVE_Browser_Automation_9223` instead of the default
`Local\OVE_Browser_Automation`. Hot-deal and deep-scrape still acquire
the default mutex on Login A's Chrome.

**Unchanged:** Sync code paths are byte-identical. Hot-deal and
deep-scrape see the exact same browser session they always have. All
recent fixes still apply.

## Rollback

To revert to single-Chrome mode, comment out `CHROME_DEBUG_PORT_SYNC` in
`.env` and restart. No code changes needed.

## Failure modes to watch for

1. **Login B falls out of device-trust → 2FA on every sync.** Fix:
   re-login manually with "trust this device" checked. Don't auto-retry
   login (memory: prior auto-retry locked an account).
2. **Login B's saved searches drift from Login A's.** The sync expects
   exact name matches. If a search is renamed/deleted on Login B, the
   sync will fail with "saved search not found". Re-create it.
3. **Second Chrome crashes / port 9223 stops responding.** The sync's
   `recover_browser_session` logic will try to restart it — but the
   launcher script today only knows about port 9222. You may need to
   manually relaunch the secondary Chrome (step 1 above).

## Future work

- Migrate the launcher (`scripts/start_ove_browser.ps1`) to be aware
  of the secondary port so secondary-Chrome auto-recovery works without
  manual intervention.
- Consider moving deep-scrape to Login B as a follow-up, once the sync
  has been stable on Login B for a week.

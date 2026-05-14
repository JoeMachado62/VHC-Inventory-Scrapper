# manual_login_b.ps1
#
# Launch Login B's Chrome (port 9223 / C:\Users\joema\AppData\Local\ove_sync
# profile) so you can manually log in to OVE / Manheim and refresh the
# saved credentials.
#
# Use this when:
#   - You've run scripts/emergency_stop_login_b.ps1 (or stop_all)
#   - Manheim has confirmed the account is unlocked
#   - You're ready to re-auth before restarting the scraper
#
# What this does:
#   1. Asks you to confirm Manheim has unlocked the account.
#   2. Wraps the existing scripts/start_ove_browser_sync.ps1 launcher,
#      which handles all the right Chrome flags (debug port, profile
#      dir, session-restore prevention, autosignin disabled).
#   3. Prints what to do in the Chrome window.
#
# Notes:
#   - If a Chrome on port 9223 is already running with the right profile,
#     start_ove_browser_sync.ps1 detects it and does nothing — just bring
#     the existing window to focus and use it.
#   - The scraper does NOT need to be stopped to use this — but if it IS
#     running, the keepalive will be hitting the same Chrome at the same
#     time as you. Best practice: stop first via emergency_stop_login_b.ps1.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' MANUAL LOGIN — LOGIN B (port 9223)' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host 'Profile: C:\Users\joema\AppData\Local\ove_sync'
Write-Host ''
Write-Host 'BEFORE PROCEEDING — confirm:' -ForegroundColor Yellow
Write-Host '  [ ] Manheim has unlocked Login B on their side'
Write-Host '  [ ] You have your Manheim password ready in case Chrome''s'
Write-Host '      saved credential prompt fails'
Write-Host '  [ ] You have run scripts/emergency_stop_login_b.ps1 if the'
Write-Host '      scraper is currently fighting with B''s Chrome'
Write-Host ''
$confirm = Read-Host 'Type READY (uppercase) to launch Chrome, anything else to abort'
if ($confirm -ne 'READY') {
    Write-Host 'Aborted. No Chrome launched.' -ForegroundColor Yellow
    exit 1
}

Write-Host ''
Write-Host 'Launching Chrome via scripts/start_ove_browser_sync.ps1 ...' -ForegroundColor Yellow
$browserScript = Join-Path $PSScriptRoot 'start_ove_browser_sync.ps1'
if (-not (Test-Path $browserScript)) {
    Write-Host ('  ERROR: launcher not found at {0}' -f $browserScript) -ForegroundColor Red
    exit 2
}
& $browserScript
Start-Sleep -Seconds 3

# Confirm Chrome came up.
$chrome = Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" |
    Where-Object { $_.CommandLine -like '*--remote-debugging-port=9223*' }
if ($chrome) {
    Write-Host ('  Chrome on port 9223 is running (PID {0}).' -f ($chrome | Select-Object -First 1).ProcessId) -ForegroundColor Green
} else {
    Write-Host '  WARNING: Chrome on port 9223 not detected after launch.' -ForegroundColor Red
    Write-Host '  Check c:\Users\joema\Auction Module\scripts\start_ove_browser_sync.ps1 manually.'
}

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' DO THIS IN THE CHROME WINDOW:' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host '  1. Navigate to:  https://www.ove.com/' -ForegroundColor Yellow
Write-Host ''
Write-Host '  2. If redirected to Manheim sign-in:'
Write-Host '       - Confirm the email/password fields auto-fill (Login B''s'
Write-Host '         credentials, NOT A''s — check the email is right)'
Write-Host '       - Click Sign In'
Write-Host '       - Complete any 2FA challenge'
Write-Host ''
Write-Host '  3. Verify OVE loads cleanly. Saved searches should be visible.'
Write-Host ''
Write-Host '  4. CLOSE THE CHROME WINDOW (or leave it — the scraper will'
Write-Host '     reattach to either state).'
Write-Host ''
Write-Host '  5. When done, restart the scraper via:'
Write-Host '       .\scripts\restart_scraper.ps1' -ForegroundColor Cyan
Write-Host ''

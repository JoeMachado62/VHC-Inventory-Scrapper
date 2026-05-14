# emergency_stop_login_a.ps1
#
# EMERGENCY STOP for Login A (port 9222 / primary Chrome).
#
# Use this when Manheim has locked Login A (or you suspect they're about to)
# and you need to stop all auto-recovery activity for A immediately.
#
# What this does:
#   1. Records a Manheim account-lock for port 9222 (12-hour cooldown by default)
#      so any future login click / browser recovery attempt on A is refused
#      at the disk-backed auth_lockout gate.
#   2. Kills the Chrome process running on port 9222 so its password manager
#      can't auto-fill the login form in the background.
#   3. Prints the current scraper status so you can verify.
#
# What this does NOT do:
#   - Does NOT kill the launcher PowerShell or Python process. The scraper
#     stays running. Login B continues normally. The startup gate will
#     refuse to spawn FRESH Python instances while A is locked, but the
#     currently-running Python keeps going.
#   - Does NOT touch Login B's Chrome or state.
#   - Does NOT clear any state. To resume, run scripts/restart_scraper.ps1
#     (which clears the lockout) AFTER you've manually re-authed A via
#     scripts/manual_login_a.ps1.
#
# Requires confirmation before acting. Safe to abort with Ctrl+C.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ' EMERGENCY STOP — LOGIN A (port 9222)' -ForegroundColor Red
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ''
Write-Host 'This will:'
Write-Host '  1. Set a Manheim account-lock for port 9222 (12-hour cooldown)'
Write-Host '  2. Kill Chrome.exe processes on port 9222'
Write-Host '  3. Show current scraper status'
Write-Host ''
Write-Host 'Login B will continue running. The scraper Python+launcher are NOT stopped.'
Write-Host ''
$confirm = Read-Host 'Type YES (uppercase) to proceed, anything else to abort'
if ($confirm -ne 'YES') {
    Write-Host 'Aborted. No changes made.' -ForegroundColor Yellow
    exit 1
}

Write-Host ''
Write-Host '[1/3] Recording lockout for port 9222...' -ForegroundColor Yellow
$pyOk = $false
try {
    & python -c "from pathlib import Path; from ove_scraper import auth_lockout; auth_lockout.record_manheim_account_locked(Path('artifacts'), port=9222, reason='operator emergency_stop_login_a.ps1')"
    if ($LASTEXITCODE -eq 0) { $pyOk = $true }
} catch {
    Write-Host ('  Lockout write FAILED: {0}' -f $_.Exception.Message) -ForegroundColor Red
}
if (-not $pyOk) {
    Write-Host '  Lockout write did not return cleanly. Check the above error and rerun.' -ForegroundColor Red
    exit 2
}
Write-Host '  Lockout recorded.' -ForegroundColor Green

Write-Host ''
Write-Host '[2/3] Killing Chrome processes on port 9222...' -ForegroundColor Yellow
$chromeProcs = Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" |
    Where-Object { $_.CommandLine -like '*--remote-debugging-port=9222*' }
if ($chromeProcs) {
    $count = 0
    foreach ($cp in $chromeProcs) {
        try {
            Stop-Process -Id $cp.ProcessId -Force -ErrorAction Stop
            $count++
        } catch {
            Write-Host ('  Failed to stop PID {0}: {1}' -f $cp.ProcessId, $_.Exception.Message) -ForegroundColor Red
        }
    }
    Write-Host ('  Killed {0} Chrome process(es) on port 9222.' -f $count) -ForegroundColor Green
} else {
    Write-Host '  No Chrome processes found on port 9222 (already stopped?).'
}

Write-Host ''
Write-Host '[3/3] Status snapshot...' -ForegroundColor Yellow
& "$PSScriptRoot\scraper_status.ps1"

Write-Host '=================================================================' -ForegroundColor Red
Write-Host ' DONE. Next steps:' -ForegroundColor Red
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ''
Write-Host '  1. Confirm with Manheim that Login A is unlocked on their side.'
Write-Host '     Until then, the 12-hour lockout cooldown is in effect.'
Write-Host ''
Write-Host '  2. When ready to re-auth, run:'
Write-Host '       .\scripts\manual_login_a.ps1' -ForegroundColor Cyan
Write-Host ''
Write-Host '  3. After confirming OVE loads cleanly in the manual Chrome,'
Write-Host '     restart the scraper with:'
Write-Host '       .\scripts\restart_scraper.ps1' -ForegroundColor Cyan
Write-Host ''

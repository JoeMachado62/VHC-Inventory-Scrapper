# emergency_stop_all.ps1
#
# NUCLEAR EMERGENCY STOP — stops EVERYTHING related to the OVE scraper.
#
# Use this when:
#   - You're not sure what's failing and want to halt all activity
#   - Both Logins A and B are misbehaving
#   - You suspect Manheim is about to lock both accounts
#   - You need to do major maintenance and want a clean slate
#
# What this does:
#   1. Sets the GLOBAL manual_unlock_required flag, which parks BOTH ports.
#      Even if Python is restarted by anything, the startup gate refuses to
#      proceed until you explicitly run `python -m ove_scraper.main unlock`.
#   2. Kills the entire scraper process tree:
#        - Launcher PowerShell (so it can't respawn Python)
#        - cmd.exe wrappers (the detached-spawn pattern)
#        - Python.exe (the actual scraper process)
#   3. Kills both Chrome instances (ports 9222 and 9223) so neither
#      browser's password manager can interact with Manheim in the
#      background.
#   4. Prints scraper status so you can verify everything is stopped.
#
# What this does NOT do:
#   - Does NOT delete cookies or any profile state.
#   - Does NOT touch any other Chrome windows you have open (only ones
#     started by the scraper's launchers, which use specific debug ports).
#   - Does NOT clear the lockout. Recovery requires running
#     scripts/restart_scraper.ps1 which prompts you to confirm before
#     clearing the global flag.
#
# Requires confirmation. Safe to abort with Ctrl+C.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ' EMERGENCY STOP — ALL (full halt)' -ForegroundColor Red
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ''
Write-Host 'This will:'
Write-Host '  1. Set GLOBAL manual_unlock_required (parks both Login A and B)'
Write-Host '  2. Kill scraper launcher + cmd wrappers + Python'
Write-Host '  3. Kill Chrome on BOTH ports 9222 and 9223'
Write-Host '  4. Show status snapshot'
Write-Host ''
Write-Host 'Nothing will run again until you explicitly restart via' -ForegroundColor Yellow
Write-Host '  .\scripts\restart_scraper.ps1' -ForegroundColor Cyan
Write-Host ''
$confirm = Read-Host 'Type STOP ALL (uppercase, with space) to proceed, anything else to abort'
if ($confirm -ne 'STOP ALL') {
    Write-Host 'Aborted. No changes made.' -ForegroundColor Yellow
    exit 1
}

Write-Host ''
Write-Host '[1/4] Setting GLOBAL manual_unlock_required...' -ForegroundColor Yellow
$globalPath = Join-Path $repoRoot 'artifacts\_state\auth_lockout_global.json'
try {
    $globalDir = Split-Path -Parent $globalPath
    if (-not (Test-Path $globalDir)) {
        New-Item -ItemType Directory -Force -Path $globalDir | Out-Null
    }
    $payload = '{' + "`r`n" + '  "manual_unlock_required": true' + "`r`n" + '}'
    [System.IO.File]::WriteAllText($globalPath, $payload, [System.Text.UTF8Encoding]::new($false))
    Write-Host ('  Wrote {0}' -f $globalPath) -ForegroundColor Green
} catch {
    Write-Host ('  Global lockout write FAILED: {0}' -f $_.Exception.Message) -ForegroundColor Red
    Write-Host '  Continuing with process kills anyway — the lockout file may be partially recoverable.'
}

Write-Host ''
Write-Host '[2/4] Killing scraper process tree...' -ForegroundColor Yellow
$scraperProcs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'python.exe' -and $_.CommandLine -like '*ove_scraper*')
}
if ($scraperProcs) {
    $count = 0
    foreach ($sp in $scraperProcs) {
        try {
            Stop-Process -Id $sp.ProcessId -Force -ErrorAction Stop
            $count++
        } catch {
            Write-Host ('  Failed to stop PID {0} ({1}): {2}' -f $sp.ProcessId, $sp.Name, $_.Exception.Message) -ForegroundColor Red
        }
    }
    Write-Host ('  Killed {0} scraper process(es).' -f $count) -ForegroundColor Green
} else {
    Write-Host '  No scraper processes found (already stopped?).'
}

# Wait briefly so Chrome processes spawned by the launcher are no longer
# being supervised before we kill them — avoids race where launcher tries
# to relaunch Chrome while we're killing it.
Start-Sleep -Seconds 2

Write-Host ''
Write-Host '[3/4] Killing Chrome on ports 9222 and 9223...' -ForegroundColor Yellow
$chromeProcs = Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" |
    Where-Object {
        $_.CommandLine -like '*--remote-debugging-port=9222*' -or
        $_.CommandLine -like '*--remote-debugging-port=9223*'
    }
if ($chromeProcs) {
    $count = 0
    foreach ($cp in $chromeProcs) {
        try {
            Stop-Process -Id $cp.ProcessId -Force -ErrorAction Stop
            $count++
        } catch {
            Write-Host ('  Failed to stop Chrome PID {0}: {1}' -f $cp.ProcessId, $_.Exception.Message) -ForegroundColor Red
        }
    }
    Write-Host ('  Killed {0} Chrome process(es).' -f $count) -ForegroundColor Green
} else {
    Write-Host '  No Chrome processes found on debug ports.'
}

Start-Sleep -Seconds 2

Write-Host ''
Write-Host '[4/4] Status snapshot...' -ForegroundColor Yellow
& "$PSScriptRoot\scraper_status.ps1"

Write-Host '=================================================================' -ForegroundColor Red
Write-Host ' DONE. Next steps:' -ForegroundColor Red
Write-Host '=================================================================' -ForegroundColor Red
Write-Host ''
Write-Host '  1. Confirm with Manheim that any locked account(s) are unlocked'
Write-Host '     on their side BEFORE attempting to re-auth.'
Write-Host ''
Write-Host '  2. Manually re-auth each affected account:'
Write-Host '       .\scripts\manual_login_a.ps1' -ForegroundColor Cyan
Write-Host '       .\scripts\manual_login_b.ps1' -ForegroundColor Cyan
Write-Host ''
Write-Host '  3. Restart the scraper:'
Write-Host '       .\scripts\restart_scraper.ps1' -ForegroundColor Cyan
Write-Host '     (this prompts before clearing the global lockout flag)'
Write-Host ''

# restart_scraper.ps1
#
# Restart the OVE scraper after an emergency stop / manual re-auth.
#
# Use this when:
#   - You ran scripts/emergency_stop_*.ps1
#   - You completed scripts/manual_login_*.ps1 for any locked account(s)
#   - You confirmed Manheim has unlocked the affected account(s) on
#     their side
#   - The scraper Python process is NOT running (verify in status snapshot)
#
# What this does:
#   1. Shows the current lockout state and asks you to review.
#   2. Asks for explicit confirmation before clearing lockouts.
#   3. Runs `python -m ove_scraper.main unlock` to clear all per-port
#      ledgers and the global manual-unlock flag.
#   4. Spawns the launcher (start_ove_scraper.ps1) DETACHED from this
#      terminal so you can close PowerShell and the scraper keeps
#      running.
#   5. Waits ~12 seconds and tails the launcher's stdout to verify
#      Python actually started.
#
# Notes:
#   - The detached spawn pattern (cmd /c start /MIN cmd /c powershell)
#     is required because Start-Process -WindowStyle Hidden has been
#     observed to silently exit on this machine. The double-cmd
#     approach truly orphans the process from the calling shell.
#   - If you want to verify in real time, in a separate PowerShell:
#       Get-Content logs\ove_scraper.log -Tail 30 -Wait

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Green
Write-Host ' RESTART SCRAPER' -ForegroundColor Green
Write-Host '=================================================================' -ForegroundColor Green
Write-Host ''

# Step 0: refuse if launcher is already running.
$mutex = New-Object System.Threading.Mutex($false, 'OVE_Scraper_Launcher')
$launcherAlreadyRunning = -not $mutex.WaitOne(0, $false)
if ($launcherAlreadyRunning) {
    $mutex.Dispose()
    Write-Host 'ERROR: launcher is already running (mutex OVE_Scraper_Launcher held).' -ForegroundColor Red
    Write-Host ''
    Write-Host 'Either:'
    Write-Host '  - The scraper is already up. Run .\scripts\scraper_status.ps1 to confirm.'
    Write-Host '  - A stale launcher is wedged. Run .\scripts\emergency_stop_all.ps1 first.'
    exit 1
}
$mutex.ReleaseMutex() | Out-Null
$mutex.Dispose()

Write-Host '--- Current lockout state (review before clearing) ---' -ForegroundColor Yellow
& python -m ove_scraper.main lockout-status
Write-Host ''

Write-Host '--- Pre-flight checklist ---' -ForegroundColor Yellow
Write-Host '  [ ] Manheim has unlocked any affected account(s) on their side'
Write-Host '  [ ] You completed manual_login_a.ps1 / manual_login_b.ps1 if needed'
Write-Host '  [ ] OVE loaded cleanly in the manual Chrome (saved searches visible)'
Write-Host '  [ ] You understand that clearing the lockout will allow the scraper'
Write-Host '      to attempt auth on both accounts'
Write-Host ''
$confirm = Read-Host 'Type RESTART (uppercase) to clear lockout AND start the scraper, anything else to abort'
if ($confirm -ne 'RESTART') {
    Write-Host 'Aborted. No changes made.' -ForegroundColor Yellow
    exit 1
}

Write-Host ''
Write-Host '[1/4] Clearing lockout state...' -ForegroundColor Yellow
try {
    & python -m ove_scraper.main unlock
    if ($LASTEXITCODE -ne 0) {
        throw ('unlock exited with code {0}' -f $LASTEXITCODE)
    }
    Write-Host '  Lockout cleared.' -ForegroundColor Green
} catch {
    Write-Host ('  ERROR clearing lockout: {0}' -f $_.Exception.Message) -ForegroundColor Red
    Write-Host '  Aborting restart. Investigate before retrying.'
    exit 2
}

Write-Host ''
Write-Host '[2/4] Verifying lockout is clear...' -ForegroundColor Yellow
& python -m ove_scraper.main lockout-status
Write-Host ''

Write-Host '[3/4] Spawning detached launcher...' -ForegroundColor Yellow
$tmpDir = Join-Path $repoRoot '.tmp'
if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
}
$detachLog = Join-Path $tmpDir 'detach_launcher.log'
# Truncate the detach log so the verification step below doesn't pick up
# stale entries from a previous run.
'' | Set-Content -Path $detachLog -Encoding UTF8
$detachCmd = Join-Path $tmpDir 'detach_launcher.cmd'
$cmdContent = @"
@echo off
cd /d "$repoRoot"
start "OVE Scraper Launcher" /MIN cmd /c "powershell -NoProfile -ExecutionPolicy Bypass -File scripts\start_ove_scraper.ps1 > .tmp\detach_launcher.log 2>&1"
exit /b 0
"@
Set-Content -Path $detachCmd -Value $cmdContent -Encoding ASCII
& cmd /c $detachCmd
Write-Host '  Launcher spawned (detached). Sleeping 12s before verification...' -ForegroundColor Green

Start-Sleep -Seconds 12

Write-Host ''
Write-Host '[4/4] Verifying launcher and Python are running...' -ForegroundColor Yellow

# Mutex check: a healthy launcher holds it.
$verifyMutex = New-Object System.Threading.Mutex($false, 'OVE_Scraper_Launcher')
$mutexHeld = -not $verifyMutex.WaitOne(0, $false)
if ($mutexHeld) {
    Write-Host '  Launcher mutex IS HELD (good — launcher is alive).' -ForegroundColor Green
} else {
    $verifyMutex.ReleaseMutex() | Out-Null
    Write-Host '  Launcher mutex is AVAILABLE (bad — launcher exited).' -ForegroundColor Red
}
$verifyMutex.Dispose()

# Process check: cmd / powershell / python all expected.
$procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'python.exe' -and $_.CommandLine -like '*ove_scraper*')
}
if ($procs) {
    Write-Host '  Scraper processes:' -ForegroundColor Green
    $procs | Select-Object ProcessId, Name | Format-Table -AutoSize
} else {
    Write-Host '  NO scraper processes found.' -ForegroundColor Red
}

# Tail the detached launcher log so we can see what happened.
Write-Host ''
Write-Host '  Last 10 lines of .tmp\detach_launcher.log:' -ForegroundColor Yellow
if (Test-Path $detachLog) {
    Get-Content $detachLog -Tail 10 | ForEach-Object { Write-Host ('    {0}' -f $_) }
} else {
    Write-Host '    (detach log not found — spawn may have failed)'
}

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Green
Write-Host ' DONE.' -ForegroundColor Green
Write-Host '=================================================================' -ForegroundColor Green
Write-Host ''
Write-Host 'To watch the scraper in real time, in a fresh PowerShell:'
Write-Host '  Get-Content logs\ove_scraper.log -Tail 30 -Wait' -ForegroundColor Cyan
Write-Host ''
Write-Host 'Look for KEEPALIVE_TICK lines within 5 minutes. Healthy ticks have:'
Write-Host '  outcome=ok  cards_render_ms=<3000-7000>  decay_signal=none'
Write-Host ''

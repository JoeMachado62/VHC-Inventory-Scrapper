$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$browserScript = Join-Path $PSScriptRoot "start_ove_browser.ps1"
$scraperScript = Join-Path $PSScriptRoot "start_ove_scraper.ps1"
$taskBrowser = "OVE Browser Session"
$taskScraper = "OVE Scraper"
$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
$startupBrowser = Join-Path $startupDir "OVE Browser Session.cmd"
$startupScraper = Join-Path $startupDir "OVE Scraper.cmd"

if (-not (Test-Path $browserScript)) {
    throw "Missing browser launcher script: $browserScript"
}
if (-not (Test-Path $scraperScript)) {
    throw "Missing scraper launcher script: $scraperScript"
}

$browserAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$browserScript`""
$scraperAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$scraperScript`""

# Two triggers per task: at logon AND at startup. The startup trigger
# means a reboot brings the daemon back even before a user logs in,
# closing the gap where a Windows Update reboot would otherwise leave
# the scraper offline until the next interactive logon.
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn
$startupTrigger = New-ScheduledTaskTrigger -AtStartup

# Per the 2026-04-08 VPS handoff:
#   - RestartCount 3 / RestartInterval 1 minute: a crashed daemon self-
#     resurrects within 2 minutes instead of waiting for a human.
#   - MultipleInstances IgnoreNew: prevents the launcher mutex collision
#     pattern we saw in the launcher log.
#   - ExecutionTimeLimit 0: this is a long-running daemon, never time it out.
#   - WakeToRun removed (handoff explicitly says no — we don't want to
#     block sleep).
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

try {
    Unregister-ScheduledTask -TaskName $taskBrowser -Confirm:$false -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskScraper -Confirm:$false -ErrorAction SilentlyContinue

    Register-ScheduledTask -TaskName $taskBrowser -Action $browserAction -Trigger @($logonTrigger, $startupTrigger) -Settings $settings -Description "Launch dedicated Chrome CDP profile for OVE scraping"
    Register-ScheduledTask -TaskName $taskScraper -Action $scraperAction -Trigger @($logonTrigger, $startupTrigger) -Settings $settings -Description "Run the long-lived OVE scraper and keep the machine awake"

    Write-Host "Installed scheduled tasks:"
    Write-Host " - $taskBrowser"
    Write-Host " - $taskScraper"
}
catch {
    Write-Warning "Task Scheduler registration failed. Falling back to Startup folder launchers."
    New-Item -ItemType Directory -Force $startupDir | Out-Null

    Set-Content -Path $startupBrowser -Encoding ASCII -Value "@echo off`r`npowershell -ExecutionPolicy Bypass -File `"$browserScript`"`r`n"
    Set-Content -Path $startupScraper -Encoding ASCII -Value "@echo off`r`npowershell -ExecutionPolicy Bypass -File `"$scraperScript`"`r`n"

    Write-Host "Installed Startup folder launchers:"
    Write-Host " - $startupBrowser"
    Write-Host " - $startupScraper"
}

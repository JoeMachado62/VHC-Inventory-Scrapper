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

$logonTrigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -StartWhenAvailable `
    -WakeToRun

try {
    Unregister-ScheduledTask -TaskName $taskBrowser -Confirm:$false -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskScraper -Confirm:$false -ErrorAction SilentlyContinue

    Register-ScheduledTask -TaskName $taskBrowser -Action $browserAction -Trigger $logonTrigger -Settings $settings -Description "Launch dedicated Chrome CDP profile for OVE scraping"
    Register-ScheduledTask -TaskName $taskScraper -Action $scraperAction -Trigger $logonTrigger -Settings $settings -Description "Run the long-lived OVE scraper and keep the machine awake"

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

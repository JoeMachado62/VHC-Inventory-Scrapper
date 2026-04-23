$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = "C:\Users\joema\AppData\Local\Programs\Python\Python312\python.exe"
$tempPath = Join-Path $repoRoot ".tmp"
$browserLauncher = Join-Path $PSScriptRoot "start_ove_browser.ps1"
$launcherLog = Join-Path $repoRoot "logs\\ove_scraper_launcher.log"
$mutexName = "OVE_Scraper_Launcher"
$mutex = $null
$hasHandle = $false

try {
    $mutex = New-Object System.Threading.Mutex($false, $mutexName)
    $hasHandle = $mutex.WaitOne(0, $false)
    if (-not $hasHandle) {
        New-Item -ItemType Directory -Force (Join-Path $repoRoot "logs") | Out-Null
        Add-Content -Path $launcherLog -Value (
            "{0} launcher already running; exiting duplicate instance" -f ([DateTime]::UtcNow.ToString("o"))
        )
        exit 0
    }

    if (-not (Test-Path $pythonPath)) {
        throw "Python not found at $pythonPath"
    }

    if (-not (Test-Path $browserLauncher)) {
        throw "Browser launcher not found at $browserLauncher"
    }

    New-Item -ItemType Directory -Force $tempPath | Out-Null
    New-Item -ItemType Directory -Force (Join-Path $repoRoot "logs") | Out-Null
    New-Item -ItemType Directory -Force (Join-Path $repoRoot "exports") | Out-Null
    New-Item -ItemType Directory -Force (Join-Path $repoRoot "artifacts") | Out-Null

    $env:TMP = $tempPath
    $env:TEMP = $tempPath

    Set-Location $repoRoot

    & $browserLauncher
    Start-Sleep -Seconds 3

    $consecutiveFailures = 0
    $maxBackoff = 300  # 5 minutes cap

    while ($true) {
        & $pythonPath -m ove_scraper.main run
        $exitCode = $LASTEXITCODE

        if ($exitCode -eq 0) {
            # Clean exit (e.g. signal shutdown) — reset backoff, restart quickly
            $consecutiveFailures = 0
            $waitSeconds = 10
        } else {
            $consecutiveFailures++
            # Exponential backoff: 10, 20, 40, 80, 160, 300 (cap)
            $waitSeconds = [Math]::Min(10 * [Math]::Pow(2, $consecutiveFailures - 1), $maxBackoff)
        }

        Add-Content -Path $launcherLog -Value (
            "{0} scraper exited code={1} failures={2}; restarting in {3}s" -f ([DateTime]::UtcNow.ToString("o")), $exitCode, $consecutiveFailures, $waitSeconds
        )
        Start-Sleep -Seconds $waitSeconds
    }
}
finally {
    if ($hasHandle -and $mutex -ne $null) {
        $mutex.ReleaseMutex() | Out-Null
    }
    if ($mutex -ne $null) {
        $mutex.Dispose()
    }
}

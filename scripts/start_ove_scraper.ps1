$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = "C:\Users\joema\AppData\Local\Programs\Python\Python312\python.exe"
$tempPath = Join-Path $repoRoot ".tmp"
$browserLauncher = Join-Path $PSScriptRoot "start_ove_browser.ps1"
# Path 2 / two-Chrome architecture (2026-04-27): the secondary
# launcher brings up Login B's Chrome on port 9223 for the sync
# workflow. It's invoked best-effort — if the script doesn't exist
# (older deployments) or fails to launch, the scraper still starts
# and falls back to single-Chrome mode (sync runs on Login A) since
# the sync code path checks chrome_debug_port_sync at runtime.
$browserSyncLauncher = Join-Path $PSScriptRoot "start_ove_browser_sync.ps1"
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

    # Best-effort secondary Chrome launch for the sync workflow. Failures
    # here (script missing, port already in use, Chrome refuses to start)
    # MUST NOT block the scraper — the run loop checks chrome_debug_port_sync
    # at runtime and silently falls back to single-Chrome mode if the
    # secondary port is unreachable.
    if (Test-Path $browserSyncLauncher) {
        try {
            & $browserSyncLauncher
            Start-Sleep -Seconds 3
        } catch {
            Add-Content -Path $launcherLog -Value (
                "{0} secondary browser launch failed: {1}" -f ([DateTime]::UtcNow.ToString("o")), $_.Exception.Message
            )
        }
    }

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

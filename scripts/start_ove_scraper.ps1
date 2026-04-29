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

    # Auth-lockout file (2026-04-28 hardening). Single source of truth
    # shared with Python at ove_scraper/auth_lockout.py. The file is
    # written by Python when account-lock or rate-limit-breach events
    # occur. The launcher reads it before each Python spawn and:
    #   - if `manual_unlock_required` is true, EXIT the launcher loop
    #     entirely (the operator must run `python -m ove_scraper.main unlock`)
    #   - if `manheim_locked_until_utc` or `rate_limit_until_utc` is in
    #     the future, sleep until then before respawning Python
    # This is the linchpin: without it the `while ($true)` loop below
    # would bypass every cross-process safeguard by simply restarting
    # Python and letting it consume another single-shot login click.
    $lockoutPath = Join-Path $repoRoot "artifacts\_state\auth_lockout.json"

    function Get-AuthLockoutWaitSeconds {
        if (-not (Test-Path $lockoutPath)) { return @{ Wait = 0; Reason = $null; Manual = $false } }
        try {
            $obj = Get-Content $lockoutPath -Raw | ConvertFrom-Json
        } catch {
            return @{ Wait = 0; Reason = "lockout-file-unreadable: $_"; Manual = $false }
        }
        if ($obj.manual_unlock_required) {
            return @{ Wait = -1; Reason = "manual_unlock_required"; Manual = $true }
        }
        $now = [DateTimeOffset]::UtcNow
        $maxWait = 0
        $reason = $null
        foreach ($field in @('manheim_locked_until_utc', 'rate_limit_until_utc')) {
            $value = $obj.$field
            if (-not $value) { continue }
            try {
                $until = [DateTimeOffset]::Parse($value)
            } catch { continue }
            if ($until -le $now) { continue }
            $delta = [int]([Math]::Ceiling(($until - $now).TotalSeconds))
            if ($delta -gt $maxWait) {
                $maxWait = $delta
                $reason = "$field=$value"
            }
        }
        return @{ Wait = $maxWait; Reason = $reason; Manual = $false }
    }

    # Exit code Python emits when it detects a lockout at startup. Must
    # match EXIT_CODE_AUTH_LOCKOUT_ACTIVE in ove_scraper/main.py. When
    # we see this code, we ALSO consult the lockout file to decide how
    # long to sleep. We do NOT count it as a "consecutive failure" for
    # the short-life circuit breaker, because the exit was deliberate.
    $exitCodeAuthLockoutActive = 99

    # Short-life circuit breaker (2026-04-28). If Python keeps exiting
    # very quickly (< $shortLifeThresholdSeconds), something is so wrong
    # that respawning is making it worse — the auth-failure park threshold
    # in main.py is supposed to catch this, but if anything bypasses it,
    # this is the last line of defense. After
    # $maxConsecutiveShortLifeFailures, the launcher exits and the
    # operator must manually restart it.
    $shortLifeThresholdSeconds = 30
    $maxConsecutiveShortLifeFailures = 5
    $consecutiveShortLifeFailures = 0

    $consecutiveFailures = 0
    $maxBackoff = 300  # 5 minutes cap

    while ($true) {
        # Pre-spawn lockout check.
        $lockout = Get-AuthLockoutWaitSeconds
        if ($lockout.Manual) {
            Add-Content -Path $launcherLog -Value (
                "{0} EXITING launcher: manual_unlock_required is set in auth_lockout.json. " +
                "Run: python -m ove_scraper.main unlock" -f ([DateTime]::UtcNow.ToString("o"))
            )
            break
        }
        if ($lockout.Wait -gt 0) {
            Add-Content -Path $launcherLog -Value (
                "{0} auth lockout active ({1}); sleeping {2}s before next Python spawn" `
                    -f ([DateTime]::UtcNow.ToString("o")), $lockout.Reason, $lockout.Wait
            )
            Start-Sleep -Seconds $lockout.Wait
            continue
        }

        $startTs = [DateTime]::UtcNow
        & $pythonPath -m ove_scraper.main run
        $exitCode = $LASTEXITCODE
        $lifetimeSeconds = ([DateTime]::UtcNow - $startTs).TotalSeconds

        # Special case: Python exited because of the lockout gate. Don't
        # treat as a failure for circuit-breaker purposes; just sleep
        # based on the lockout file.
        if ($exitCode -eq $exitCodeAuthLockoutActive) {
            Add-Content -Path $launcherLog -Value (
                "{0} Python exited with auth-lockout code ({1}); deferring to lockout file" `
                    -f ([DateTime]::UtcNow.ToString("o")), $exitCode
            )
            $consecutiveShortLifeFailures = 0
            $consecutiveFailures = 0
            Start-Sleep -Seconds 30
            continue
        }

        if ($exitCode -eq 0) {
            $consecutiveFailures = 0
            $consecutiveShortLifeFailures = 0
            $waitSeconds = 10
        } else {
            $consecutiveFailures++
            $waitSeconds = [Math]::Min(10 * [Math]::Pow(2, $consecutiveFailures - 1), $maxBackoff)
        }

        # Track short-life failures separately. A "short life" is any
        # exit (clean or crash) that happened in less than
        # $shortLifeThresholdSeconds. If we get a streak of these, the
        # scraper is in a tight crash-restart loop and the launcher
        # gives up.
        if ($lifetimeSeconds -lt $shortLifeThresholdSeconds) {
            $consecutiveShortLifeFailures++
        } else {
            $consecutiveShortLifeFailures = 0
        }

        Add-Content -Path $launcherLog -Value (
            "{0} scraper exited code={1} failures={2} life={3:N1}s short_life_streak={4}; restarting in {5}s" `
                -f ([DateTime]::UtcNow.ToString("o")), $exitCode, $consecutiveFailures, $lifetimeSeconds, $consecutiveShortLifeFailures, $waitSeconds
        )

        if ($consecutiveShortLifeFailures -ge $maxConsecutiveShortLifeFailures) {
            Add-Content -Path $launcherLog -Value (
                "{0} EXITING launcher: {1} consecutive short-life ( < {2}s ) failures. " +
                "Investigate logs/ove_scraper.log; restart launcher manually after fix." `
                    -f ([DateTime]::UtcNow.ToString("o")), $consecutiveShortLifeFailures, $shortLifeThresholdSeconds
            )
            break
        }

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

$ErrorActionPreference = "Stop"

# Secondary Chrome launcher for the Path 2 / two-Chrome architecture
# (2026-04-27). Runs alongside start_ove_browser.ps1 but on its own
# port + profile so the saved-search sync workflow has its own Chrome
# session — typically logged into a different OVE account ("Login B")
# — without contending for the primary Chrome that hosts hot-deal +
# deep-scrape work.
#
# Mirrors start_ove_browser.ps1 exactly except for $debugPort and
# $profilePath. Keeping the two scripts in lockstep means any
# fingerprinting / launch-flag fix made to the primary script should
# be ported here too.

$chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$profilePath = "C:\Users\joema\AppData\Local\ove_sync"
$debugPort = 9223
# 2026-04-28 hardening: launch Chrome with about:blank instead of the OVE
# URL — see start_ove_browser.ps1 for full rationale. Mirror exactly so
# Login B's Chrome behaves identically to Login A's at launch.
$oveUrl = "about:blank"

if (-not (Test-Path $chromePath)) {
    throw "Chrome not found at $chromePath"
}

New-Item -ItemType Directory -Force $profilePath | Out-Null

$existing = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -eq "chrome.exe" -and
        $_.CommandLine -like "*--remote-debugging-port=$debugPort*" -and
        $_.CommandLine -like "*$profilePath*"
    } |
    Select-Object -First 1

# If a Chrome process exists, verify the debug port is actually
# listening. Chrome can crash its debugging protocol while the main
# process stays alive; killing the zombie lets us relaunch cleanly.
if ($existing) {
    $portOpen = $false
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("127.0.0.1", $debugPort)
        $portOpen = $true
        $tcp.Close()
    } catch {
        $portOpen = $false
    }
    if (-not $portOpen) {
        Write-Host "Chrome process $($existing.ProcessId) found but port $debugPort is not listening. Killing stale process."
        Stop-Process -Id $existing.ProcessId -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        Get-CimInstance Win32_Process |
            Where-Object { $_.Name -eq "chrome.exe" -and $_.CommandLine -like "*$profilePath*" } |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        Start-Sleep -Seconds 1
        $existing = $null
    }
}

if (-not $existing) {
    # Patch Chrome's "didn't shut down correctly" state so the next
    # launch doesn't show the session-crashed bubble or restore-pages
    # prompt — both block CDP automation.
    $prefsPath = Join-Path $profilePath "Default\Preferences"
    if (Test-Path $prefsPath) {
        try {
            $prefsJson = Get-Content $prefsPath -Raw | ConvertFrom-Json
            if ($prefsJson.profile) {
                $prefsJson.profile.exit_type = "Normal"
                $prefsJson.profile.exited_cleanly = $true
            }
            # 2026-04-28 hardening: disable Chrome's auto-sign-in. Same
            # rationale and behavior as start_ove_browser.ps1.
            if (-not $prefsJson.credentials_enable_autosignin -or $prefsJson.credentials_enable_autosignin -ne $false) {
                $prefsJson | Add-Member -NotePropertyName credentials_enable_autosignin -NotePropertyValue $false -Force
            }
            $prefsJson | ConvertTo-Json -Depth 100 -Compress | Set-Content $prefsPath -Encoding UTF8
        } catch {
            Write-Host "Could not patch Chrome Preferences: $_"
        }
    }

    Start-Process -FilePath $chromePath -ArgumentList @(
        "--remote-debugging-port=$debugPort",
        "--user-data-dir=$profilePath",
        "--disable-session-crashed-bubble",
        "--hide-crash-restore-bubble",
        "--disable-features=InfiniteSessionRestore",
        "--restore-last-session=false",
        "--no-first-run",
        "--no-default-browser-check",
        $oveUrl
    )
}

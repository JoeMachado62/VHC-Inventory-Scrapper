$ErrorActionPreference = "Stop"

$chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$profilePath = "C:\chrome-cdp-profile"
$debugPort = 9222
# 2026-04-28 hardening: launch Chrome with about:blank instead of the OVE
# URL. Pre-fix, every Chrome relaunch loaded ove.com/buy#/ which redirects
# to Manheim auth — and Chrome's password manager auto-fill could submit
# credentials before Python even attached. Now Python explicitly navigates
# to OVE on its own schedule, gated by the disk-backed auth lockout.
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

# If a Chrome process exists, verify port 9222 is actually listening.
# Chrome can crash its debugging protocol while the main process stays
# alive (GPU crash, update, network stack reset). When this happens the
# scraper enters an unrecoverable CDP-drop loop and sends repeated
# auth-lost alerts. Killing the zombie process lets us relaunch cleanly.
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
        # Kill any child renderer processes with the same profile to
        # release file locks on the user-data-dir.
        Get-CimInstance Win32_Process |
            Where-Object { $_.Name -eq "chrome.exe" -and $_.CommandLine -like "*$profilePath*" } |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        Start-Sleep -Seconds 1
        $existing = $null
    }
}

if (-not $existing) {
    # Before launching, clean up the "Chrome didn't shut down correctly"
    # state from the Preferences file. Without this, every unclean kill
    # produces a session-crashed bubble + restore-pages prompt, which
    # blocks the scraper from finding the OVE page and triggers another
    # recovery cycle (infinite kill/relaunch loop).
    $prefsPath = Join-Path $profilePath "Default\Preferences"
    if (Test-Path $prefsPath) {
        try {
            $prefsJson = Get-Content $prefsPath -Raw | ConvertFrom-Json
            if ($prefsJson.profile) {
                $prefsJson.profile.exit_type = "Normal"
                $prefsJson.profile.exited_cleanly = $true
            }
            # 2026-04-28 hardening: disable Chrome's auto-sign-in feature
            # so the password manager fills the login form but does NOT
            # automatically submit it. Pre-fix, a stale-session redirect
            # to Manheim auth could cause Chrome to auto-submit before
            # Python attached, racing the disk-backed lockout. With
            # auto-sign-in disabled, the only path that submits is the
            # explicit Python click in _try_single_shot_login_click —
            # which IS gated by the lockout.
            if (-not $prefsJson.credentials_enable_autosignin -or $prefsJson.credentials_enable_autosignin -ne $false) {
                # Add or overwrite. ConvertFrom-Json returns a PSCustomObject;
                # use Add-Member to set the property idempotently.
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

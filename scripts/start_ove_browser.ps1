$ErrorActionPreference = "Stop"

$chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$profilePath = "C:\chrome-cdp-profile"
$debugPort = 9222
$oveUrl = "https://www.ove.com/buy#/"

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
                $prefsJson | ConvertTo-Json -Depth 100 -Compress | Set-Content $prefsPath -Encoding UTF8
            }
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

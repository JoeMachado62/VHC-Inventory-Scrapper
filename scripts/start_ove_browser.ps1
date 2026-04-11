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
    Start-Process -FilePath $chromePath -ArgumentList @(
        "--remote-debugging-port=$debugPort",
        "--user-data-dir=$profilePath",
        $oveUrl
    )
}

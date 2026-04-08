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

if (-not $existing) {
    Start-Process -FilePath $chromePath -ArgumentList @(
        "--remote-debugging-port=$debugPort",
        "--user-data-dir=$profilePath",
        $oveUrl
    )
}

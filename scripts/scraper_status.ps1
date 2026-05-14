# scraper_status.ps1
#
# Shows the CURRENT state of the OVE scraper:
#   - Per-port lockout state (global flag + each port)
#   - Running processes (launcher, Python, Chrome on each port)
#   - Last few KEEPALIVE_TICK lines from the scraper log
#
# Read-only. Safe to run anytime.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' OVE SCRAPER STATUS' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host '--- Lockout state ---' -ForegroundColor Yellow
try {
    & python -m ove_scraper.main lockout-status
} catch {
    Write-Host ('Failed to read lockout state: {0}' -f $_.Exception.Message) -ForegroundColor Red
}

Write-Host ''
Write-Host '--- Running scraper processes ---' -ForegroundColor Yellow
$procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*ove_scraper*') -or
    ($_.Name -eq 'python.exe' -and $_.CommandLine -like '*ove_scraper*')
}
if ($procs) {
    $procs | Select-Object ProcessId, Name, ParentProcessId, CreationDate | Format-Table -AutoSize
} else {
    Write-Host '  (no scraper processes running)'
}

Write-Host '--- Running Chrome instances on debug ports ---' -ForegroundColor Yellow
$chrome = Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" |
    Where-Object {
        $_.CommandLine -like '*--remote-debugging-port=9222*' -or
        $_.CommandLine -like '*--remote-debugging-port=9223*'
    }
if ($chrome) {
    $chrome | Select-Object ProcessId,
        @{N='Login';E={
            if ($_.CommandLine -like '*--remote-debugging-port=9222*') { 'A (9222)' }
            elseif ($_.CommandLine -like '*--remote-debugging-port=9223*') { 'B (9223)' }
            else { '?' }
        }},
        ParentProcessId | Format-Table -AutoSize
} else {
    Write-Host '  (no Chrome running on ports 9222 or 9223)'
}

Write-Host ''
Write-Host '--- Mutex (only one launcher allowed at a time) ---' -ForegroundColor Yellow
$mutex = New-Object System.Threading.Mutex($false, 'OVE_Scraper_Launcher')
try {
    if ($mutex.WaitOne(0, $false)) {
        Write-Host '  Launcher mutex AVAILABLE - no launcher is running'
        $mutex.ReleaseMutex() | Out-Null
    } else {
        Write-Host '  Launcher mutex HELD - a launcher process is running'
    }
} finally {
    $mutex.Dispose()
}

Write-Host ''
Write-Host '--- Last 5 KEEPALIVE_TICK lines from logs/ove_scraper.log ---' -ForegroundColor Yellow
$logPath = Join-Path $repoRoot 'logs\ove_scraper.log'
if (Test-Path $logPath) {
    # Tail then filter — much faster than Select-String over the whole file.
    $tail = Get-Content $logPath -Tail 2000 -ErrorAction SilentlyContinue
    $lines = $tail | Where-Object { $_ -like '*KEEPALIVE_TICK*' } | Select-Object -Last 5
    if ($lines) {
        $lines | ForEach-Object { Write-Host ('  {0}' -f $_) }
    } else {
        Write-Host '  (no KEEPALIVE_TICK lines in last 2000 log lines)'
    }
} else {
    Write-Host ('  (log file not found at {0})' -f $logPath)
}

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ''

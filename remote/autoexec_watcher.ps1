# Autoexec watcher for Sage 50 remote machine
# Watches for new *.job.txt files and executes commands.

$ErrorActionPreference = "Stop"

$autoexecDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec"
$exeDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\exe"
$watcherDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\watcher"
$logDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\log"
$outputDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\output"
$doneDir = "C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\done"
$logPath = Join-Path $logDir "autoexec.log"

if (-not (Test-Path $autoexecDir)) { New-Item -ItemType Directory -Force $autoexecDir | Out-Null }
if (-not (Test-Path $exeDir)) { New-Item -ItemType Directory -Force $exeDir | Out-Null }
if (-not (Test-Path $watcherDir)) { New-Item -ItemType Directory -Force $watcherDir | Out-Null }
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force $logDir | Out-Null }
if (-not (Test-Path $outputDir)) { New-Item -ItemType Directory -Force $outputDir | Out-Null }
if (-not (Test-Path $doneDir)) { New-Item -ItemType Directory -Force $doneDir | Out-Null }

function Write-Log($msg) {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logPath -Value "$stamp $msg"
}

function Run-Job($jobPath) {
    $baseName = Split-Path $jobPath -Leaf
    if ($baseName -match '^(executed_|failed_|processing_)') {
        return
    }
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $processing = Join-Path $autoexecDir ("processing_{0}_{1}" -f $stamp, $baseName)
    try {
        Move-Item -Force $jobPath $processing
    } catch {
        Write-Log "WARN could not move $jobPath to processing: $($_.Exception.Message)"
        return
    }

    $lines = Get-Content -Path $processing | Where-Object { $_ -and $_.Trim().Length -gt 0 }
    if ($lines.Count -lt 1) {
        Write-Log "ERROR empty job file: $processing"
        $failed = Join-Path $doneDir ("failed_{0}_{1}" -f $stamp, $baseName)
        Move-Item -Force $processing $failed
        return
    }

    $exeName = $lines[0].Trim()
    $args = @()
    if ($lines.Count -gt 1) { $args = $lines[1..($lines.Count-1)] }

    $exePath = Join-Path $exeDir $exeName
    if (-not (Test-Path $exePath)) {
        Write-Log "ERROR exe not found: $exePath (job $processing)"
        $failed = Join-Path $doneDir ("failed_{0}_{1}" -f $stamp, $baseName)
        Move-Item -Force $processing $failed
        return
    }

    if (-not ($args -contains "--out-dir")) {
        $args = $args + @("--out-dir", $outputDir)
    }
    $safeBase = $baseName -replace '[^A-Za-z0-9_.-]+', '_'
    $stdoutPath = Join-Path $logDir ("job_{0}_{1}.out.txt" -f $stamp, $safeBase)
    $stderrPath = Join-Path $logDir ("job_{0}_{1}.err.txt" -f $stamp, $safeBase)
    Write-Log "RUN $exePath $($args -join ' ')"
    try {
        $p = Start-Process -FilePath $exePath -ArgumentList $args -Wait -PassThru `
            -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
        $code = $p.ExitCode
    } catch {
        Write-Log "ERROR execution failed: $($_.Exception.Message)"
        $code = 999
    }

    if ($code -eq 0) {
        $done = Join-Path $doneDir ("executed_{0}_{1}" -f $stamp, $baseName)
        Move-Item -Force $processing $done
        Write-Log "OK exit=$code job=$done"
    } else {
        $failed = Join-Path $doneDir ("failed_{0}_{1}" -f $stamp, $baseName)
        Move-Item -Force $processing $failed
        Write-Log "FAIL exit=$code job=$failed"
    }
}

# Do NOT process existing jobs on startup; only handle newly created files.
Write-Log "Startup: existing job files will be ignored (newly created files only)"

$fsw = New-Object System.IO.FileSystemWatcher
$fsw.Path = $autoexecDir
$fsw.Filter = "*.job.txt"
$fsw.EnableRaisingEvents = $true
$fsw.IncludeSubdirectories = $false

$action = {
    Start-Sleep -Milliseconds 200
    Run-Job $Event.SourceEventArgs.FullPath
}

Register-ObjectEvent $fsw Created -Action $action | Out-Null

Write-Log "Watcher started in $autoexecDir"
Write-Host "Watcher running. Press Ctrl+C to stop."

while ($true) { Start-Sleep -Seconds 1 }

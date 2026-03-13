# Autoexec: Sage 50 remote runner

This watcher runs on the remote Sage machine and executes jobs dropped into:

C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec

## How jobs work
Create a text file ending in `.job.txt`.
- Line 1: exe name (must exist in `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec\exe`)
- Line 2+: arguments (one per line)

Example job file contents:

odbc_schema_probe.exe
--password
S@g31879

When detected, the watcher:
- renames it to `processing_*`
- executes the command
- renames to `executed_*` or `failed_*`
- logs to `autoexec.log` (stored in `C:\Users\soadmin\Dropbox\ENZO-Sage50\scripts`)
- forces output to `C:\Users\soadmin\Dropbox\ENZO-Sage50\autoexec_output`

## Run the watcher
From PowerShell (remote machine):

```powershell
powershell -ExecutionPolicy Bypass -File C:\Users\soadmin\Dropbox\ENZO-Sage50\scripts\autoexec_watcher.ps1
```

## Auto-start on login (Task Scheduler)
Create a new task:
- Trigger: At log on
- Action: Start a program
- Program/script: `powershell`
- Add arguments:
  `-ExecutionPolicy Bypass -File C:\Users\soadmin\Dropbox\ENZO-Sage50\scripts\autoexec_watcher.ps1`
- Run whether user is logged on or not (optional)

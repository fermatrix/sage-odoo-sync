# Clean autoexec log/done to make runs deterministic
Remove-Item -Force "ENZO-Sage50\autoexec\log\*" -ErrorAction SilentlyContinue
Remove-Item -Force "ENZO-Sage50\autoexec\done\*" -ErrorAction SilentlyContinue

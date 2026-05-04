# Sage 50 ODBC probe (remote)

## CAUTION

All Sage-generated CSV files **must use semicolon (`;`)** as delimiter.

- Required for all pipelines in this project (`_master_sage`, monthly `13_2026`, API sync inputs).
- Do **not** generate Sage files with comma delimiter.
- For `odbc_query_runner.py`, keep default delimiter (`;`) and do not pass `--delimiter ,`.

This folder contains a minimal ODBC connectivity probe for Sage 50.
It lists available tables and writes them to a CSV in the Dropbox folder.

## Build (on a 32-bit Python environment)

1. Install 32-bit Python on the build machine.
2. Run:

```powershell
.\build_exe.ps1
```

This produces `dist\odbc_probe.exe`.

## Run on the remote Sage machine

Copy `dist\odbc_probe.exe` to:

`C:\Users\soadmin\Dropbox\ENZO-Sage50\_tests\odbc_probe.exe`

Then run from a command prompt:

```bat
odbc_probe.exe --password "S@g31879"
```

Output CSV will be created in:

`C:\Users\soadmin\Dropbox\ENZO-Sage50\_tests\`

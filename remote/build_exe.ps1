# Build 32-bit executable for Sage 50 ODBC probe
# Requires 32-bit Python and pyodbc + pyinstaller installed in that environment.

$ErrorActionPreference = "Stop"

python -m pip install --upgrade pip
python -m pip install pyodbc pyinstaller

# Clean build artifacts
if (Test-Path .\dist) { Remove-Item -Recurse -Force .\dist }
if (Test-Path .\build) { Remove-Item -Recurse -Force .\build }

python -m PyInstaller --onefile --name odbc_probe .\odbc_probe.py

Write-Host "Built: dist\\odbc_probe.exe"
# Exit on errors
$ErrorActionPreference = "Stop"

# Install PyInstaller if missing
if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    pip install pyinstaller | Out-Null
}

# Clean previous build artifacts
if (Test-Path dist) { Remove-Item dist -Recurse -Force }
if (Test-Path build) { Remove-Item build -Recurse -Force }

# Create executable
pyinstaller --noconfirm --onefile `
    --add-data "templates;templates" `
    --add-data "static;static" `
    --collect-submodules reportlab.graphics.barcode `
    gui.py

Write-Host "Executable created at dist/app.exe"

# Exit on errors
$ErrorActionPreference = "Stop"

# Install PyInstaller if missing
if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    pip install pyinstaller | Out-Null
}

# Clean previous build artifacts
if (Test-Path dist) { Remove-Item dist -Recurse -Force }
if (Test-Path build) { Remove-Item build -Recurse -Force }

# Build server executable
pyinstaller --noconfirm --onefile --name ultrasuite-server `
    --add-data "templates;templates" `
    --add-data "static;static" `
    --collect-submodules reportlab.graphics.barcode `
    app.py

# Build GUI executable
pyinstaller --noconfirm --onefile --name ultrasuite-gui `
    --add-data "templates;templates" `
    --add-data "static;static" `
    --collect-submodules reportlab.graphics.barcode `
    gui.py

Write-Host "Executables created: dist/ultrasuite-server.exe and dist/ultrasuite-gui.exe"

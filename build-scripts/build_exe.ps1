# Exit on errors
$ErrorActionPreference = "Stop"

# Ensure pywebview is available so the GUI executable includes it
pip show pywebview > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    pip install pywebview | Out-Null
}

# Clean previous build artifacts
if (Test-Path dist) { Remove-Item dist -Recurse -Force }
if (Test-Path build) { Remove-Item build -Recurse -Force }

# Build server executable
pyinstaller --noconfirm --onefile --name ultrasuite-server `
    --add-data "templates:templates" `
    --add-data "static:static" `
    --collect-submodules reportlab.graphics.barcode `
    app.py

# Build GUI executable
pyinstaller --noconfirm --onefile --name ultrasuite-gui `
    --add-data "templates:templates" `
    --add-data "static:static" `
    --collect-submodules reportlab.graphics.barcode `
    --noconsole --windowed `
    gui.py

Write-Host "Executables created: dist/ultrasuite-server.exe and dist/ultrasuite-gui.exe"

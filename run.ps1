# Exit on errors
$ErrorActionPreference = "Stop"

# Kill any previous app.py process
Get-CimInstance Win32_Process | Where-Object {
    $_.Name -like "python*" -and $_.CommandLine -match "app.py"
} | ForEach-Object {
    Write-Host "Killing previous process ID $($_.ProcessId) running app.py"
    Stop-Process -Id $_.ProcessId -Force
}

# Delete finance.db if it exists
if (Test-Path .\finance.db) {
    Remove-Item .\finance.db
    Write-Host "Deleted finance.db"
}

# Pull latest code
git pull origin main

# Open browser
Start-Process "firefox.exe" "http://localhost:5000"

# Run Python app in the foreground
Write-Host "`nStarting app.py... Press Ctrl+C to stop."
python .\app.py


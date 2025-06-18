# ultrasuite

<p align="center">
  <img src="./ultrasuite-logo.png" alt="ultrasuite logo" width="200">
</p>

Ultrasuite is a Flask application for aggregating Shopify and QuickBooks Online sales data. Upload transaction files, manage SKU aliases, and view comprehensive monthly or yearly reports.

## Features

- **Data Upload** – Import Shopify CSV files and QuickBooks Excel exports.
- **Dashboard** – View last upload dates and a summary of SKU aliases.
- **SKU Mapping** – Map alias SKUs to canonical SKUs and categorize them by type.
- **Reports** – Interactive charts for monthly trends and yearly comparisons, with optional PDF export.
- **Settings** – Choose a logo, color theme, and default report sections.

Data is stored locally in `finance.db` and charts are generated with Matplotlib.

## Getting Started

1. Install Python 3 along with the dependencies:
   ```bash
   pip install Flask pandas matplotlib openpyxl xhtml2pdf
   ```
2. Start the application:
   ```bash
   python app.py
   ```
3. Open `http://localhost:5000` in your browser.

All data stays on your machine; no external services are required.

## Project Layout

- `app.py` – main Flask application.
- `templates/` – Jinja2 templates for all pages.

## Programmatic Reports

Helper functions in `app.py` expose the underlying report data for integration into other Python code:

- `get_year_overall(year)` – monthly totals for the given year.
- `get_year_summary(year)` – totals by sales type for the year.
- `get_last_month_summary(year, month=None)` – last full month totals by type.
- `get_last_month_details(year, month=None)` – detailed SKU breakdown for the last full month.
- `get_shopify_monthly()` – Shopify income by month across all years.
- `get_shopify_quarterly()` – Shopify income by quarter across all years.

### Programmatic PDF Export

Call `/export-report` with query parameters to download a PDF:

```
/export-report?year=2025&month=4&include_month_summary=1&include_year_overall=1
```

Omit `month` to use the last full month. Options accept `1`/`true` or can be left out to disable a section.

## Packaging an EXE Installer

1. Ensure `PyInstaller` is installed:
   ```bash
   pip install pyinstaller
   ```
2. Run the provided script to build `dist/app.exe`:
   ```powershell
   ./build_exe.ps1
   ```
3. (Optional) Generate a Windows installer with [Inno Setup](https://jrsoftware.org/isinfo.php):
   ```powershell
   iscc installer.iss
   ```



## Automated Builds

GitHub Actions runs when a tag starting with v is pushed. It executes `build_exe.ps1` and `iscc` to upload the executable and installer as artifacts.

## License

This project is provided as-is for demonstration purposes.

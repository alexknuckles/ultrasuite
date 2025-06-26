# ultrasuite

<p align="center">
  <img src="static/ultrasuite-logo.png" alt="ultrasuite logo" width="200">
</p>

Ultrasuite is a Flask application for aggregating sales data. Upload transaction files, manage SKU aliases, and view comprehensive monthly or yearly reports.

## Features

- **Data Upload** – Import CSV files and Excel exports.
- **Dashboard** – View last upload dates and a summary of mapped SKUs.
- **SKU Mapping** – Map alias SKUs to canonical SKUs and categorize them by type.
- **Reports** – Interactive charts for monthly trends and yearly comparisons, with optional PDF export.
- **Settings** – Customize reports and appearance.
- **Report Options** – Choose the default tab and how many recent years to display.
- **Shopify Sync** – Connect to the Shopify API to download transactions.
- **Raw Orders** – Full Shopify order data is stored for future use.

Data is stored locally and charts are generated with Matplotlib.

Download the latest release from the [releases page](https://github.com/alexknuckles/ultrasuite/releases).

## From Source

1. Clone this repository or download the source archive.
2. Install Python 3 along with the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the application:
   ```bash
   python app.py  # runs the Flask server
   # or
   python gui.py  # opens the app in a pywebview window
   ```
4. Open `http://localhost:5000` in your browser if you prefer using a web browser.

All data stays on your machine; no external services are required.

## QuickBooks Online

When entering your QuickBooks credentials under **Settings → Sync**, be sure to
select the correct environment. Choose **Production** to connect with your live
company data or **Sandbox** when testing against Intuit's sandbox API.

## Project Layout

- `app.py` – main Flask application.
- `gui.py` – launches the app in a pywebview window.
- `templates/` – Jinja2 templates for all pages.
- `build-scripts/` – PowerShell and Inno Setup scripts for Windows builds.

## Programmatic Reports

Helper functions in `app.py` expose the underlying report data for integration into other Python code:

- `get_year_overall(year)` – monthly totals for the given year.
- `get_year_summary(year)` – totals by sales type for the year.
- `get_last_month_summary(year, month=None)` – last full month totals by type.
- `get_last_month_details(year, month=None)` – detailed SKU breakdown for the last full month.
- `get_shopify_monthly()` – Shopify income by month across recent years.
- `get_shopify_quarterly()` – Shopify income by quarter across recent years.
- `get_traffic_matrix()` – HubSpot website traffic metrics with each selected year shown side by side.
  Source names from HubSpot are normalized so variations like `PAID_SOCIAL` or
  `ORGANIC_SOCIAL` map to the standard categories.

### Programmatic PDF Export

Call `/export-report` with query parameters to download a PDF:

```
/export-report?year=2025&month=4
```

If a parameter is omitted the value saved in **Settings → Reports** will be used.
Options such as `include_month_summary` and `include_year_overall` override those
defaults when present.

### Traffic Matrix API

Access aggregated website traffic data at `/traffic-matrix`.

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

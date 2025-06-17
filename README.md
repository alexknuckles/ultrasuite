# ultrasuite

<p align="center">
  <img src="./ultrasuite-logo.png" alt="ultrasuite logo" width="200">
</p>

This repository contains a Flask web application for aggregating and analyzing sales data from Shopify and QuickBooks Online. The app lets you upload transaction files, manage SKU aliases, and view monthly sales reports.

## Features

- **Data Upload** – Import Shopify CSV files and QuickBooks Excel exports.
- **Dashboard** – See the last upload time for each data source and a summary of SKU aliases.
- **SKU Mapping** – Map multiple alias SKUs to a canonical SKU and categorize them by type.
- **Reports** – View monthly trends, last month details, and yearly comparisons with interactive charts. Reports can be exported as PDF files.
- **Debug View** – Simple view for debugging transaction aggregates.
- **Settings Page** – Configure branding text, upload a logo, and choose which PDF sections to include by default.

Uploaded data is stored in a local SQLite database (`finance.db`), and charts are rendered using Matplotlib.

## Getting Started

1. Install Python 3 and the required packages:
   ```bash
   pip install Flask pandas matplotlib openpyxl xhtml2pdf
   ```
   The application uses **xhtml2pdf** to generate PDF reports. This library is
   pure Python and does not require additional system packages, so installing it
   with `pip` is usually sufficient on all platforms.
2. Run the application:
   ```bash
   python app.py
   ```
3. Open your browser to `http://localhost:5000`.

Data uploads and database files are kept locally; no external services are required.

## Customization

The light theme uses ocean colors, and a Codex-style dark mode can be toggled
from the navigation bar. You can customize the primary and highlight colors by editing the
CSS variables (`--ultra-primary`, `--ultra-highlight`).

## Project Layout

- `app.py` – main Flask application.
- `templates/` – Jinja2 templates for all pages.

## Programmatic Reports

Several helper functions in `app.py` return the underlying report data so it can
be used from other Python code:

- `get_year_overall(year)` – monthly totals for the given year.
- `get_year_summary(year)` – totals by sales type for the year.
- `get_last_month_summary(year, month=None)` – last full month totals by type.
- `get_last_month_details(year, month=None)` – detailed SKU breakdown for the
  last full month.

### Programmatic PDF Export

Pass query parameters to `/export-report` to download a PDF without using the
web form. Parameters match the form fields:

```
/export-report?year=2025&month=4&include_month_summary=1&include_year_overall=1
```

Omit `month` to use the last full month. Options accept `1`/`true` or can be
left out to disable a section.

## License

This project is provided as-is for demonstration purposes.

# ultrasuite

![ultrasuite logo](./ultrasuite-logo.png)

This repository contains a Flask web application for aggregating and analyzing sales data from Shopify and QuickBooks Online. The app lets you upload transaction files, manage SKU aliases, and view monthly sales reports.

## Features

- **Data Upload** – Import Shopify CSV files and QuickBooks Excel exports.
- **Dashboard** – See the last upload time for each data source and a summary of SKU aliases.
- **SKU Mapping** – Map multiple alias SKUs to a canonical SKU and categorize them by type.
- **Reports** – View monthly trends, last month details, and yearly comparisons with interactive charts.
- **Debug View** – Simple view for debugging transaction aggregates.

Uploaded data is stored in a local SQLite database (`finance.db`), and charts are rendered using Matplotlib.

## Getting Started

1. Install Python 3 and the required packages:
   ```bash
   pip install Flask pandas matplotlib
   ```
2. Run the application:
   ```bash
   python app.py
   ```
3. Open your browser to `http://localhost:5000`.

Data uploads and database files are kept locally; no external services are required.

## Project Layout

- `app.py` – main Flask application.
- `templates/` – Jinja2 templates for all pages.

## License

This project is provided as-is for demonstration purposes.

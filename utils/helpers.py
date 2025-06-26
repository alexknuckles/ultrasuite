# Helper functions extracted from app.py for reusability
import os
import time
import requests
from datetime import datetime
from io import BytesIO
import json
import pandas as pd
from markupsafe import Markup

from database import add_api_response, add_log, get_setting

DEFAULT_THEME_PRIMARY = "#1976d2"
DEFAULT_THEME_HIGHLIGHT = "#bbdefb"
DEFAULT_THEME_BACKGROUND = "#f8f9fa"
DEFAULT_THEME_TEXT = "#363636"


def _hex_to_rgb(value: str):
    """Return (r, g, b) for a hex color string."""
    value = value.lstrip("#")
    if len(value) == 3:
        value = "".join(c * 2 for c in value)
    try:
        r, g, b = [int(value[i : i + 2], 16) for i in (0, 2, 4)]
    except Exception:
        return 0, 0, 0
    return r, g, b


def _is_dark_color(value: str) -> bool:
    """Return True if the color is dark based on luminance."""
    r, g, b = _hex_to_rgb(value)
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return luminance < 128


def _chart_style():
    bg = get_setting("theme_background", DEFAULT_THEME_BACKGROUND)
    return "dark_background" if _is_dark_color(bg) else "default"


def _try_read_csv(data: bytes, encodings=None) -> pd.DataFrame:
    """Try reading CSV bytes with a series of encodings."""
    encodings = encodings or ["utf-8", "utf-8-sig", "utf-16", "latin-1", "cp1252"]
    for enc in encodings:
        try:
            return pd.read_csv(BytesIO(data), encoding=enc)
        except Exception:
            continue
    raise ValueError("Unsupported CSV encoding")


def _parse_shopify(file_storage) -> pd.DataFrame:
    data = file_storage.read()
    file_storage.seek(0)
    try:
        df = _try_read_csv(data)
    except Exception:
        try:
            df = pd.read_excel(BytesIO(data))
        except Exception as exc:
            raise ValueError("Could not parse Shopify file") from exc
    required = {"Created at", "Lineitem sku"}
    if not required.issubset(set(df.columns)):
        raise ValueError("Invalid Shopify data")
    cleaned = df[
        [
            "Created at",
            "Lineitem sku",
            "Lineitem name",
            "Lineitem quantity",
            "Lineitem price",
        ]
    ].copy()
    cleaned["Total"] = pd.to_numeric(df["Lineitem price"], errors="coerce").fillna(
        0
    ) * pd.to_numeric(df["Lineitem quantity"], errors="coerce").fillna(0)
    cleaned.columns = ["created_at", "sku", "description", "quantity", "price", "total"]
    return cleaned


def _parse_qbo(file_storage) -> pd.DataFrame:
    data = file_storage.read()
    file_storage.seek(0)
    try:
        df = pd.read_excel(BytesIO(data), skiprows=4)
    except Exception:
        try:
            df = _try_read_csv(data)
        except Exception as exc:
            raise ValueError("Could not parse QuickBooks file") from exc
    expected = {"transaction_date", "product_service"}
    if set(df.columns[:2]).intersection(expected) != expected:
        try:
            df.columns = [
                "deleted_code",
                "transaction_date",
                "transaction_type",
                "transaction_number",
                "customer_name",
                "line_description",
                "quantity",
                "sales_price",
                "amount",
                "balance",
                "product_service",
            ]
        except ValueError as exc:
            raise ValueError("Invalid QuickBooks data") from exc
    df = df[df.get("transaction_date").notna()]
    sku_col = next(
        (c for c in df.columns if c.lower().replace(" ", "_") == "sku"), None
    )
    if sku_col:
        df["sku"] = df[sku_col]
    else:
        df["sku"] = pd.NA
    cleaned = df[
        [
            "transaction_date",
            "sku",
            "line_description",
            "quantity",
            "sales_price",
            "amount",
        ]
    ].copy()
    cleaned.columns = ["created_at", "sku", "description", "quantity", "price", "total"]
    return cleaned


def _normalize_hubspot_source(src):
    """Return standardized source name or ``None`` if unknown."""
    HUBSPOT_SOURCE_MAP = {
        "EMAIL_MARKETING": "Email Marketing",
        "EMAIL": "Email Marketing",
        "ORGANIC_SEARCH": "Organic Search",
        "ORGANIC": "Organic Search",
        "SOCIAL_MEDIA": "Social Media",
        "PAID_SOCIAL": "Social Media",
        "SOCIAL_MEDIA_PAID": "Social Media",
        "SOCIAL_MEDIA_ORGANIC": "Social Media",
        "ORGANIC_SOCIAL": "Social Media",
        "REFERRALS": "Referrals",
        "REFERRAL": "Referrals",
        "DIRECT_TRAFFIC": "Direct Traffic",
        "DIRECT": "Direct Traffic",
        "OTHER_CAMPAIGNS": "Other Campaigns",
        "OTHER_CAMPAIGN": "Other Campaigns",
        "OTHER": "Other Campaigns",
        "PAID_SEARCH": "Paid Search",
        "PAID_SEARCHES": "Paid Search",
    }
    if not isinstance(src, str):
        return None
    key = src.strip().replace(" ", "_").upper()
    return HUBSPOT_SOURCE_MAP.get(key)


def fetch_hubspot_traffic_data(
    token,
    start_year: int = 2021,
    end_year: int = 2025,
    *,
    retries: int = 3,
    cache_dir: str | None = None,
):
    """Return monthly HubSpot traffic analytics for a date range."""
    url = "https://api.hubapi.com/analytics/v2/reports/sources/monthly"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start": f"{start_year:04d}0101",
        "end": f"{end_year:04d}1231",
    }

    offset = None
    rows = []
    page = 0
    while True:
        if offset is not None:
            params["offset"] = offset
        attempt = 0
        while True:
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=15)
                add_api_response("fetch_hubspot", resp.status_code, resp.text[:2000])
                if resp.status_code == 429 and attempt < retries - 1:
                    time.sleep(2**attempt)
                    attempt += 1
                    continue
                resp.raise_for_status()
            except Exception:
                if attempt >= retries - 1:
                    raise
                time.sleep(2**attempt)
                attempt += 1
                continue
            break

        payload = resp.json()
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            with open(os.path.join(cache_dir, f"hubspot_{page:03d}.json"), "w") as f:
                json.dump(payload, f)
        for row in payload.get("data", []):
            source = _normalize_hubspot_source(row.get("source"))
            if not source:
                continue
            year = int(row.get("year"))
            month = int(row.get("month"))
            rows.append(
                {
                    "year": year,
                    "month": datetime(year, month, 1).strftime("%b"),
                    "source": source,
                    "sessions": int(row.get("sessions")),
                    "bounce_rate": float(row.get("bounce_rate")),
                    "avg_time_min": float(row.get("avg_time")) / 60,
                }
            )
        offset = payload.get("offset")
        page += 1
        if not offset:
            break
    return pd.DataFrame(rows)


def format_dt(value):
    """Format ISO timestamp into 'mm/dd/yy - h:mma/pm'."""
    try:
        dt = datetime.fromisoformat(value)
        time_str = dt.strftime("%I:%M%p").lstrip("0").lower()
        date_str = dt.strftime("%m/%d/%y")
        return f"{date_str} - {time_str}"
    except Exception:
        return value


def trend(value, compare=None):
    """Return HTML with colored arrow indicating trend."""
    try:
        if value in ("-", None):
            return value
        sign_source = compare if compare is not None else value
        sign_str = str(sign_source).strip()
        if sign_str == "∞":
            arrow = "▲"
            color = "has-text-success"
        else:
            cleaned = (
                sign_str.replace("$", "").replace(",", "").replace("%", "").strip()
            )
            num = float(cleaned)
            if num > 0:
                arrow = "▲"
                color = "has-text-success"
            elif num < 0:
                arrow = "▼"
                color = "has-text-danger"
            else:
                return value
        inline = "#0f9d58" if color == "has-text-success" else "#d93025"
        return Markup(
            f"<span class='no-wrap {color}' style='color:{inline}'><span class='trend-arrow'>{arrow}</span> {value}</span>"
        )
    except Exception:
        return value


def format_minutes(value):
    """Format minutes with two decimals."""
    try:
        return f"{float(value):.2f}"
    except Exception:
        return value


def heatmap_bg(diff):
    """Return style attribute for heatmap cell based on diff."""
    try:
        val = float(diff)
    except Exception:
        return ""
    if val > 0:
        color = "#e6f4ea"
    elif val < 0:
        color = "#fce8e6"
    else:
        return ""
    return f"style='background:{color}'"


def inject_globals():
    theme = {
        "primary": get_setting("theme_primary", DEFAULT_THEME_PRIMARY),
        "highlight": get_setting("theme_highlight", DEFAULT_THEME_HIGHLIGHT),
        "background": get_setting("theme_background", DEFAULT_THEME_BACKGROUND),
        "text": get_setting("theme_text", DEFAULT_THEME_TEXT),
    }
    return {
        "app_name": get_setting("app_title", "ultrasuite"),
        "theme": theme,
    }


def fetch_resources(uri, rel):
    """Return local file path for xhtml2pdf resource URIs."""
    if uri.startswith("/static"):
        return os.path.join(os.getcwd(), uri.lstrip("/"))
    return uri


def log_error(message: str) -> None:
    """Record a message in the application log."""
    try:
        add_log(message)
    except Exception:
        pass

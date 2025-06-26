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

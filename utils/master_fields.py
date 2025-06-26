from __future__ import annotations

"""Utility helpers for mapping synced API fields to master columns."""

from typing import Dict, Mapping

import pandas as pd

# Maps API-specific field names to master labels used across the app.
FIELD_MAPS: Dict[str, Mapping[str, str]] = {
    "shopify": {
        "price": "price",
        "total": "total",
        "quantity": "qty",
        "description": "description",
        "created_at": "created",
        "sku": "sku",
    },
    "qbo": {
        "price": "price",
        "total": "total",
        "quantity": "qty",
        "description": "description",
        "created_at": "created",
        "sku": "sku",
    },
    "hubspot": {
        "year": "year",
        "month": "month",
        "month_num": "month_num",
        "source": "source",
        "sessions": "sessions",
        "bounce_rate": "bounce_rate",
        "avg_time_min": "avg_time_min",
    },
}


def apply_master_fields(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Return ``df`` with ``master_*`` columns added for ``source``."""
    mapping = FIELD_MAPS.get(source, {})
    out = df.copy()
    for src, master in mapping.items():
        col = f"master_{master}"
        if src in out.columns and col not in out.columns:
            out[col] = out[src]
    return out

"""HubSpot API helper functions."""

from __future__ import annotations

from dataclasses import dataclass

import json
import os
import time
from datetime import datetime

import pandas as pd
import requests

from database import add_api_response


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


@dataclass
class HubSpotClient:
    """Simple HubSpot API client."""

    token: str

    def fetch_traffic_data(
        self,
        start_year: int = 2021,
        end_year: int = 2025,
        *,
        retries: int = 3,
        cache_dir: str | None = None,
    ) -> pd.DataFrame:
        """Return monthly HubSpot traffic analytics for a date range."""
        url = "https://api.hubapi.com/analytics/v2/reports/sources/monthly"
        headers = {"Authorization": f"Bearer {self.token}"}
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
                    add_api_response(
                        "fetch_hubspot", resp.status_code, resp.text[:2000]
                    )
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
                with open(
                    os.path.join(cache_dir, f"hubspot_{page:03d}.json"), "w"
                ) as f:
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
                        "month_num": month,
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


def fetch_hubspot_traffic_data(
    token,
    start_year: int = 2021,
    end_year: int = 2025,
    *,
    retries: int = 3,
    cache_dir: str | None = None,
):
    """Compatibility wrapper for fetching HubSpot traffic data."""
    client = HubSpotClient(token)
    return client.fetch_traffic_data(
        start_year=start_year,
        end_year=end_year,
        retries=retries,
        cache_dir=cache_dir,
    )

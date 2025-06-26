"""Helper functions for fetching Shopify data."""

from __future__ import annotations

import pandas as pd
import requests


def fetch_shopify_api(
    domain: str, token: str, *, since: str | None = None, next_url: str | None = None
):
    """Return order data and pagination cursor from the Shopify API."""
    base = f"https://{domain}/admin/api/2023-07"
    headers = {"X-Shopify-Access-Token": token}

    if next_url:
        url = next_url
        params = None
    else:
        url = f"{base}/orders.json"
        params = {"limit": 250, "status": "any"}
        if since:
            params["created_at_min"] = since
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    orders = payload.get("orders", [])

    rows = []
    line_items = []
    for order in orders:
        order_id = order.get("id")
        created_at = order.get("created_at")
        for idx, item in enumerate(order.get("line_items", [])):
            rows.append(
                {
                    "created_at": created_at,
                    "sku": item.get("sku"),
                    "description": item.get("name"),
                    "quantity": item.get("quantity"),
                    "price": item.get("price"),
                    "total": float(item.get("price", 0))
                    * float(item.get("quantity", 0)),
                }
            )
            line_items.append({"order_id": order_id, "line_num": idx, "data": item})

    df = pd.DataFrame(rows)
    next_cursor = resp.links.get("next", {}).get("url")
    return df, orders, line_items, next_cursor


def fetch_shopify_list(
    domain: str, token: str, endpoint: str, key: str, *, since: str | None = None
):
    """Return a list of records from a Shopify collection endpoint."""
    base = f"https://{domain}/admin/api/2023-07/{endpoint}.json"
    headers = {"X-Shopify-Access-Token": token}
    params = {"limit": 250}
    if since:
        params["updated_at_min"] = since
    resp = requests.get(base, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get(key, []), resp.links.get("next", {}).get("url")

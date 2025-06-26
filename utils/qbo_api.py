"""QuickBooks Online API helper functions."""

from __future__ import annotations

import requests
import pandas as pd


def refresh_qbo_access(client_id: str, client_secret: str, refresh_token: str):
    """Return a new access token and refresh token for QuickBooks."""
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    resp = requests.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        auth=auth,
        data=data,
        timeout=10,
    )
    resp.raise_for_status()
    tokens = resp.json()
    return tokens.get("access_token"), tokens.get("refresh_token")


def qbo_api_url(realm_id: str, path: str, environment: str = "prod") -> str:
    """Return API base URL for QuickBooks."""
    base = (
        "https://sandbox-quickbooks.api.intuit.com"
        if environment == "sandbox"
        else "https://quickbooks.api.intuit.com"
    )
    return f"{base}/v3/company/{realm_id}/{path}"


def fetch_qbo_api(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    realm_id: str,
    environment: str,
    *,
    doc_type: str = "SalesReceipt",
    start_pos: int = 1,
    item_map: dict | None = None,
    fetch_lists: bool = False,
):
    """Return transaction data from QuickBooks."""
    access_token, new_refresh = refresh_qbo_access(
        client_id, client_secret, refresh_token
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    query = f"select * from {doc_type} startposition {start_pos} maxresults 1000"
    url = qbo_api_url(realm_id, "query", environment)
    resp = requests.post(url, headers=headers, json={"query": query}, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    docs = payload.get("QueryResponse", {}).get(doc_type, [])

    rows = []
    lines = []
    for doc in docs:
        created = doc.get("TxnDate")
        doc_id = doc.get("Id") or doc.get("DocNumber")
        for idx, line in enumerate(doc.get("Line", [])):
            detail = line.get("SalesItemLineDetail", {})
            rows.append(
                {
                    "created_at": created,
                    "sku": detail.get("ItemRef", {}).get("value"),
                    "description": line.get("Description"),
                    "quantity": detail.get("Qty"),
                    "price": detail.get("UnitPrice"),
                    "total": line.get("Amount"),
                }
            )
            lines.append({"doc_id": doc_id, "line_num": idx, "data": line})

    df = pd.DataFrame(rows)
    next_pos = start_pos + 1000 if len(docs) == 1000 else None
    item_map = item_map or {}
    return (
        df,
        [],
        docs,
        lines,
        new_refresh,
        headers,
        item_map,
        next_pos,
        [],
        [],
        [],
        [],
    )

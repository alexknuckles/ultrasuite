"""QuickBooks Online API helper functions."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import requests


@dataclass
class QBOClient:
    """Simple QuickBooks Online API client."""

    client_id: str
    client_secret: str
    refresh_token: str
    realm_id: str
    environment: str = "prod"

    def refresh_access(self) -> tuple[str | None, str | None]:
        """Return a new access token and refresh token."""
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}
        resp = requests.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            auth=auth,
            data=data,
            timeout=10,
        )
        resp.raise_for_status()
        tokens = resp.json()
        self.refresh_token = tokens.get("refresh_token")
        return tokens.get("access_token"), self.refresh_token

    def api_url(self, path: str) -> str:
        """Return the API base URL."""
        base = (
            "https://sandbox-quickbooks.api.intuit.com"
            if self.environment == "sandbox"
            else "https://quickbooks.api.intuit.com"
        )
        return f"{base}/v3/company/{self.realm_id}/{path}"

    def fetch_transactions(
        self,
        *,
        doc_type: str = "SalesReceipt",
        start_pos: int = 1,
        item_map: dict | None = None,
        fetch_lists: bool = False,
    ):
        """Return transaction data from QuickBooks."""
        access_token, new_refresh = self.refresh_access()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        query = f"select * from {doc_type} startposition {start_pos} maxresults 1000"
        url = self.api_url("query")
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


def refresh_qbo_access(client_id: str, client_secret: str, refresh_token: str):
    """Wrapper to refresh a QBO token without instantiating a client."""
    client = QBOClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        realm_id="",
    )
    return client.refresh_access()


def qbo_api_url(realm_id: str, path: str, environment: str = "prod") -> str:
    """Wrapper for generating a QBO API URL."""
    client = QBOClient(
        client_id="",
        client_secret="",
        refresh_token="",
        realm_id=realm_id,
        environment=environment,
    )
    return client.api_url(path)


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
    """Compatibility wrapper for fetching transactions."""
    client = QBOClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        realm_id=realm_id,
        environment=environment,
    )
    return client.fetch_transactions(
        doc_type=doc_type,
        start_pos=start_pos,
        item_map=item_map,
        fetch_lists=fetch_lists,
    )

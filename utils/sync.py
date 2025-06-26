import json
from collections.abc import MutableMapping
from typing import Any, Dict

import sqlite3


def flatten_json(
    data: Any, parent_key: str = "", sep: str = "_"
) -> Dict[str, Any]:
    """Return a flat dict from a nested JSON structure."""
    items = {}
    if isinstance(data, MutableMapping):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
            if isinstance(v, MutableMapping):
                items.update(flatten_json(v, new_key, sep=sep))
            elif isinstance(v, list):
                items[new_key] = json.dumps(v)
            else:
                items[new_key] = v
    elif isinstance(data, list):
        items[parent_key] = json.dumps(data)
    else:
        items[parent_key] = data
    return items


def ensure_columns(conn: sqlite3.Connection, table: str, columns) -> None:
    """Ensure all columns exist on the table."""
    existing = {
        row["name"] for row in conn.execute(f"PRAGMA table_info({table})")
    }
    for col in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")


def upsert_record(
    conn: sqlite3.Connection, table: str, record: Dict[str, Any], pk: str
) -> None:
    """Insert or replace a record with dynamic columns."""
    flattened = flatten_json(record)
    flattened[pk] = record.get(pk)
    flattened["raw_json"] = json.dumps(record)
    ensure_columns(conn, table, flattened.keys())
    cols = ", ".join(flattened.keys())
    placeholders = ", ".join(["?"] * len(flattened))
    values = [flattened[c] for c in flattened.keys()]
    conn.execute(
        f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})",
        values,
    )

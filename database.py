import os
import sqlite3

import sys
from datetime import datetime, timezone


if getattr(sys, "frozen", False):
    base_dir = os.path.join(os.path.expanduser("~"), "ultrasuite")
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

UPLOAD_FOLDER = os.path.join(base_dir, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
DB_PATH = os.path.join(base_dir, "finance.db")


def get_db(timeout=30.0):
    """Return a SQLite connection with a longer timeout.

    A 30 second timeout reduces ``database is locked`` errors when multiple
    writes occur concurrently. WAL mode is enabled to improve concurrency.
    """
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS meta ("
        "source TEXT PRIMARY KEY, "
        "last_updated TEXT, "
        "last_transaction TEXT, "
        "first_transaction TEXT, "
        "last_synced TEXT"
        ")"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS shopify (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS shopify_orders (order_id INTEGER PRIMARY KEY, data TEXT)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS qbo (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS sku_map (alias TEXT PRIMARY KEY, canonical_sku TEXT, type TEXT, source TEXT, changed_at TEXT)"
    )
    c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    c.execute(
        "CREATE TABLE IF NOT EXISTS duplicate_log ("
        "resolved_at TEXT, "
        "shopify_id INTEGER, "
        "qbo_id INTEGER, "
        "action TEXT, "
        "sku TEXT, "
        "shopify_sku TEXT, "
        "qbo_sku TEXT, "
        "quantity REAL, "
        "total REAL, "
        "shopify_desc TEXT, "
        "qbo_desc TEXT, "
        "created_at TEXT, "
        "shopify_created_at TEXT, "
        "qbo_created_at TEXT, "
        "ignored INTEGER DEFAULT 0"
        ")"
    )
    conn.commit()
    conn.close()


def migrate_types():
    conn = get_db()
    conn.execute("UPDATE sku_map SET type='parts' WHERE type='maintenance'")
    conn.commit()
    conn.close()


def migrate_meta():
    """Ensure new columns exist in the meta table."""
    conn = get_db()
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(meta)").fetchall()]
    if "last_transaction" not in cols:
        conn.execute("ALTER TABLE meta ADD COLUMN last_transaction TEXT")
        conn.commit()
    if "first_transaction" not in cols:
        conn.execute("ALTER TABLE meta ADD COLUMN first_transaction TEXT")
        conn.commit()
    if "last_synced" not in cols:
        conn.execute("ALTER TABLE meta ADD COLUMN last_synced TEXT")
        conn.commit()
    conn.close()


def migrate_sku_source():
    """Ensure source column exists in the sku_map table."""
    conn = get_db()
    cols = [
        row["name"] for row in conn.execute("PRAGMA table_info(sku_map)").fetchall()
    ]
    if "source" not in cols:
        conn.execute("ALTER TABLE sku_map ADD COLUMN source TEXT")
        conn.commit()
    conn.close()


def migrate_sku_changed():
    """Ensure changed_at column exists in the sku_map table."""
    conn = get_db()
    cols = [
        row["name"] for row in conn.execute("PRAGMA table_info(sku_map)").fetchall()
    ]
    if "changed_at" not in cols:
        conn.execute("ALTER TABLE sku_map ADD COLUMN changed_at TEXT")
        conn.commit()
    conn.close()


def migrate_duplicate_log():
    """Ensure duplicate_log table exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS duplicate_log ("
        "resolved_at TEXT, "
        "shopify_id INTEGER, "
        "qbo_id INTEGER, "
        "action TEXT, "
        "sku TEXT, "
        "shopify_sku TEXT, "
        "qbo_sku TEXT, "
        "quantity REAL, "
        "total REAL, "
        "shopify_desc TEXT, "
        "qbo_desc TEXT, "
        "created_at TEXT, "
        "ignored INTEGER DEFAULT 0"
        ")"
    )
    cols = [
        row["name"]
        for row in conn.execute("PRAGMA table_info(duplicate_log)").fetchall()
    ]
    if "created_at" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN created_at TEXT")
        conn.commit()
    if "shopify_created_at" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN shopify_created_at TEXT")
        conn.commit()
    if "qbo_created_at" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN qbo_created_at TEXT")
        conn.commit()
    if "shopify_sku" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN shopify_sku TEXT")
        conn.commit()
    if "qbo_sku" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN qbo_sku TEXT")
        conn.commit()
    if "ignored" not in cols:
        conn.execute("ALTER TABLE duplicate_log ADD COLUMN ignored INTEGER DEFAULT 0")
        conn.commit()
    conn.commit()
    conn.close()


def migrate_shopify_orders():
    """Ensure table for raw Shopify orders exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS shopify_orders (order_id INTEGER PRIMARY KEY, data TEXT)"
    )
    conn.close()


def migrate_shopify_lines():
    """Ensure table for raw Shopify line items exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS shopify_lines ("
        "order_id INTEGER, "
        "line_num INTEGER, "
        "data TEXT, "
        "PRIMARY KEY (order_id, line_num)"
        ")"
    )
    conn.close()


def migrate_qbo_docs():
    """Ensure table for raw QBO documents exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qbo_docs (doc_id TEXT PRIMARY KEY, data TEXT)"
    )
    conn.close()


def migrate_qbo_lines():
    """Ensure table for raw QBO line items exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS qbo_lines ("
        "doc_id TEXT, "
        "line_num INTEGER, "
        "data TEXT, "
        "PRIMARY KEY (doc_id, line_num)"
        ")"
    )
    conn.close()


def migrate_app_log():
    """Ensure table for application logs exists."""
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS app_log (logged_at TEXT, message TEXT)")
    conn.close()


def migrate_hubspot_traffic():
    """Ensure table for HubSpot traffic analytics exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hubspot_traffic ("
        "year INTEGER, "
        "month INTEGER, "
        "source TEXT, "
        "sessions REAL, "
        "avg_time REAL, "
        "bounce_rate REAL, "
        "PRIMARY KEY (year, month, source)"
        ")"
    )
    conn.close()


def migrate_api_responses():
    """Ensure table for storing API responses exists."""
    conn = get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_response ("
        "logged_at TEXT, "
        "endpoint TEXT, "
        "status INTEGER, "
        "body TEXT"
        ")"
    )
    conn.close()


def get_setting(key, default=""):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key, value, conn=None):
    """Persist a single application setting.

    Parameters
    ----------
    key : str
        Setting name.
    value : str
        Setting value.
    conn : sqlite3.Connection | None, optional
        Existing database connection to use. When ``None``, a new connection
        is created for the operation. Providing a connection avoids opening a
        separate transaction and helps prevent ``database is locked`` errors
        during long-running updates.
    """

    own_conn = conn is None
    if own_conn:
        conn = get_db()
    conn.execute("REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))
    if own_conn:
        conn.commit()
        conn.close()


def set_settings(pairs):
    """Update multiple settings in a single transaction."""
    conn = get_db()
    conn.executemany(
        "REPLACE INTO settings(key, value) VALUES (?, ?)",
        pairs,
    )
    conn.commit()
    conn.close()


def get_qbo_environment(default="prod"):
    """Return the configured QBO environment."""
    env = get_setting("qbo_environment", default)
    return env if env in {"prod", "sandbox"} else default


def set_qbo_environment(value):
    """Persist the QBO environment setting."""
    if value not in {"prod", "sandbox"}:
        raise ValueError("Invalid environment")
    set_setting("qbo_environment", value)


def add_log(message):
    conn = get_db()
    conn.execute(
        "INSERT INTO app_log(logged_at, message) VALUES (?, ?)",
        (datetime.now(timezone.utc).isoformat(), message),
    )
    conn.commit()
    conn.close()


def get_logs(limit=100):
    conn = get_db()
    rows = conn.execute(
        "SELECT logged_at, message FROM app_log ORDER BY logged_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def add_api_response(endpoint, status, body):
    conn = get_db()
    conn.execute(
        "INSERT INTO api_response(logged_at, endpoint, status, body) VALUES (?, ?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), endpoint, status, body),
    )
    conn.commit()
    conn.close()


def get_api_responses(limit=50):
    conn = get_db()
    rows = conn.execute(
        "SELECT logged_at, endpoint, status, body FROM api_response ORDER BY logged_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return rows


init_db()
migrate_types()
migrate_meta()
migrate_sku_source()
migrate_sku_changed()
migrate_duplicate_log()
migrate_shopify_orders()
migrate_shopify_lines()
migrate_qbo_docs()
migrate_qbo_lines()
migrate_app_log()
migrate_hubspot_traffic()
migrate_api_responses()


def migrate_sync_tables():
    """Ensure tables for full sync data exist."""
    conn = get_db()
    tables = {
        "shopify_orders": "shopify_id",
        "shopify_customers": "shopify_id",
        "shopify_products": "shopify_id",
        "qbo_customers": "qbo_id",
        "qbo_invoices": "qbo_id",
        "qbo_payments": "qbo_id",
        "qbo_products": "qbo_id",
    }
    for name, pk in tables.items():
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {name} ({pk} TEXT PRIMARY KEY, raw_json TEXT)"
        )
    conn.close()


migrate_sync_tables()

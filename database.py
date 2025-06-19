import os
import sqlite3

import sys


if getattr(sys, 'frozen', False):
    base_dir = os.path.join(os.path.expanduser('~'), 'ultrasuite')
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

UPLOAD_FOLDER = os.path.join(base_dir, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
DB_PATH = os.path.join(base_dir, 'finance.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS meta ("
        "source TEXT PRIMARY KEY, "
        "last_updated TEXT, "
        "last_transaction TEXT, "
        "first_transaction TEXT"
        ")"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS shopify (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS qbo (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS sku_map (alias TEXT PRIMARY KEY, canonical_sku TEXT, type TEXT, source TEXT, changed_at TEXT)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
    )
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
    cols = [row['name'] for row in conn.execute('PRAGMA table_info(meta)').fetchall()]
    if 'last_transaction' not in cols:
        conn.execute('ALTER TABLE meta ADD COLUMN last_transaction TEXT')
        conn.commit()
    if 'first_transaction' not in cols:
        conn.execute('ALTER TABLE meta ADD COLUMN first_transaction TEXT')
        conn.commit()
    conn.close()

def migrate_sku_source():
    """Ensure source column exists in the sku_map table."""
    conn = get_db()
    cols = [row['name'] for row in conn.execute('PRAGMA table_info(sku_map)').fetchall()]
    if 'source' not in cols:
        conn.execute('ALTER TABLE sku_map ADD COLUMN source TEXT')
        conn.commit()
    conn.close()

def migrate_sku_changed():
    """Ensure changed_at column exists in the sku_map table."""
    conn = get_db()
    cols = [row['name'] for row in conn.execute('PRAGMA table_info(sku_map)').fetchall()]
    if 'changed_at' not in cols:
        conn.execute('ALTER TABLE sku_map ADD COLUMN changed_at TEXT')
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
    cols = [row['name'] for row in conn.execute('PRAGMA table_info(duplicate_log)').fetchall()]
    if 'created_at' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN created_at TEXT')
        conn.commit()
    if 'shopify_created_at' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN shopify_created_at TEXT')
        conn.commit()
    if 'qbo_created_at' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN qbo_created_at TEXT')
        conn.commit()
    if 'shopify_sku' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN shopify_sku TEXT')
        conn.commit()
    if 'qbo_sku' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN qbo_sku TEXT')
        conn.commit()
    if 'ignored' not in cols:
        conn.execute('ALTER TABLE duplicate_log ADD COLUMN ignored INTEGER DEFAULT 0')
        conn.commit()
    conn.commit()
    conn.close()

def get_setting(key, default=""):
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute('REPLACE INTO settings(key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

init_db()
migrate_types()
migrate_meta()
migrate_sku_source()
migrate_sku_changed()
migrate_duplicate_log()

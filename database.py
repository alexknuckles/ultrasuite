import os
import sqlite3

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'finance.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS meta (source TEXT PRIMARY KEY, last_updated TEXT)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS shopify (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS qbo (created_at TEXT, sku TEXT, description TEXT, quantity REAL, price REAL, total REAL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS sku_map (alias TEXT PRIMARY KEY, canonical_sku TEXT, type TEXT)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()
    conn.close()

def migrate_types():
    conn = get_db()
    conn.execute("UPDATE sku_map SET type='parts' WHERE type='maintenance'")
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

import os

from datetime import datetime, timedelta, timezone
from io import BytesIO
import base64
from calendar import monthrange
import requests
import json

import pandas as pd
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from xhtml2pdf import pisa
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    send_from_directory,
    abort,
    jsonify,
    session,
)
from markupsafe import Markup
from werkzeug.utils import secure_filename
from database import (
    UPLOAD_FOLDER,
    get_db,
    get_setting,
    set_setting,
    add_log,
    get_logs,
)

# default theme colors
DEFAULT_THEME_PRIMARY = '#1976d2'
DEFAULT_THEME_HIGHLIGHT = '#bbdefb'
DEFAULT_THEME_BACKGROUND = '#f8f9fa'
DEFAULT_THEME_TEXT = '#363636'

THEMES = {
    'ocean': {
        'primary': '#1976d2',
        'highlight': '#bbdefb',
        'background': '#f8f9fa',
        'text': '#363636',
    },
    'orchid': {
        'primary': '#9c27b0',
        'highlight': '#f3e5f5',
        'background': '#ffffff',
        'text': '#363636',
    },
    'codex': {
        'primary': '#1976d2',
        'highlight': '#161b22',
        'background': '#0d1117',
        'text': '#c9d1d9',
    },
    'midnight': {
        'primary': '#90caf9',
        'highlight': '#0d47a1',
        'background': '#121212',
        'text': '#e0e0e0',
    },
}

def _hex_to_rgb(value):
    """Return (r, g, b) for a hex color string."""
    value = value.lstrip('#')
    if len(value) == 3:
        value = ''.join(c * 2 for c in value)
    try:
        r, g, b = [int(value[i:i + 2], 16) for i in (0, 2, 4)]
    except Exception:
        return 0, 0, 0
    return r, g, b


def _is_dark_color(value):
    """Return True if the color is dark based on luminance."""
    r, g, b = _hex_to_rgb(value)
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return luminance < 128


def _chart_style():
    bg = get_setting('theme_background', DEFAULT_THEME_BACKGROUND)
    return 'dark_background' if _is_dark_color(bg) else 'default'

def _try_read_csv(data: bytes, encodings=None):
    """Try reading CSV bytes with a series of encodings."""
    encodings = encodings or ["utf-8", "utf-8-sig", "utf-16", "latin-1", "cp1252"]
    for enc in encodings:
        try:
            return pd.read_csv(BytesIO(data), encoding=enc)
        except Exception:
            continue
    raise ValueError("Unsupported CSV encoding")


def _parse_shopify(file_storage):
    data = file_storage.read()
    file_storage.seek(0)
    # try csv first then excel
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
    cleaned = df[[
        "Created at",
        "Lineitem sku",
        "Lineitem name",
        "Lineitem quantity",
        "Lineitem price",
    ]].copy()
    cleaned["Total"] = pd.to_numeric(df["Lineitem price"], errors="coerce").fillna(0) * pd.to_numeric(
        df["Lineitem quantity"], errors="coerce"
    ).fillna(0)
    cleaned.columns = ["created_at", "sku", "description", "quantity", "price", "total"]
    return cleaned


def _parse_qbo(file_storage):
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
        # Set proper headers if they appear exactly as expected from QBO
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
    cleaned = df[[
        "transaction_date",
        "product_service",
        "line_description",
        "quantity",
        "sales_price",
        "amount",
    ]].copy()
    cleaned.columns = ["created_at", "sku", "description", "quantity", "price", "total"]
    return cleaned


def _fetch_shopify_api(domain, token):
    """Return a DataFrame of Shopify orders via API and raw JSON data."""
    headers = {"X-Shopify-Access-Token": token}
    url = f"https://{domain}/admin/api/2023-07/orders.json"
    params = {"status": "any", "limit": 250}
    orders = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("orders", [])
        orders.extend(data)
        link = resp.headers.get("Link", "")
        next_url = None
        for part in link.split(','):
            if 'rel="next"' in part:
                start = part.find('<') + 1
                end = part.find('>')
                next_url = part[start:end]
                break
        url = next_url
        params = None
    rows = []
    for order in orders:
        created = order.get("created_at")
        for line in order.get("line_items", []):
            rows.append({
                "created_at": created,
                "sku": line.get("sku"),
                "description": line.get("name"),
                "quantity": line.get("quantity"),
                "price": line.get("price"),
                "total": (float(line.get("price", 0)) * float(line.get("quantity", 0))),
            })
    df = pd.DataFrame(rows, columns=["created_at", "sku", "description", "quantity", "price", "total"])
    return df, orders


def _refresh_qbo_access(client_id, client_secret, refresh_token):
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
    return tokens.get("access_token"), tokens.get("refresh_token", refresh_token)


def _fetch_qbo_api(client_id, client_secret, refresh_token, realm_id):
    """Return a DataFrame of QBO transactions via API."""
    access_token, new_refresh = _refresh_qbo_access(
        client_id, client_secret, refresh_token
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    url = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/reports/TransactionList"
    params = {"start_date": "2000-01-01"}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for row in data.get("Rows", {}).get("Row", []):
        cols = [c.get("value") for c in row.get("ColData", [])]
        if len(cols) < 7:
            continue
        created_at = cols[0]
        desc = cols[4]
        sku = cols[5]
        total = float(cols[6] or 0)
        rows.append({
            "created_at": created_at,
            "sku": sku,
            "description": desc,
            "quantity": 1,
            "price": total,
            "total": total,
        })
    df = pd.DataFrame(
        rows,
        columns=["created_at", "sku", "description", "quantity", "price", "total"],
    )
    return df, new_refresh

app = Flask(__name__)
app.secret_key = 'secret'

# Height in pixels for the branding logo on exported PDFs
# Increase size now that margins are trimmed further
LOGO_SIZE = 360

# Available SKU type categories for reports
CATEGORIES = [
    'machine',
    'detergent_filter_kits',
    'detergent',
    'filters',
    'parts',
    'service',
    'shopify',
    'shipping',
]

CATEGORY_LABELS = {
    'machine': 'Machines',
    'detergent_filter_kits': 'Detergent & Filter Kits',
    'detergent': 'Detergents',
    'filters': 'Filters',
    'parts': 'Parts',
    'service': 'Service',
    'shopify': 'Shopify',
    'shipping': 'Shipping',
}

# Common orderings for monthly and quarterly summaries
MONTHS_ORDER = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
]
QUARTER_MAP = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
}

def format_dt(value):
    """Format ISO timestamp into 'mm/dd/yy - h:mma/pm'."""
    try:
        dt = datetime.fromisoformat(value)
        time_str = dt.strftime("%I:%M%p").lstrip('0').lower()
        date_str = dt.strftime("%m/%d/%y")
        return f"{date_str} - {time_str}"
    except Exception:
        return value

app.jinja_env.filters['format_dt'] = format_dt

def trend(value, compare=None):
    """Return HTML with colored arrow indicating trend.

    If ``compare`` is provided, its numeric sign is used to determine the
    arrow direction while ``value`` is displayed unchanged. This allows
    showing the previous period's value with an arrow based on the change
    from the current period.
    """
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
                sign_str.replace("$", "")
                .replace(",", "")
                .replace("%", "")
                .strip()
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
        inline = '#0f9d58' if color == 'has-text-success' else '#d93025'
        return Markup(
            f"<span class='no-wrap {color}' style='color:{inline}'><span class='trend-arrow'>{arrow}</span> {value}</span>"
        )
    except Exception:
        return value

app.jinja_env.filters['trend'] = trend


@app.context_processor
def inject_globals():
    theme = {
        'primary': get_setting('theme_primary', DEFAULT_THEME_PRIMARY),
        'highlight': get_setting('theme_highlight', DEFAULT_THEME_HIGHLIGHT),
        'background': get_setting('theme_background', DEFAULT_THEME_BACKGROUND),
        'text': get_setting('theme_text', DEFAULT_THEME_TEXT),
    }
    return {
        'app_name': get_setting('app_title', 'ultrasuite'),
        'theme': theme,
    }


def fetch_resources(uri, rel):
    """Return local file path for xhtml2pdf resource URIs."""
    if uri.startswith('/static'):
        return os.path.join(app.root_path, uri.lstrip('/'))
    return uri

def log_error(message):
    """Record a message in the application log."""
    try:
        add_log(message)
    except Exception:
        pass


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )

@app.route('/logo.png')
def logo():
    """Serve the application logo, using an uploaded file if available."""
    custom = get_setting('app_logo', '')
    if custom:
        path = custom if os.path.isabs(custom) else os.path.join(app.root_path, custom)
        if os.path.exists(path):
            return send_file(path, mimetype='image/png')
    return send_file(os.path.join(app.root_path, 'static', 'ultrasuite-logo.png'), mimetype='image/png')


@app.route('/branding-logo.png')
def branding_logo():
    """Serve the uploaded branding logo for reports, falling back to default."""
    custom = get_setting('branding_logo', '')
    if custom:
        path = custom if os.path.isabs(custom) else os.path.join(app.root_path, custom)
        if os.path.exists(path):
            return send_file(path, mimetype='image/png')
    return send_file(os.path.join(app.root_path, 'static', 'ultrasuite-logo.png'), mimetype='image/png')


@app.route('/')
def dashboard():
    conn = get_db()
    meta_df = pd.read_sql_query('SELECT * FROM meta', conn)
    sku_df = pd.read_sql_query(
        'SELECT type, COUNT(DISTINCT canonical_sku) as sku_count '
        'FROM sku_map GROUP BY type',
        conn,
    )
    conn.close()
    return render_template(
        'dashboard.html',
        meta=meta_df.itertuples(),
        sku_stats=sku_df.itertuples(),
    )

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        pairs = []
        for key in request.files:
            if key.startswith('data_file_'):
                idx = key.split('_')[-1]
                file = request.files.get(key)
                source = request.form.get(f'source_{idx}', '').lower()
                pairs.append((file, source))
        if not pairs:
            flash('Please provide a file and select its source.')
            return redirect(request.url)

        conn = get_db()
        try:
            for data_file, source in pairs:
                if not data_file or not source:
                    flash('Please provide a file and select its source.')
                    conn.close()
                    return redirect(request.url)

                if source == 'shopify':
                    cleaned = _parse_shopify(data_file)
                    cleaned.to_sql('shopify', conn, if_exists='replace', index=False)
                elif source == 'qbo':
                    cleaned = _parse_qbo(data_file)
                    cleaned.to_sql('qbo', conn, if_exists='replace', index=False)
                elif source == 'sku_map':
                    try:
                        if data_file.filename.lower().endswith(('.xls', '.xlsx')):
                            df = pd.read_excel(data_file)
                        else:
                            df = pd.read_csv(data_file)
                    except Exception:
                        flash('Unable to parse SKU map file.')
                        conn.close()
                        return redirect(request.url)
                    required = {'alias', 'canonical_sku', 'type'}
                    if not required.issubset(df.columns):
                        flash('SKU map file must contain alias, canonical_sku and type columns.')
                        conn.close()
                        return redirect(request.url)
                    if 'source' not in df.columns:
                        df['source'] = ''
                    conn.execute('DELETE FROM sku_map')
                    for row in df[['alias', 'canonical_sku', 'type', 'source']].itertuples(index=False):
                        alias = str(row.alias).lower().strip()
                        canonical = str(row.canonical_sku).lower().strip() or alias
                        type_val = str(row.type).lower().strip() or 'unmapped'
                        source_val = str(row.source).lower().strip() if hasattr(row, 'source') else ''
                        conn.execute(
                            'INSERT INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
                            (alias, canonical, type_val, source_val, datetime.now(timezone.utc).isoformat()),
                        )
                    continue
                else:
                    flash('Unknown source selected.')
                    conn.close()
                    return redirect(request.url)

                created = pd.to_datetime(
                    cleaned['created_at'].astype(str),
                    errors='coerce',
                    format='mixed',
                    utc=True,
                ).dt.tz_localize(None)
                last_txn = created.max()
                first_txn = created.min()
                row = conn.execute('SELECT last_synced FROM meta WHERE source=?', (source,)).fetchone()
                last_synced = row['last_synced'] if row else None
                conn.execute(
                    'REPLACE INTO meta (source, last_updated, last_transaction, first_transaction, last_synced) VALUES (?, ?, ?, ?, ?)',
                    (
                        source,
                        datetime.now().isoformat(),
                        last_txn.isoformat() if pd.notna(last_txn) else None,
                        first_txn.isoformat() if pd.notna(first_txn) else None,
                        last_synced,
                    )
                )
                _update_sku_map(conn, cleaned['sku'], source)
            action = get_setting('duplicate_action', 'review')
            if action in {'shopify', 'qbo', 'both'}:
                _resolve_duplicates(conn, action)
            conn.commit()
            flash('File uploaded and data updated.')
            conn.close()
            return redirect(url_for('dashboard'))
        except ValueError:
            flash('Failed to process file: Invalid data')
            conn.close()
            return render_template('upload.html')
        except Exception as exc:
            flash(f'Failed to process file: {exc}')
            conn.close()
            return render_template('upload.html')
    return render_template('upload.html')


def _update_sku_map(conn, sku_series, source=None):
    aliases = sku_series.dropna().str.lower().str.strip().unique()
    for alias in aliases:
        row = conn.execute('SELECT 1 FROM sku_map WHERE alias=?', (alias,)).fetchone()
        if not row:
            conn.execute(
                'INSERT INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
                (alias, alias, 'unmapped', source, datetime.now(timezone.utc).isoformat())
            )


def _save_types(conn, form):
    entries = [k.split('_')[1] for k in form.keys() if k.startswith('canonical_')]
    for idx in entries:
        canonical = form.get(f'canonical_{idx}', '').lower().strip()
        type_val = form.get(f'type_{idx}', 'unmapped')
        if canonical:
            conn.execute(
                'UPDATE sku_map SET type=?, changed_at=? WHERE canonical_sku=?',
                (type_val, datetime.now(timezone.utc).isoformat(), canonical)
            )


def _resolve_duplicates(conn, action):
    """Resolve duplicate transactions between Shopify and QBO."""
    pairs = _find_duplicates(conn)
    for p in pairs:
        if p.get('unmatched'):
            continue
        conn.execute(
            "INSERT INTO duplicate_log(resolved_at, shopify_id, qbo_id, action, sku, shopify_sku, qbo_sku, quantity, total, shopify_desc, qbo_desc, created_at, shopify_created_at, qbo_created_at, ignored) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
            (
                datetime.now(timezone.utc).isoformat(),
                p['shopify_id'],
                p['qbo_id'],
                action,
                p['sku'],
                p.get('shopify_sku'),
                p.get('qbo_sku'),
                p['quantity'],
                p['total'],
                p['shopify_desc'],
                p['qbo_desc'],
                p['created_at'],
                p['shopify_created_at'],
                p['qbo_created_at'],
            ),
        )
        if action == 'shopify':
            conn.execute('DELETE FROM qbo WHERE rowid=?', (p['qbo_id'],))
        elif action == 'qbo':
            conn.execute('DELETE FROM shopify WHERE rowid=?', (p['shopify_id'],))
        elif action == 'both':
            continue

def _resolve_duplicate_pair(conn, sid, qid, action):
    """Resolve a specific duplicate pair."""
    pairs = _find_duplicates(conn)
    pair = next((p for p in pairs if p['shopify_id'] == sid and p['qbo_id'] == qid), None)
    if not pair:
        return
    conn.execute('UPDATE duplicate_log SET ignored=0 WHERE shopify_id=? AND qbo_id=?', (sid, qid))
    conn.execute(
        "INSERT INTO duplicate_log(resolved_at, shopify_id, qbo_id, action, sku, shopify_sku, qbo_sku, quantity, total, shopify_desc, qbo_desc, created_at, shopify_created_at, qbo_created_at, ignored) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
        (
            datetime.now(timezone.utc).isoformat(),
            sid,
            qid,
            action,
            pair['sku'],
            pair.get('shopify_sku'),
            pair.get('qbo_sku'),
            pair['quantity'],
            pair['total'],
            pair['shopify_desc'],
            pair['qbo_desc'],
            pair['created_at'],
            pair['shopify_created_at'],
            pair['qbo_created_at'],
        ),
    )
    if action == 'shopify':
        conn.execute('DELETE FROM qbo WHERE rowid=?', (qid,))
    elif action == 'qbo':
        conn.execute('DELETE FROM shopify WHERE rowid=?', (sid,))

def _find_duplicates(conn, sku=None, start=None, end=None):
    """Return possible duplicate transactions between Shopify and QBO.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    sku : str, optional
        Canonical SKU to filter by.
    start : datetime, optional
        Include transactions on or after this date.
    end : datetime, optional
        Include transactions on or before this date.
    """
    shopify = pd.read_sql_query(
        'SELECT rowid AS id, created_at, sku, description, quantity, total FROM shopify',
        conn,
    )
    qbo = pd.read_sql_query(
        'SELECT rowid AS id, created_at, sku, description, quantity, total FROM qbo',
        conn,
    )
    mapping = pd.read_sql_query('SELECT alias, canonical_sku FROM sku_map', conn)

    alias_map = mapping.copy()
    alias_map['alias'] = alias_map['alias'].str.lower()
    alias_map = alias_map.set_index('alias')['canonical_sku']

    def canonical(alias):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key]
            return key
        return alias

    for df in (shopify, qbo):
        df['canonical'] = df['sku'].apply(canonical)
        df['quantity'] = (
            pd.to_numeric(df['quantity'], errors='coerce').astype(float)
        )
        df['total'] = (
            pd.to_numeric(df['total'], errors='coerce').astype(float)
        )
        df['created_at'] = (
            pd.to_datetime(df['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
            .dt.tz_localize(None)
        )
        df['date'] = df['created_at'].dt.date

    shopify = shopify.dropna(subset=['created_at'])
    qbo = qbo.dropna(subset=['created_at'])

    if start is not None:
        shopify = shopify[shopify['created_at'] >= start]
        qbo = qbo[qbo['created_at'] >= start]
    if end is not None:
        shopify = shopify[shopify['created_at'] <= end]
        qbo = qbo[qbo['created_at'] <= end]
    if sku:
        shopify = shopify[shopify['canonical'] == sku]
        qbo = qbo[qbo['canonical'] == sku]

    merged = pd.merge(
        shopify,
        qbo,
        on=['date', 'canonical', 'quantity', 'total'],
        suffixes=('_s', '_q'),
    )

    ignored_df = pd.read_sql_query(
        'SELECT shopify_id, qbo_id FROM duplicate_log WHERE ignored=1', conn
    )
    ignored_pairs = set(map(tuple, ignored_df.values)) if not ignored_df.empty else set()

    unmatched_df = pd.read_sql_query(
        "SELECT shopify_id, qbo_id FROM duplicate_log WHERE action='unmatched'",
        conn,
    )
    unmatched_pairs = set(map(tuple, unmatched_df.values)) if not unmatched_df.empty else set()

    rows = []
    for r in merged.itertuples(index=False):
        ts = min(r.created_at_s, r.created_at_q)
        rows.append(
            {
                'shopify_id': r.id_s,
                'qbo_id': r.id_q,
                'created_at': ts.isoformat(sep=' ', timespec='seconds'),
                'shopify_created_at': r.created_at_s.isoformat(sep=' ', timespec='seconds'),
                'qbo_created_at': r.created_at_q.isoformat(sep=' ', timespec='seconds'),
                'sku': r.canonical,
                'shopify_sku': r.sku_s,
                'qbo_sku': r.sku_q,
                'shopify_desc': r.description_s,
                'qbo_desc': r.description_q,
                'quantity': r.quantity,
                'total': r.total,
                'unmatched': (r.id_s, r.id_q) in unmatched_pairs,
                'ignored': (r.id_s, r.id_q) in ignored_pairs,
            }
        )
    return rows


def _safe_concat(frames, **kwargs):
    """Concatenate frames, skipping empties and all-NA data."""
    if not frames:
        return pd.DataFrame()

    valid = [df for df in frames if not df.empty and not df.isna().all().all()]
    if valid:
        return pd.concat(valid, **kwargs)

    # All frames are empty or all-NA; preserve union of columns
    columns = []
    for df in frames:
        for col in df.columns:
            if col not in columns:
                columns.append(col)
    return pd.DataFrame(columns=columns)

@app.route('/sku-map', methods=['GET', 'POST'])
def sku_map_page():
    conn = get_db()
    if request.method == 'POST':
        if 'merge' in request.form:
            _save_types(conn, request.form)
            target = request.form.get('merge_target', '').lower().strip()
            selected = [
                request.form[k].lower().strip()
                for k in request.form
                if k.startswith('select_')
            ]
            if not target and selected:
                target = selected[0]
            if target and selected:
                type_row = conn.execute(
                    'SELECT type FROM sku_map WHERE canonical_sku=? LIMIT 1',
                    (target,),
                ).fetchone()
                target_type = type_row['type'] if type_row else 'unmapped'
                for canonical in selected:
                    rows = conn.execute(
                        'SELECT alias, source FROM sku_map WHERE canonical_sku=?',
                        (canonical,),
                    ).fetchall()
                    for r in rows:
                        conn.execute(
                            'REPLACE INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
                            (r['alias'], target, target_type, r['source'], datetime.now(timezone.utc).isoformat()),
                        )
                    if canonical != target:
                        conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (canonical,))
                conn.execute('UPDATE sku_map SET type=?, changed_at=? WHERE canonical_sku=?',
                             (target_type, datetime.now(timezone.utc).isoformat(), target))
                conn.commit()
                flash('Entries merged.')
        else:
            entries = [k.split('_')[1] for k in request.form.keys() if k.startswith('canonical_')]
            for idx in entries:
                canonical = request.form.get(f'canonical_{idx}', '').lower().strip()
                aliases = request.form.get(f'aliases_{idx}', '')
                type_val = request.form.get(f'type_{idx}', 'unmapped')
                if not canonical:
                    continue
                alias_list = [a.lower().strip() for a in aliases.split(',') if a.strip()]
                alias_set = set(alias_list + [canonical])
                conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (canonical,))
                for alias in alias_set:
                    conn.execute(
                        'REPLACE INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
                        (alias, canonical, type_val, '', datetime.now(timezone.utc).isoformat())
                    )
            canonical_new = request.form.get('canonical_new', '').lower().strip()
            aliases_new = request.form.get('aliases_new', '')
            type_new = request.form.get('type_new', 'unmapped')
            if canonical_new:
                alias_list = [a.lower().strip() for a in aliases_new.split(',') if a.strip()]
                alias_set = set(alias_list + [canonical_new])
                for alias in alias_set:
                    conn.execute(
                        'REPLACE INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
                        (alias, canonical_new, type_new, '', datetime.now(timezone.utc).isoformat())
                    )
            conn.commit()
            flash('SKU map updated.')
        conn.close()
        return redirect(url_for('sku_map_page'))

    mapping_df = pd.read_sql_query(
        'SELECT alias, canonical_sku, type, source, changed_at FROM sku_map',
        conn,
    )
    shopify = pd.read_sql_query('SELECT created_at, sku FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku FROM qbo', conn)
    conn.close()

    all_txn = _safe_concat([shopify, qbo], ignore_index=True)
    all_txn['created_at'] = (
        pd.to_datetime(
            all_txn['created_at'].astype(str),
            errors='coerce',
            format='mixed',
            utc=True,
        )
        .dt.tz_localize(None)
    )
    all_txn = all_txn.dropna(subset=['created_at'])

    alias_map = mapping_df.copy()
    alias_map['alias'] = alias_map['alias'].str.lower()
    alias_map = alias_map.set_index('alias')['canonical_sku']

    def canonical(alias):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key]
            return key
        return alias

    all_txn['canonical'] = all_txn['sku'].apply(canonical)
    last_dates = all_txn.groupby('canonical')['created_at'].max().to_dict()
    changed_dates = pd.to_datetime(
        mapping_df['changed_at'], errors='coerce', utc=True
    ).dt.tz_localize(None)
    mapping_df['changed_dt'] = changed_dates
    changed_map = mapping_df.groupby('canonical_sku')['changed_dt'].max().to_dict()

    grouped = {}
    for r in mapping_df.itertuples(index=False):
        entry = grouped.setdefault(r.canonical_sku, {
            'canonical': r.canonical_sku,
            'aliases': [],
            'type': r.type,
            'source': ''
        })
        if r.alias == r.canonical_sku:
            entry['source'] = r.source or ''
        else:
            entry['aliases'].append(r.alias)

    grouped_list = []
    for g in grouped.values():
        aliases_sorted = sorted(g['aliases'])
        g['alias_count'] = len(aliases_sorted)
        g['aliases'] = ', '.join(aliases_sorted)
        last_change = last_dates.get(g['canonical'])
        mod_change = changed_map.get(g['canonical'])
        if pd.isna(mod_change):
            mod_change = None
        if last_change and mod_change:
            g['change_date'] = max(last_change, mod_change)
        else:
            g['change_date'] = mod_change or last_change
        grouped_list.append(g)

    merged_groups = sorted(
        (g for g in grouped_list if g['alias_count'] > 0 and g['type'] != 'unmapped'),
        key=lambda x: x.get('change_date') or datetime.min,
        reverse=True,
    )
    mapped_groups = sorted(
        (g for g in grouped_list if g['type'] != 'unmapped'),
        key=lambda x: x.get('change_date') or datetime.min,
        reverse=True,
    )
    unmapped_groups = sorted(
        (g for g in grouped_list if g['type'] == 'unmapped'),
        key=lambda x: x.get('change_date') or datetime.min,
        reverse=True,
    )

    all_count = len(grouped_list)

    return render_template(
        'sku_map.html',
        grouped=unmapped_groups,
        merged=merged_groups,
        mapped=mapped_groups,
        all_count=all_count,
    )


@app.route('/export-sku-map')
def export_sku_map():
    """Download the SKU mapping as a CSV file."""
    conn = get_db()
    df = pd.read_sql_query(
        'SELECT alias, canonical_sku, type, source, changed_at FROM sku_map',
        conn,
    )
    conn.close()
    output = BytesIO()
    output.write(df.to_csv(index=False).encode('utf-8'))
    output.seek(0)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'sku_map_{timestamp}.csv'
    return send_file(
        output,
        mimetype='text/csv',
        download_name=filename,
        as_attachment=True,
    )


@app.route('/import-sku-map', methods=['POST'])
def import_sku_map():
    """Import SKU mapping from an uploaded CSV or Excel file."""
    file = request.files.get('sku_file')
    if not file or not file.filename:
        flash('No SKU map file provided.')
        return redirect(url_for('settings_page'))
    try:
        if file.filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file)
        else:
            df = pd.read_csv(file)
    except Exception:
        flash('Unable to parse SKU map file.')
        return redirect(url_for('settings_page'))
    required = {'alias', 'canonical_sku', 'type'}
    if not required.issubset(df.columns):
        flash('SKU map file must contain alias, canonical_sku and type columns.')
        return redirect(url_for('settings_page'))
    if 'source' not in df.columns:
        df['source'] = ''
    conn = get_db()
    conn.execute('DELETE FROM sku_map')
    for row in df[['alias', 'canonical_sku', 'type', 'source']].itertuples(index=False):
        alias = str(row.alias).lower().strip()
        canonical = str(row.canonical_sku).lower().strip() or alias
        type_val = str(row.type).lower().strip() or 'unmapped'
        source_val = str(row.source).lower().strip() if hasattr(row, 'source') else ''
        conn.execute(
            'INSERT INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
            (alias, canonical, type_val, source_val, datetime.now(timezone.utc).isoformat()),
        )
    conn.commit()
    conn.close()
    flash('SKU map imported.')
    return redirect(request.referrer or url_for('settings_page'))


@app.route('/update-parent', methods=['POST'])
def update_parent():
    """Change the canonical SKU for an existing entry."""
    data = request.get_json(force=True)
    old_parent = (data.get('alias') or '').lower().strip()
    new_parent = (data.get('parent') or '').lower().strip()
    if not old_parent or not new_parent:
        return jsonify({'status': 'error'}), 400
    conn = get_db()
    rows = conn.execute(
        'SELECT alias, type, source FROM sku_map WHERE canonical_sku=?',
        (old_parent,),
    ).fetchall()
    if not rows:
        conn.close()
        return jsonify({'status': 'error'}), 404
    group_type = rows[0]['type']
    for r in rows:
        conn.execute(
            'REPLACE INTO sku_map(alias, canonical_sku, type, source, changed_at) VALUES(?,?,?,?,?)',
            (r['alias'], new_parent, group_type, r['source'], datetime.now(timezone.utc).isoformat()),
        )
    if old_parent != new_parent:
        conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (old_parent,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/update-type', methods=['POST'])
def update_type():
    """Change the type for an existing canonical SKU."""
    data = request.get_json(force=True)
    canonical = (data.get('canonical') or '').lower().strip()
    new_type = (data.get('type') or 'unmapped').lower().strip()
    if not canonical:
        return jsonify({'status': 'error'}), 400
    conn = get_db()
    conn.execute(
        'UPDATE sku_map SET type=?, changed_at=? WHERE canonical_sku=?',
        (new_type, datetime.now(timezone.utc).isoformat(), canonical),
    )
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

def calculate_report_data(year, month_param=None):
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)
    conn.close()

    all_data = _safe_concat([shopify, qbo], ignore_index=True)

    # ensure numeric totals for reliable aggregation
    all_data['total'] = pd.to_numeric(all_data['total'], errors='coerce').fillna(0)
    all_data["quantity"] = pd.to_numeric(all_data["quantity"], errors="coerce").fillna(0)

    all_data['created_at'] = (
        pd.to_datetime(
            all_data['created_at'].astype(str),
            errors='coerce',
            format='mixed',
            utc=True,
        )
        .dt.tz_localize(None)
    )
    all_data = all_data.dropna(subset=['created_at'])

    alias_map = mapping.set_index('alias')
    def map_row(alias, field):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, field]
            return key if field == 'canonical_sku' else 'unmapped'
        return alias if field == 'canonical_sku' else 'unmapped'

    all_data['canonical'] = all_data['sku'].apply(lambda x: map_row(x, 'canonical_sku'))
    all_data['type'] = all_data['sku'].apply(lambda x: map_row(x, 'type'))
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month'] = all_data['created_at'].dt.strftime('%b')
    all_data['month_num'] = all_data['created_at'].dt.month

    summary = all_data.groupby(['year', 'month', 'month_num']).agg({'total':'sum','quantity':'sum'}).reset_index()
    cutoff_month = datetime.now().month if year == datetime.now().year else 12

    this_year = summary[summary['year'] == year].set_index('month')
    last_year = summary[summary['year'] == year - 1].set_index('month')

    months_list = [
        {'num': i, 'name': m}
        for i, m in enumerate(MONTHS_ORDER, start=1)
    ]
    month_choices = months_list[:cutoff_month]
    rows = []
    for i, month in enumerate(MONTHS_ORDER[:cutoff_month], start=1):
        current = this_year["total"].get(month, 0)
        previous = last_year["total"].get(month, 0)
        pct = "-"
        if previous > 0:
            pct = f"{((current - previous) / previous) * 100:.1f}%"
        elif current > 0:
            pct = "∞"
        pct_sign = current - previous
        rows.append((month, current, previous, pct, pct_sign))

    # aggregate sales by quarter for the overall report
    quarter_rows = []
    for q, months in QUARTER_MAP.items():
        months_in_range = [m for m in months if m <= cutoff_month]
        if not months_in_range:
            continue
        cur_total = summary[
            (summary['year'] == year) & summary['month_num'].isin(months_in_range)
        ]['total'].sum()
        prev_total = summary[
            (summary['year'] == year - 1)
            & summary['month_num'].isin(months_in_range)
        ]['total'].sum()
        pct = "-"
        if prev_total > 0:
            pct = f"{((cur_total - prev_total) / prev_total) * 100:.1f}%"
        elif cur_total > 0:
            pct = "∞"
        pct_sign = cur_total - prev_total
        quarter_rows.append((f"Q{q}", cur_total, prev_total, pct, pct_sign))

    categories = CATEGORIES
    labels = CATEGORY_LABELS

    # Shopify-only monthly totals across all years
    shopify_only = shopify.copy()
    shopify_only['total'] = pd.to_numeric(shopify_only['total'], errors='coerce').fillna(0)
    shopify_only['created_at'] = (
        pd.to_datetime(shopify_only['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    shopify_only = shopify_only.dropna(subset=['created_at'])
    shopify_only['year'] = shopify_only['created_at'].dt.year
    shopify_only['month_num'] = shopify_only['created_at'].dt.month

    shopify_summary = shopify_only.groupby(['year', 'month_num'])['total'].sum().reset_index()
    shopify_years = sorted(shopify_summary['year'].unique(), reverse=True)
    shopify_pivot = (
        shopify_summary.pivot(index='month_num', columns='year', values='total')
        .reindex(range(1, 13))
        .fillna(0)
    )
    shopify_avg = shopify_pivot.replace(0, math.nan).mean(axis=1)
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    shopify_rows = []
    for m in range(1, 13):
        month_name = MONTHS_ORDER[m - 1]
        raw_vals = [float(shopify_pivot.at[m, y]) if y in shopify_pivot.columns else 0.0 for y in shopify_years]
        avg_val = shopify_avg.get(m)
        avg_val = 0.0 if pd.isna(avg_val) else float(avg_val)
        values = []
        for i, y in enumerate(shopify_years):
            val = raw_vals[i]
            if y == current_year and m > current_month:
                values.append({'val': None, 'diff': None})
            else:
                values.append({'val': val, 'diff': val - avg_val})
        shopify_rows.append({'month': month_name, 'values': values, 'avg': avg_val})

    raw_totals = [float(shopify_pivot[y].sum()) if y in shopify_pivot.columns else 0.0 for y in shopify_years]
    nonzero_totals = [t for t in raw_totals if t != 0]
    shopify_avg_total = float(sum(nonzero_totals) / len(nonzero_totals)) if nonzero_totals else 0.0
    shopify_totals = []
    for i, y in enumerate(shopify_years):
        val = raw_totals[i]
        diff = val - shopify_avg_total
        if y == current_year and current_month < 12:
            diff = None
        shopify_totals.append({'val': val, 'diff': diff})

    # Shopify totals by quarter
    shopify_quarters = []
    for q, months in QUARTER_MAP.items():
        vals = []
        for y in shopify_years:
            val = shopify_summary[(shopify_summary['year'] == y) & shopify_summary['month_num'].isin(months)]['total'].sum()
            if y == current_year and max(months) > current_month:
                vals.append(None)
            else:
                vals.append(float(val))
        nonzero = [v for v in vals if v not in (None, 0)]
        avg_val = sum(nonzero) / len(nonzero) if nonzero else 0.0
        values = []
        for i, y in enumerate(shopify_years):
            v = vals[i]
            if v is None:
                values.append({'val': None, 'diff': None})
            else:
                values.append({'val': v, 'diff': v - avg_val})
        shopify_quarters.append({'quarter': f'Q{q}', 'values': values, 'avg': avg_val})




    # yearly summary by type
    summary_type = all_data.groupby(['year', 'month_num', 'type']).agg({'total': 'sum', 'quantity': 'sum'}).reset_index()
    type_rows = []
    for cat in categories:
        cur = summary_type[(summary_type['year'] == year) & (summary_type['type'] == cat)]
        prev = summary_type[
            (summary_type['year'] == year - 1)
            & (summary_type['type'] == cat)
            & (summary_type['month_num'] <= cutoff_month)
        ]
        totals = cur.set_index('month_num').reindex(range(1, cutoff_month + 1), fill_value=0)
        totals = totals['total']
        total_cur = totals.sum()
        total_prev = prev['total'].sum()
        vs_last = "-"
        if total_prev > 0:
            vs_last = f"{((total_cur - total_prev) / total_prev) * 100:.1f}%"
        elif total_cur > 0:
            vs_last = "∞"
        vs_last_sign = total_cur - total_prev
        overall_cat = summary_type[summary_type['type'] == cat]
        avg_month = overall_cat['total'].mean() if len(overall_cat) else 0
        best_month = overall_cat['total'].max() if len(overall_cat) else 0
        avg_qty = overall_cat['quantity'].mean() if len(overall_cat) else 0
        best_qty = overall_cat['quantity'].max() if len(overall_cat) else 0
        type_rows.append({
            'type': labels.get(cat, cat),
            'total': total_cur,
            'vs_last': vs_last,
            'vs_last_sign': vs_last_sign,
            'avg_month': avg_month,
            'best_month': best_month,
            'avg_qty': avg_qty,
            'best_qty': best_qty,
        })

    # last full month summary by type
    now = datetime.now()
    if month_param:
        last_month_year = year
        last_month_num = month_param
    else:
        if year == now.year:
            if now.month == 1:
                last_month_year = year - 1
                last_month_num = 12
            else:
                last_month_year = year
                last_month_num = now.month - 1
        else:
            last_month_year = year
            last_month_num = 12

    last_month_label = datetime(last_month_year, last_month_num, 1).strftime('%b')
    last_start = f"{last_month_year}-{last_month_num:02d}-01"
    last_end = f"{last_month_year}-{last_month_num:02d}-{monthrange(last_month_year, last_month_num)[1]:02d}"

    last_rows = []
    for cat in categories:
        cur_month = summary_type[
            (summary_type['year'] == last_month_year)
            & (summary_type['month_num'] == last_month_num)
            & (summary_type['type'] == cat)
        ]
        prev_month = summary_type[
            (summary_type['year'] == last_month_year - 1)
            & (summary_type['month_num'] == last_month_num)
            & (summary_type['type'] == cat)
        ]
        cur_total = cur_month['total'].sum()
        prev_total = prev_month['total'].sum()
        vs_last = "-"
        if prev_total > 0:
            vs_last = f"{((cur_total - prev_total) / prev_total) * 100:.1f}%"
        elif cur_total > 0:
            vs_last = "∞"
        vs_last_sign = cur_total - prev_total
        prev_same = summary_type[
            (summary_type['type'] == cat)
            & (summary_type['month_num'] == last_month_num)
            & (summary_type['year'] < last_month_year)
        ]
        avg_month = prev_same['total'].mean() if len(prev_same) else 0
        best_month = prev_same['total'].max() if len(prev_same) else 0
        avg_month_sign = cur_total - avg_month
        best_month_sign = cur_total - best_month
        last_rows.append({
            'type': labels.get(cat, cat),
            'total': cur_total,
            'vs_last': vs_last,
            'vs_last_sign': vs_last_sign,
            'avg_month': avg_month,
            'avg_month_sign': avg_month_sign,
            'best_month': best_month,
            'best_month_sign': best_month_sign,
        })

    # overall totals for the last full month
    total_cur = summary_type[
        (summary_type['year'] == last_month_year)
        & (summary_type['month_num'] == last_month_num)
    ].agg({'total': 'sum', 'quantity': 'sum'})
    prev_total_cur = summary_type[
        (summary_type['year'] == last_month_year - 1)
        & (summary_type['month_num'] == last_month_num)
    ]['total'].sum()
    total_val = total_cur['total']
    vs_last = '-'
    if prev_total_cur > 0:
        vs_last = f"{((total_val - prev_total_cur) / prev_total_cur) * 100:.1f}%"
    elif total_val > 0:
        vs_last = '∞'
    prev_months = summary_type[
        (summary_type['month_num'] == last_month_num)
        & (summary_type['year'] < last_month_year)
    ]
    avg_month = prev_months['total'].mean() if len(prev_months) else 0
    best_month = prev_months['total'].max() if len(prev_months) else 0
    last_rows.append({
        'type': 'Total',
        'total': total_val,
        'vs_last': vs_last,
        'vs_last_sign': total_val - prev_total_cur,
        'avg_month': avg_month,
        'avg_month_sign': total_val - avg_month,
        'best_month': best_month,
        'best_month_sign': total_val - best_month,
    })

    # detailed breakdown by SKU for the last full month
    summary_sku = all_data.groupby(
        ['year', 'month_num', 'canonical', 'type']
    ).agg({'total': 'sum', 'quantity': 'sum'}).reset_index()

    sku_details = {}
    for cat in categories:
        cat_rows = []
        cat_df = summary_sku[summary_sku['type'] == cat]
        cat_year = cat_df[(cat_df['year'] == year) & (cat_df['month_num'] <= cutoff_month)]
        cat_last = cat_df[(cat_df['year'] == last_month_year) & (cat_df['month_num'] == last_month_num)]
        cat_prev = cat_df[(cat_df['year'] == last_month_year - 1) & (cat_df['month_num'] == last_month_num)]
        skus = cat_df['canonical'].unique()
        for sku in sorted(skus):
            cur_total_chk = cat_df[(cat_df['canonical'] == sku) & (cat_df['year'] == year)]['total'].sum()
            prev_total_chk = cat_df[(cat_df['canonical'] == sku) & (cat_df['year'] == year - 1)]['total'].sum()
            if cur_total_chk == 0 or prev_total_chk == 0:
                continue
            ydf = cat_year[cat_year['canonical'] == sku]
            overall_sku = cat_df[cat_df['canonical'] == sku]
            ldf = cat_last[cat_last['canonical'] == sku]
            pdf = cat_prev[cat_prev['canonical'] == sku]
            year_total = ydf['total'].sum()
            year_qty = ydf['quantity'].sum()
            month_total = ldf['total'].sum()
            month_qty = ldf['quantity'].sum()
            last_year_total = pdf['total'].sum()
            last_year_sign = month_total - last_year_total
            prev_sku = overall_sku[
                (overall_sku['month_num'] == last_month_num)
                & (overall_sku['year'] < last_month_year)
            ]
            avg_month = prev_sku['total'].mean() if len(prev_sku) else 0
            avg_qty = prev_sku['quantity'].mean() if len(prev_sku) else 0
            best_month = prev_sku['total'].max() if len(prev_sku) else 0
            best_qty = prev_sku['quantity'].max() if len(prev_sku) else 0
            avg_month_sign = month_total - avg_month
            avg_qty_sign = month_qty - avg_qty
            best_month_sign = month_total - best_month
            best_qty_sign = month_qty - best_qty
            cat_rows.append({
                'sku': sku,
                'year_total': year_total,
                'year_qty': year_qty,
                'month_total': month_total,
                'month_qty': month_qty,
                'avg_month': avg_month,
                'avg_month_sign': avg_month_sign,
                'avg_qty': avg_qty,
                'avg_qty_sign': avg_qty_sign,
                'last_year': last_year_total,
                'last_year_sign': last_year_sign,
                'best_month': best_month,
                'best_month_sign': best_month_sign,
                'best_qty': best_qty,
                'best_qty_sign': best_qty_sign,
            })
        sku_details[cat] = cat_rows

    years = sorted(set(summary["year"].unique()).union({year}), reverse=True)
    return {
        'rows': rows,
        'quarter_rows': quarter_rows,
        'selected_year': year,
        'selected_month': last_month_num,
        'years': years,
        'months': month_choices,
        'labels': labels,
        'type_rows': type_rows,
        'last_month_label': last_month_label,
        'last_rows': last_rows,
        'sku_details': sku_details,
        'has_month_details': any(len(v) > 0 for v in sku_details.values()),
        'last_month_year': last_month_year,
        'last_month_num': last_month_num,
        'last_start': last_start,
        'last_end': last_end,
        'shopify_years': shopify_years,
        'shopify_rows': shopify_rows,
        'shopify_totals': shopify_totals,
        'shopify_avg_total': shopify_avg_total,
        'shopify_quarters': shopify_quarters,
    }


def get_year_overall(year):
    """Return month-by-month totals for ``year``."""
    data = calculate_report_data(year)
    return data['rows']


def get_year_summary(year):
    """Return yearly totals by type for ``year``."""
    data = calculate_report_data(year)
    return data['type_rows']


def get_last_month_summary(year, month=None):
    """Return summary by type for the last full month."""
    data = calculate_report_data(year, month)
    return {
        'label': data['last_month_label'],
        'rows': data['last_rows'],
    }


def get_last_month_details(year, month=None):
    """Return detailed SKU breakdown for the last full month."""
    data = calculate_report_data(year, month)
    return {
        'label': data['last_month_label'],
        'sku_details': data['sku_details'],
    }


def get_shopify_monthly():
    """Return Shopify monthly totals across years respecting the year limit."""
    data = calculate_report_data(datetime.now().year)
    year_limit = int(get_setting('reports_year_limit', '5') or 5)
    years = data['shopify_years'][:year_limit]
    rows = []
    for r in data['shopify_rows']:
        values = r['values'][:len(years)]
        nonzero = [v['val'] for v in values if v['val'] != 0]
        avg = sum(nonzero) / len(nonzero) if nonzero else 0.0
        rows.append({
            'month': r['month'],
            'values': [{
                'val': v['val'],
                'diff': v['val'] - avg
            } for v in values],
            'avg': avg,
        })
    totals = data['shopify_totals'][:len(years)]
    total_vals = [t['val'] for t in totals if t['val'] != 0]
    avg_total = sum(total_vals) / len(total_vals) if total_vals else 0.0
    totals = [{
        'val': t['val'],
        'diff': t['val'] - avg_total
    } for t in totals]
    return {
        'years': years,
        'rows': rows,
        'totals': totals,
        'average_total': avg_total,
    }


def get_shopify_quarterly():
    """Return Shopify quarterly totals across years respecting the year limit."""
    data = calculate_report_data(datetime.now().year)
    year_limit = int(get_setting('reports_year_limit', '5') or 5)
    years = data['shopify_years'][:year_limit]
    rows = []
    for r in data['shopify_quarters']:
        values = r['values'][:len(years)]
        nonzero = [v['val'] for v in values if v['val'] != 0]
        avg = sum(nonzero) / len(nonzero) if nonzero else 0.0
        rows.append({
            'quarter': r['quarter'],
            'values': [{
                'val': v['val'],
                'diff': v['val'] - avg
            } for v in values],
            'avg': avg,
        })
    return {
        'years': years,
        'rows': rows,
    }


def generate_year_chart_base64(year, *, light=False):
    """Return base64 PNG for the year-over-year monthly sales chart.

    Parameters
    ----------
    year : int
        Year to generate chart for.
    light : bool, optional
        Use light mode chart styling regardless of theme.
    """
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    conn.close()

    all_data = _safe_concat([shopify, qbo])
    all_data["quantity"] = pd.to_numeric(all_data["quantity"], errors="coerce").fillna(0)
    all_data["total"] = pd.to_numeric(all_data["total"], errors="coerce").fillna(0)
    all_data['created_at'] = (
        pd.to_datetime(all_data['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    all_data = all_data.dropna(subset=['created_at'])
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month'] = all_data['created_at'].dt.strftime('%b')

    summary = all_data.groupby(['year', 'month'])['total'].sum().reset_index()
    this_year = summary[summary['year'] == year].set_index('month')
    last_year = summary[summary['year'] == year - 1].set_index('month')

    y1 = [this_year['total'].get(m, 0) for m in MONTHS_ORDER]
    y2 = [last_year['total'].get(m, 0) for m in MONTHS_ORDER]

    style = 'default' if light else _chart_style()
    with plt.style.context(style):
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(MONTHS_ORDER, y1, label=str(year), marker='o')
        ax.plot(MONTHS_ORDER, y2, label=str(year-1), linestyle='--', marker='x')
        ax.set_title('Monthly Sales Comparison')
        ax.set_ylabel('Total Sales ($)')
        ax.legend()
        ax.grid(True)

        output = BytesIO()
        fig.tight_layout()
        plt.savefig(output, format='png')
        plt.close(fig)
        output.seek(0)
    return base64.b64encode(output.read()).decode('utf-8')


def generate_last_month_chart_base64(year, month_param=None, *, light=False):
    """Return base64 PNG for the last-month sales by type bar chart.

    Parameters
    ----------
    year : int
        Year to generate chart for.
    month_param : int, optional
        Explicit month to use instead of current month.
    light : bool, optional
        Use light mode chart styling regardless of theme.
    """
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)
    conn.close()

    all_data = _safe_concat([shopify, qbo], ignore_index=True)
    all_data['total'] = pd.to_numeric(all_data['total'], errors='coerce').fillna(0)
    all_data['created_at'] = (
        pd.to_datetime(all_data['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    all_data = all_data.dropna(subset=['created_at'])

    alias_map = mapping.set_index('alias')

    def map_row(alias, field):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, field]
            return key if field == 'canonical_sku' else 'unmapped'
        return alias if field == 'canonical_sku' else 'unmapped'

    all_data['type'] = all_data['sku'].apply(lambda x: map_row(x, 'type'))
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month_num'] = all_data['created_at'].dt.month

    now = datetime.now()
    if month_param:
        last_year = year
        last_month = month_param
    else:
        if year == now.year:
            if now.month == 1:
                last_year = year - 1
                last_month = 12
            else:
                last_year = year
                last_month = now.month - 1
        else:
            last_year = year
            last_month = 12

    summary = (
        all_data.groupby(['year', 'month_num', 'type'])['total']
        .sum()
        .reset_index()
    )

    cur = summary[(summary['year'] == last_year) & (summary['month_num'] == last_month)].set_index('type')
    prev = summary[(summary['year'] == last_year - 1) & (summary['month_num'] == last_month)].set_index('type')

    categories = CATEGORIES
    labels = CATEGORY_LABELS

    y1 = [cur['total'].get(cat, 0) for cat in categories]
    y2 = [prev['total'].get(cat, 0) for cat in categories]
    xlabels = [labels.get(cat, cat) for cat in categories]

    style = 'default' if light else _chart_style()
    with plt.style.context(style):
        fig, ax = plt.subplots(figsize=(10, 4))
        idx = range(len(categories))
        width = 0.35
        ax.bar([i - width / 2 for i in idx], y1, width=width, label=f'{last_year}-{last_month:02d}')
        ax.bar([i + width / 2 for i in idx], y2, width=width, label=f'{last_year - 1}-{last_month:02d}')
        ax.set_xticks(list(idx))
        ax.set_xticklabels(xlabels, rotation=30, ha='right')
        ax.set_ylabel('Total Sales ($)')
        ax.set_title('Last Month Sales by Type')
        ax.legend()
        ax.grid(axis='y')

        output = BytesIO()
        fig.tight_layout()
        plt.savefig(output, format='png')
        plt.close(fig)
        output.seek(0)
    return base64.b64encode(output.read()).decode('utf-8')


@app.route('/monthly-report')
def monthly_report():
    year = request.args.get('year', default=datetime.now().year, type=int)
    month_param = request.args.get('month', type=int)
    data = calculate_report_data(year, month_param)
    year_limit = int(get_setting('reports_year_limit', '5') or 5)
    data['years'] = sorted(data['years'], reverse=True)[:year_limit]
    if year not in data['years']:
        data['years'].append(year)
        data['years'] = sorted(data['years'], reverse=True)
    data['shopify_years'] = data['shopify_years'][:year_limit]
    for row in data['shopify_rows']:
        row['values'] = row['values'][:len(data['shopify_years'])]
    data['shopify_totals'] = data['shopify_totals'][:len(data['shopify_years'])]
    for row in data['shopify_quarters']:
        row['values'] = row['values'][:len(data['shopify_years'])]
    data['default_tab'] = get_setting('reports_start_tab', 'by-month')
    data['categories'] = CATEGORIES
    return render_template('report.html', **data)


@app.route('/export-report', methods=['GET', 'POST'])
def export_report():
    values = request.values
    if request.method == 'POST' or request.args:
        year = int(values.get('year', datetime.now().year))
        month_val = values.get('month') or get_setting('default_export_month', '')
        month = int(month_val) if month_val and str(month_val).isdigit() else None

        def _check(name, setting):
            val = values.get(name)
            if val is None:
                return get_setting(setting, '1') == '1'
            return str(val).lower() in {'on', '1', 'true', 'yes'}

        include_month_summary = _check('include_month_summary', 'default_include_month_summary')
        include_month_details = _check('include_month_details', 'default_include_month_details')
        include_year_overall = _check('include_year_overall', 'default_include_year_overall')
        include_year_summary = _check('include_year_summary', 'default_include_year_summary')
        include_shopify = _check('include_shopify', 'default_include_shopify')
        detail_types = values.getlist('detail_types')
        if len(detail_types) == 1:
            detail_types = [t.strip() for t in detail_types[0].split(',') if t.strip()]
        if not detail_types:
            default_types = get_setting('default_detail_types', ','.join(CATEGORIES))
            detail_types = [t for t in default_types.split(',') if t]
        data = calculate_report_data(year, month)
        year_limit = int(get_setting('reports_year_limit', '5') or 5)
        data['years'] = sorted(data['years'], reverse=True)[:year_limit]
        if year not in data['years']:
            data['years'].append(year)
            data['years'] = sorted(data['years'], reverse=True)
        data['shopify_years'] = data['shopify_years'][:year_limit]
        for row in data['shopify_rows']:
            row['values'] = row['values'][:len(data['shopify_years'])]
        data['shopify_totals'] = data['shopify_totals'][:len(data['shopify_years'])]
        for row in data['shopify_quarters']:
            row['values'] = row['values'][:len(data['shopify_years'])]
        selected = [t for t in detail_types if t in CATEGORIES]
        data['sku_details'] = {t: data['sku_details'].get(t, []) for t in selected}
        data['has_month_details'] = any(len(v) > 0 for v in data['sku_details'].values())
        data.update({
            'include_month_summary': include_month_summary,
            'include_month_details': include_month_details,
            'include_year_overall': include_year_overall,
            'include_year_summary': include_year_summary,
            'include_shopify': include_shopify,
            'detail_types': selected,
            'branding': get_setting('branding', ''),
            'report_title': get_setting('report_title', ''),
            'branding_logo_url': url_for('branding_logo', _external=True),
            'logo_size': LOGO_SIZE,
            'primary_color': get_setting('branding_primary', ''),
            'highlight_color': get_setting('branding_highlight', ''),
            'year_chart': generate_year_chart_base64(year, light=True) if include_year_overall else '',
            'last_month_chart': generate_last_month_chart_base64(year, month, light=True) if include_month_summary else '',
        })
        html = render_template('report_pdf.html', **data, datetime=datetime)
        output = BytesIO()
        pisa.CreatePDF(html, dest=output, link_callback=fetch_resources)
        output.seek(0)
        return send_file(output, download_name='report.pdf', mimetype='application/pdf')

    abort(404)



@app.route('/report-chart')
def report_chart():
    year = request.args.get('year', default=datetime.now().year, type=int)
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    conn.close()

    all_data = _safe_concat([shopify, qbo])
    all_data["quantity"] = pd.to_numeric(all_data["quantity"], errors="coerce").fillna(0)
    all_data["total"] = pd.to_numeric(all_data["total"], errors="coerce").fillna(0)
    all_data['created_at'] = (
        pd.to_datetime(all_data['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    all_data = all_data.dropna(subset=['created_at'])
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month'] = all_data['created_at'].dt.strftime('%b')

    summary = all_data.groupby(['year', 'month'])['total'].sum().reset_index()
    this_year = summary[summary['year'] == year].set_index('month')
    last_year = summary[summary['year'] == year - 1].set_index('month')

    y1 = [this_year['total'].get(m, 0) for m in MONTHS_ORDER]
    y2 = [last_year['total'].get(m, 0) for m in MONTHS_ORDER]

    with plt.style.context(_chart_style()):
        fig, ax = plt.subplots(figsize=(10,4))
        ax.plot(MONTHS_ORDER, y1, label=str(year), marker='o')
        ax.plot(MONTHS_ORDER, y2, label=str(year-1), linestyle='--', marker='x')
        ax.set_title('Monthly Sales Comparison')
        ax.set_ylabel('Total Sales ($)')
        ax.legend()
        ax.grid(True)

        output = BytesIO()
        fig.tight_layout()
        plt.savefig(output, format='png')
        plt.close(fig)
        output.seek(0)
    return send_file(output, mimetype='image/png')


@app.route('/transactions')
def transactions_page():
    """Show transactions and totals for a SKU across uploads."""
    sku = request.args.get('sku', '').lower().strip()
    source = request.args.get('source', '').lower()
    period = request.args.get('period', '')
    start = request.args.get('start')
    end = request.args.get('end')
    tx_source_default = get_setting('transactions_default_source', 'both')
    tx_period_default = get_setting('transactions_default_period', 'last30')
    if not source:
        source = tx_source_default
    if start in (None, '', 'None'):
        start = None
    if end in (None, '', 'None'):
        end = None
    conn = get_db()
    shopify = pd.read_sql_query(
        'SELECT created_at, sku, description, quantity, price, total FROM shopify',
        conn,
    )
    qbo = pd.read_sql_query(
        'SELECT created_at, sku, description, quantity, price, total FROM qbo',
        conn,
    )
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)

    alias_map = mapping.set_index('alias')
    sku_options = sorted(
        mapping[
            (mapping['alias'] == mapping['canonical_sku'])
            & (mapping['type'] != 'unmapped')
        ][
            'canonical_sku'
        ].unique()
    )

    def parse_dates(df):
        df['created_at'] = (
            pd.to_datetime(df['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
            .dt.tz_localize(None)
        )
        df.dropna(subset=['created_at'], inplace=True)
        return df

    shopify = parse_dates(shopify)
    qbo = parse_dates(qbo)

    all_dates = _safe_concat([shopify[['created_at']], qbo[['created_at']]])['created_at'].dropna()
    years = sorted(all_dates.dt.year.dropna().unique(), reverse=True)
    month_periods = sorted(all_dates.dt.to_period('M').unique(), reverse=True)
    month_options = [
        {
            'value': f"month-{p.year}-{p.month:02d}",
            'label': p.strftime('%b %Y'),
        }
        for p in month_periods
    ]
    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year
    quarter_options = []
    for i in range(4):
        q = current_quarter - i
        y = current_year
        if q <= 0:
            q += 4
            y -= 1
        quarter_options.append({'value': f"quarter-{y}-Q{q}", 'label': f"Q{q} {y}"})

    period_type = ''
    period_year_val = ''
    period_month_val = ''

    if period.startswith('year-'):
        period_type = 'year'
        period_year_val = period.split('-')[1]
    elif period.startswith('month-'):
        period_type = 'month'
        _y, _m = period.split('-')[1:]
        period_year_val = _y
        period_month_val = f"{_y}-{_m}"
    elif period == 'last30':
        period_type = 'last30'
    elif period == 'custom' or start or end:
        period_type = 'custom'

    def canonical(alias):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, 'canonical_sku']
            return key
        return alias

    if not start and not end:
        if period.startswith('year-'):
            year_num = int(period.split('-')[1])
            start = f"{year_num}-01-01"
            end = f"{year_num}-12-31"
        elif period.startswith('month-'):
            year_num, month_num = map(int, period.split('-')[1:])
            start = f"{year_num}-{month_num:02d}-01"
            end = f"{year_num}-{month_num:02d}-{monthrange(year_num, month_num)[1]:02d}"
        elif not period:
            if tx_period_default == 'last30':
                end_dt_def = datetime.now().date()
                start_dt_def = end_dt_def - timedelta(days=29)
                start = start_dt_def.isoformat()
                end = end_dt_def.isoformat()
                period = 'last30'
            else:
                period = ''
        elif period != 'all':
            end_dt_def = datetime.now().date()
            start_dt_def = end_dt_def - timedelta(days=29)
            start = start_dt_def.isoformat()
            end = end_dt_def.isoformat()
            period = 'last30'

    start_dt = pd.to_datetime(start) if start else None
    end_dt = pd.to_datetime(end) if end else None

    def process(df):
        df = df.copy()
        df['canonical'] = df['sku'].apply(canonical)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
        if start_dt is not None:
            df = df[df['created_at'] >= start_dt]
        if end_dt is not None:
            df = df[df['created_at'] <= end_dt]
        return df

    shopify = process(shopify)
    qbo = process(qbo)

    if sku:
        shopify = shopify[shopify['canonical'] == sku]
        qbo = qbo[qbo['canonical'] == sku]
    else:
        mapped = set(sku_options)
        shopify = shopify[shopify['canonical'].isin(mapped)]
        qbo = qbo[qbo['canonical'].isin(mapped)]

    show_shopify = source in ('both', 'shopify')
    show_qbo = source in ('both', 'qbo')

    summary = {
        'shopify': {
            'quantity': shopify['quantity'].sum(),
            'total': shopify['total'].sum(),
        },
        'qbo': {
            'quantity': qbo['quantity'].sum(),
            'total': qbo['total'].sum(),
        },
    }

    frames = []
    if show_shopify:
        frames.append(shopify.assign(source_title='Shopify'))
    if show_qbo:
        frames.append(qbo.assign(source_title='QBO'))
    if frames:
        df_all = _safe_concat(frames, ignore_index=True).sort_values('created_at')
    else:
        df_all = pd.DataFrame(columns=shopify.columns.tolist() + ['source_title'])

    dup_all = _find_duplicates(conn, sku=sku or None, start=start_dt, end=end_dt)
    duplicates = [d for d in dup_all if not d.get('ignored')]
    ignored_dups = [d for d in dup_all if d.get('ignored')]

    params = []
    clauses = []
    if sku:
        clauses.append('sku=?')
        params.append(sku)
    if start_dt is not None:
        clauses.append('created_at >= ?')
        params.append(start_dt.isoformat(sep=' ', timespec='seconds'))
    if end_dt is not None:
        clauses.append('created_at <= ?')
        params.append(end_dt.isoformat(sep=' ', timespec='seconds'))
    query = (
        'SELECT resolved_at, created_at, shopify_created_at, qbo_created_at, sku, '
        'shopify_sku, qbo_sku, shopify_desc, qbo_desc, quantity, total, action, shopify_id, qbo_id, ignored '
        'FROM duplicate_log WHERE action!="unmatched" AND ignored=0'
    )
    if clauses:
        query += ' AND ' + ' AND '.join(clauses)
    query += ' ORDER BY resolved_at DESC LIMIT 20'
    resolved_dups = pd.read_sql_query(query, conn, params=params).to_dict('records')
    dup_action = get_setting('duplicate_action', 'review')
    conn.close()

    return render_template(
        'transactions.html',
        sku=sku,
        rows=df_all.itertuples(),
        summary=summary,
        show_shopify=show_shopify,
        show_qbo=show_qbo,
        source=source,
        sku_options=sku_options,
        start=start,
        end=end,
        years=years,
        month_options=month_options,
        quarter_options=quarter_options,
        period=period,
        period_type=period_type,
        period_year=period_year_val,
        period_month=period_month_val,
        duplicates=duplicates,
        resolved_duplicates=resolved_dups,
        ignored_duplicates=ignored_dups,
        dup_action=dup_action,
    )


@app.route('/last-month-chart')
def last_month_chart():
    year = request.args.get('year', default=datetime.now().year, type=int)
    month_param = request.args.get('month', type=int)
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)
    conn.close()

    all_data = _safe_concat([shopify, qbo], ignore_index=True)
    all_data['total'] = pd.to_numeric(all_data['total'], errors='coerce').fillna(0)
    all_data['created_at'] = (
        pd.to_datetime(all_data['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    all_data = all_data.dropna(subset=['created_at'])

    alias_map = mapping.set_index('alias')

    def map_row(alias, field):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, field]
            return key if field == 'canonical_sku' else 'unmapped'
        return alias if field == 'canonical_sku' else 'unmapped'

    all_data['type'] = all_data['sku'].apply(lambda x: map_row(x, 'type'))
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month_num'] = all_data['created_at'].dt.month

    now = datetime.now()
    if month_param:
        last_year = year
        last_month = month_param
    else:
        if year == now.year:
            if now.month == 1:
                last_year = year - 1
                last_month = 12
            else:
                last_year = year
                last_month = now.month - 1
        else:
            last_year = year
            last_month = 12

    summary = (
        all_data.groupby(['year', 'month_num', 'type'])['total']
        .sum()
        .reset_index()
    )

    categories = CATEGORIES
    labels = CATEGORY_LABELS

    cur = summary[(summary['year'] == last_year) & (summary['month_num'] == last_month)].set_index('type')
    prev = summary[(summary['year'] == last_year - 1) & (summary['month_num'] == last_month)].set_index('type')

    y1 = [cur['total'].get(cat, 0) for cat in categories]
    y2 = [prev['total'].get(cat, 0) for cat in categories]
    xlabels = [labels.get(cat, cat) for cat in categories]

    with plt.style.context(_chart_style()):
        fig, ax = plt.subplots(figsize=(10, 4))
        idx = range(len(categories))
        width = 0.35
        ax.bar([i - width / 2 for i in idx], y1, width=width, label=f'{last_year}-{last_month:02d}')
        ax.bar([i + width / 2 for i in idx], y2, width=width, label=f'{last_year - 1}-{last_month:02d}')
        ax.set_xticks(list(idx))
        ax.set_xticklabels(xlabels, rotation=30, ha='right')
        ax.set_ylabel('Total Sales ($)')
        ax.set_title('Last Month Sales by Type')
        ax.legend()
        ax.grid(axis='y')

        output = BytesIO()
        fig.tight_layout()
        plt.savefig(output, format='png')
        plt.close(fig)
        output.seek(0)
    return send_file(output, mimetype='image/png')


@app.route('/sku/<sku>')
def sku_detail(sku):
    """Display total quantity and sales for a SKU broken down by source."""
    conn = get_db()
    shopify = pd.read_sql_query(
        'SELECT created_at, sku, quantity, total FROM shopify', conn
    )
    qbo = pd.read_sql_query(
        'SELECT created_at, sku, quantity, total FROM qbo', conn
    )
    mapping = pd.read_sql_query('SELECT alias, canonical_sku FROM sku_map', conn)
    conn.close()

    alias_map = mapping.set_index('alias')

    def canonical(alias):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, 'canonical_sku']
            return key
        return alias

    for df in (shopify, qbo):
        df['canonical'] = df['sku'].apply(canonical)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)

    s_df = shopify[shopify['canonical'] == sku]
    q_df = qbo[qbo['canonical'] == sku]

    summary = {
        'shopify': {
            'quantity': s_df['quantity'].sum(),
            'total': s_df['total'].sum(),
        },
        'qbo': {
            'quantity': q_df['quantity'].sum(),
            'total': q_df['total'].sum(),
        },
    }
    return render_template('sku_summary.html', sku=sku, summary=summary)


@app.route('/sku/<sku>/<source>')
def sku_transactions(sku, source):
    """List individual transactions for a SKU from the specified source."""
    if source not in ('shopify', 'qbo'):
        return abort(404)
    conn = get_db()
    df = pd.read_sql_query(
        f'SELECT created_at, sku, description, price, quantity, total FROM {source}',
        conn,
    )
    mapping = pd.read_sql_query('SELECT alias, canonical_sku FROM sku_map', conn)
    conn.close()

    alias_map = mapping.set_index('alias')

    def canonical(alias):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in alias_map.index:
                return alias_map.loc[key, 'canonical_sku']
            return key
        return alias

    df['canonical'] = df['sku'].apply(canonical)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
    df['created_at'] = (
        pd.to_datetime(df['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    df = df[df['canonical'] == sku].dropna(subset=['created_at'])

    years = sorted(df['created_at'].dt.year.dropna().unique(), reverse=True)
    months = [
        {'num': i, 'name': name}
        for i, name in enumerate(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], start=1)
    ]

    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    if year:
        df = df[df['created_at'].dt.year == year]
    if month:
        df = df[df['created_at'].dt.month == month]

    df = df.sort_values('created_at')

    total_qty = df['quantity'].sum()
    total_amount = df['total'].sum()

    return render_template(
        'sku_transactions.html',
        sku=sku,
        source=source,
        source_title='Shopify' if source == 'shopify' else 'QBO',
        rows=df.itertuples(),
        years=years,
        months=months,
        selected_year=year,
        selected_month=month,
        total_qty=total_qty,
        total_amount=total_amount,
    )


@app.route('/resolve-duplicate', methods=['POST'])
def resolve_duplicate():
    action = request.form.get('action', 'both')
    sid = request.form.get('shopify_id', type=int)
    qid = request.form.get('qbo_id', type=int)
    if sid is None or qid is None:
        return jsonify(success=False), 400
    conn = get_db()
    row = conn.execute(
        'SELECT ignored FROM duplicate_log WHERE shopify_id=? AND qbo_id=? ORDER BY resolved_at DESC LIMIT 1',
        (sid, qid),
    ).fetchone()
    if row and row['ignored']:
        conn.close()
        return jsonify(success=False), 400
    _resolve_duplicate_pair(
        conn,
        sid,
        qid,
        action if action in {'shopify', 'qbo', 'both'} else 'both',
    )
    conn.commit()
    conn.close()
    return jsonify(success=True)


@app.route('/unmatch-duplicate', methods=['POST'])
def unmatch_duplicate():
    """Reopen a resolved duplicate for manual review."""
    sid = request.form.get('shopify_id', type=int)
    qid = request.form.get('qbo_id', type=int)
    if sid is None or qid is None:
        return jsonify(success=False), 400
    conn = get_db()
    row = conn.execute(
        'SELECT action, sku, quantity, total, shopify_desc, qbo_desc, '
        'shopify_created_at, qbo_created_at FROM duplicate_log '
        'WHERE shopify_id=? AND qbo_id=? ORDER BY resolved_at DESC LIMIT 1',
        (sid, qid),
    ).fetchone()
    new_sid, new_qid = sid, qid
    if row:
        price = row['total'] / row['quantity'] if row['quantity'] else 0
        if row['action'] == 'shopify':
            cur = conn.execute(
                'INSERT INTO qbo(created_at, sku, description, quantity, price, total) '
                'VALUES(?,?,?,?,?,?)',
                (
                    row['qbo_created_at'],
                    row['sku'],
                    row['qbo_desc'],
                    row['quantity'],
                    price,
                    row['total'],
                ),
            )
            new_qid = cur.lastrowid
        elif row['action'] == 'qbo':
            cur = conn.execute(
                'INSERT INTO shopify(created_at, sku, description, quantity, price, total) '
                'VALUES(?,?,?,?,?,?)',
                (
                    row['shopify_created_at'],
                    row['sku'],
                    row['shopify_desc'],
                    row['quantity'],
                    price,
                    row['total'],
                ),
            )
            new_sid = cur.lastrowid
    conn.execute(
        'UPDATE duplicate_log SET action="unmatched", ignored=0, shopify_id=?, qbo_id=? '
        'WHERE shopify_id=? AND qbo_id=?',
        (new_sid, new_qid, sid, qid),
    )
    conn.commit()
    conn.close()
    return jsonify(success=True)


@app.route('/ignore-duplicate', methods=['POST'])
def ignore_duplicate():
    """Mark a duplicate pair as ignored."""
    sid = request.form.get('shopify_id', type=int)
    qid = request.form.get('qbo_id', type=int)
    if sid is None or qid is None:
        return jsonify(success=False), 400
    conn = get_db()
    cur = conn.execute(
        'UPDATE duplicate_log SET ignored=1 WHERE shopify_id=? AND qbo_id=?',
        (sid, qid),
    )
    if cur.rowcount == 0:
        pairs = _find_duplicates(conn)
        row = next((p for p in pairs if p['shopify_id'] == sid and p['qbo_id'] == qid), None)
        if row:
            conn.execute(
                "INSERT INTO duplicate_log(resolved_at, shopify_id, qbo_id, action, sku, shopify_sku, qbo_sku, quantity, total, shopify_desc, qbo_desc, created_at, shopify_created_at, qbo_created_at, ignored) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    sid,
                    qid,
                    'both',
                    row['sku'],
                    row.get('shopify_sku'),
                    row.get('qbo_sku'),
                    row['quantity'],
                    row['total'],
                    row['shopify_desc'],
                    row['qbo_desc'],
                    row['created_at'],
                    row['shopify_created_at'],
                    row['qbo_created_at'],
                ),
            )
        else:
            conn.execute(
                'INSERT INTO duplicate_log(resolved_at, shopify_id, qbo_id, action, ignored) VALUES (?,?,?,?,1)',
                (datetime.now(timezone.utc).isoformat(), sid, qid, 'both'),
            )
    conn.commit()
    conn.close()
    return jsonify(success=True)


@app.route('/unignore-duplicate', methods=['POST'])
def unignore_duplicate():
    """Remove ignored status from a duplicate pair."""
    sid = request.form.get('shopify_id', type=int)
    qid = request.form.get('qbo_id', type=int)
    if sid is None or qid is None:
        return jsonify(success=False), 400
    conn = get_db()
    conn.execute(
        'UPDATE duplicate_log SET ignored=0 WHERE shopify_id=? AND qbo_id=?',
        (sid, qid),
    )
    conn.commit()
    conn.close()
    return jsonify(success=True)



@app.route('/settings', methods=['GET', 'POST'])
def settings_page():
    if request.method == 'POST':
        # Appearance settings
        theme_primary = request.form.get('theme_primary', '').strip()
        theme_highlight = request.form.get('theme_highlight', '').strip()
        theme_background = request.form.get('theme_background', '').strip()
        theme_text = request.form.get('theme_text', '').strip()
        set_setting('theme_primary', theme_primary)
        set_setting('theme_highlight', theme_highlight)
        set_setting('theme_background', theme_background)
        set_setting('theme_text', theme_text)

        primary_color = request.form.get('primary_color', '').strip()
        highlight_color = request.form.get('highlight_color', '').strip()
        set_setting('branding_primary', primary_color)
        set_setting('branding_highlight', highlight_color)
        app_title = request.form.get('app_title', '').strip()
        report_title = request.form.get('report_title', '').strip()
        branding = request.form.get('branding', '').strip()
        set_setting('app_title', app_title)
        set_setting('report_title', report_title)
        set_setting('branding', branding)
        include_month_summary = 'include_month_summary' in request.form
        include_month_details = 'include_month_details' in request.form
        include_year_overall = 'include_year_overall' in request.form
        include_year_summary = 'include_year_summary' in request.form
        include_shopify = 'include_shopify' in request.form
        reports_start_tab = request.form.get('reports_start_tab', 'by-month')
        year_limit_val = request.form.get('reports_year_limit', '5')
        try:
            year_limit = int(year_limit_val)
        except ValueError:
            year_limit = 5
        prev_dup_action = get_setting('duplicate_action', 'review')
        dup_action = request.form.get('dup_action', 'review')
        tx_source_default = request.form.get('tx_source_default', 'both')
        tx_period_default = request.form.get('tx_period_default', 'last30')
        detail_types = request.form.getlist('detail_types')
        set_setting('default_detail_types', ','.join(detail_types))
        set_setting('default_include_month_summary', '1' if include_month_summary else '0')
        set_setting('default_include_month_details', '1' if include_month_details else '0')
        set_setting('default_include_year_overall', '1' if include_year_overall else '0')
        set_setting('default_include_year_summary', '1' if include_year_summary else '0')
        set_setting('default_include_shopify', '1' if include_shopify else '0')
        set_setting('reports_start_tab', reports_start_tab)
        set_setting('reports_year_limit', str(max(1, year_limit)))
        shopify_domain = request.form.get('shopify_domain', '').strip()
        shopify_token = request.form.get('shopify_token', '').strip()
        set_setting('shopify_domain', shopify_domain)
        set_setting('shopify_token', shopify_token)
        qbo_client_id = request.form.get('qbo_client_id', '').strip()
        qbo_client_secret = request.form.get('qbo_client_secret', '').strip()
        qbo_refresh_token = request.form.get('qbo_refresh_token', '').strip()
        qbo_realm_id = request.form.get('qbo_realm_id', '').strip()
        hubspot_token = request.form.get('hubspot_token', '').strip()
        set_setting('qbo_client_id', qbo_client_id)
        set_setting('qbo_client_secret', qbo_client_secret)
        set_setting('qbo_refresh_token', qbo_refresh_token)
        set_setting('qbo_realm_id', qbo_realm_id)
        set_setting('hubspot_token', hubspot_token)
        default_month = request.form.get('default_month', '')
        set_setting('default_export_month', default_month)
        set_setting('duplicate_action', dup_action)
        set_setting('transactions_default_source', tx_source_default)
        set_setting('transactions_default_period', tx_period_default)
        if dup_action in {'shopify', 'qbo', 'both'} and dup_action != prev_dup_action:
            conn = get_db()
            _resolve_duplicates(conn, dup_action)
            conn.commit()
            conn.close()
        logo_file = request.files.get('logo')
        if logo_file and logo_file.filename:
            filename = secure_filename(logo_file.filename)
            ext = os.path.splitext(filename)[1].lower()
            if ext in {'.png', '.jpg', '.jpeg', '.gif'}:
                save_name = f'branding_logo{ext}'
                path = os.path.join(UPLOAD_FOLDER, save_name)
                logo_file.save(path)
                set_setting('branding_logo', os.path.join(UPLOAD_FOLDER, save_name))
        app_logo_file = request.files.get('app_logo')
        if app_logo_file and app_logo_file.filename:
            filename = secure_filename(app_logo_file.filename)
            ext = os.path.splitext(filename)[1].lower()
            if ext in {'.png', '.jpg', '.jpeg', '.gif'}:
                save_name = f'app_logo{ext}'
                path = os.path.join(UPLOAD_FOLDER, save_name)
                app_logo_file.save(path)
                set_setting('app_logo', os.path.join(UPLOAD_FOLDER, save_name))
        flash('Settings saved.')
        return redirect(url_for('settings_page'))
    primary_color = get_setting('branding_primary', DEFAULT_THEME_PRIMARY)
    highlight_color = get_setting('branding_highlight', DEFAULT_THEME_HIGHLIGHT)
    app_title = get_setting('app_title', 'ultrasuite')
    report_title = get_setting('report_title', 'Monthly Report')
    branding = get_setting('branding', '')
    theme_primary = get_setting('theme_primary', DEFAULT_THEME_PRIMARY)
    theme_highlight = get_setting('theme_highlight', DEFAULT_THEME_HIGHLIGHT)
    theme_background = get_setting('theme_background', DEFAULT_THEME_BACKGROUND)
    theme_text = get_setting('theme_text', DEFAULT_THEME_TEXT)
    active_theme = ''
    for name, vals in THEMES.items():
        if (
            theme_primary == vals['primary'] and
            theme_highlight == vals['highlight'] and
            theme_background == vals['background'] and
            theme_text == vals['text']
        ):
            active_theme = name
            break
    include_month_summary = get_setting('default_include_month_summary', '1') == '1'
    include_month_details = get_setting('default_include_month_details', '1') == '1'
    include_year_overall = get_setting('default_include_year_overall', '1') == '1'
    include_year_summary = get_setting('default_include_year_summary', '1') == '1'
    include_shopify = get_setting('default_include_shopify', '1') == '1'
    reports_start_tab = get_setting('reports_start_tab', 'by-month')
    year_limit = int(get_setting('reports_year_limit', '5') or 5)
    dup_action = get_setting('duplicate_action', 'review')
    tx_source_default = get_setting('transactions_default_source', 'both')
    tx_period_default = get_setting('transactions_default_period', 'last30')
    shopify_domain = get_setting('shopify_domain', '')
    shopify_token = get_setting('shopify_token', '')
    shopify_last_sync = get_setting('shopify_last_sync', '')
    qbo_last_sync = get_setting('qbo_last_sync', '')
    hubspot_last_sync = get_setting('hubspot_last_sync', '')
    qbo_client_id = get_setting('qbo_client_id', '')
    qbo_client_secret = get_setting('qbo_client_secret', '')
    qbo_refresh_token = get_setting('qbo_refresh_token', '')
    qbo_realm_id = get_setting('qbo_realm_id', '')
    hubspot_token = get_setting('hubspot_token', '')
    types_default = get_setting('default_detail_types', ','.join(CATEGORIES))
    detail_types = [t for t in types_default.split(',') if t]
    detail_types_all = len(detail_types) == len(CATEGORIES)
    logo_path = get_setting('branding_logo', '')
    app_logo_path = get_setting('app_logo', '')
    month_default = get_setting('default_export_month', '')
    month_int = int(month_default) if str(month_default).isdigit() else None
    months = calculate_report_data(datetime.now().year)['months']
    return render_template(
        'settings.html',
        primary_color=primary_color,
        highlight_color=highlight_color,
        include_month_summary=include_month_summary,
        include_month_details=include_month_details,
        include_year_overall=include_year_overall,
        include_year_summary=include_year_summary,
        include_shopify=include_shopify,
        detail_types=detail_types,
        detail_types_all=detail_types_all,
        categories=CATEGORIES,
        labels=CATEGORY_LABELS,
        logo_path=logo_path,
        app_logo_path=app_logo_path,
        months=months,
        default_month=month_int,
        app_title=app_title,
        report_title=report_title,
        branding=branding,
        theme_primary=theme_primary,
        theme_highlight=theme_highlight,
        theme_background=theme_background,
        theme_text=theme_text,
        active_theme=active_theme,
        dup_action=dup_action,
        tx_source_default=tx_source_default,
        tx_period_default=tx_period_default,
        reports_start_tab=reports_start_tab,
        reports_year_limit=year_limit,
        shopify_domain=shopify_domain,
        shopify_token=shopify_token,
        shopify_last_sync=shopify_last_sync,
        qbo_last_sync=qbo_last_sync,
        qbo_client_id=qbo_client_id,
        qbo_client_secret=qbo_client_secret,
        qbo_refresh_token=qbo_refresh_token,
        qbo_realm_id=qbo_realm_id,
        hubspot_token=hubspot_token,
        hubspot_last_sync=hubspot_last_sync,
    )
@app.route("/test-shopify", methods=["POST"])
def test_shopify_connection():
    domain = request.form.get("domain", "").strip()
    token = request.form.get("token", "").strip()
    if not domain or not token:
        return jsonify(success=False), 400
    url = f"https://{domain}/admin/api/2023-07/shop.json"
    try:
        resp = requests.get(url, headers={"X-Shopify-Access-Token": token}, timeout=5)
        ok = resp.status_code == 200
    except Exception as exc:
        log_error(f"Test Shopify connection error: {exc}")
        ok = False
    return jsonify(success=ok)


@app.route("/sync-shopify", methods=["POST"])
def sync_shopify_data():
    """Fetch Shopify orders via API and update the database."""
    domain = get_setting("shopify_domain", "")
    token = get_setting("shopify_token", "")
    # Retrieve duplicate handling preference before any write operations
    action = get_setting("duplicate_action", "review")
    if not domain or not token:
        return jsonify(success=False, error="Missing credentials"), 400
    try:
        df, orders = _fetch_shopify_api(domain, token)
    except Exception as exc:
        log_error(f"Shopify sync error: {exc}")
        return jsonify(success=False, error=str(exc)), 500
    if df.empty:
        return jsonify(success=False, error="No data returned"), 400
    conn = get_db()
    df.to_sql("shopify", conn, if_exists="replace", index=False)
    conn.execute("DELETE FROM shopify_orders")
    for o in orders:
        conn.execute(
            "INSERT INTO shopify_orders(order_id, data) VALUES (?, ?)",
            (o.get("id"), json.dumps(o)),
        )
    _update_sku_map(conn, df["sku"], "shopify")
    if action in {"shopify", "qbo", "both"}:
        _resolve_duplicates(conn, action)
    created = pd.to_datetime(df["created_at"].astype(str), errors="coerce", format="mixed", utc=True).dt.tz_localize(None)
    last_txn = created.max()
    first_txn = created.min()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "REPLACE INTO meta (source, last_updated, last_transaction, first_transaction, last_synced) VALUES (?, ?, ?, ?, ?)",
        (
            "shopify",
            now,
            last_txn.isoformat() if pd.notna(last_txn) else None,
            first_txn.isoformat() if pd.notna(first_txn) else None,
            now,
        ),
    )
    conn.commit()
    conn.close()
    set_setting("shopify_last_sync", now)
    return jsonify(success=True)


@app.route("/qbo/connect")
def qbo_connect():
    """Begin QuickBooks OAuth flow."""
    client_id = get_setting("qbo_client_id", "")
    client_secret = get_setting("qbo_client_secret", "")
    if not client_id or not client_secret:
        flash("Client ID and secret are required", "error")
        return redirect(url_for("settings_page"))
    state = base64.urlsafe_b64encode(os.urandom(16)).decode()
    session["qbo_state"] = state
    redirect_uri = url_for("qbo_callback", _external=True)
    auth_url = (
        "https://appcenter.intuit.com/connect/oauth2"
        f"?client_id={client_id}&response_type=code&scope=com.intuit.quickbooks.accounting"
        f"&redirect_uri={redirect_uri}&state={state}"
    )
    return redirect(auth_url)


@app.route("/qbo/callback")
def qbo_callback():
    """Handle QuickBooks OAuth redirect."""
    state = request.args.get("state", "")
    code = request.args.get("code", "")
    realm_id = request.args.get("realmId", "")
    if not code or state != session.pop("qbo_state", None):
        flash("OAuth failed", "error")
        return redirect(url_for("settings_page"))
    client_id = get_setting("qbo_client_id", "")
    client_secret = get_setting("qbo_client_secret", "")
    redirect_uri = url_for("qbo_callback", _external=True)
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    try:
        resp = requests.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            auth=auth,
            data=data,
            timeout=10,
        )
        resp.raise_for_status()
        tokens = resp.json()
    except Exception:
        flash("OAuth token exchange failed", "error")
        return redirect(url_for("settings_page"))
    refresh = tokens.get("refresh_token")
    if refresh:
        set_setting("qbo_refresh_token", refresh)
    if realm_id:
        set_setting("qbo_realm_id", realm_id)
    flash("QuickBooks connected", "success")
    return redirect(url_for("settings_page"))


@app.route("/test-qbo", methods=["POST"])
def test_qbo_connection():
    client_id = request.form.get("client_id", "").strip()
    client_secret = request.form.get("client_secret", "").strip()
    refresh_token = request.form.get("refresh_token", "").strip()
    realm_id = request.form.get("realm_id", "").strip()
    if not client_id or not client_secret or not refresh_token or not realm_id:
        return jsonify(success=False), 400
    try:
        access_token, new_refresh = _refresh_qbo_access(
            client_id, client_secret, refresh_token
        )
        url = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/companyinfo/{realm_id}"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=5,
        )
        ok = resp.status_code == 200
        if not ok:
            snippet = resp.text[:200].replace("\n", " ")
            log_error(f"Test QBO connection failed: HTTP {resp.status_code} - {snippet}")
        elif new_refresh and new_refresh != refresh_token:
            set_setting("qbo_refresh_token", new_refresh)
    except Exception as exc:
        log_error(f"Test QBO connection error: {exc}")
        ok = False
    return jsonify(success=ok)


@app.route("/sync-qbo", methods=["POST"])
def sync_qbo_data():
    client_id = get_setting("qbo_client_id", "")
    client_secret = get_setting("qbo_client_secret", "")
    refresh_token = get_setting("qbo_refresh_token", "")
    realm_id = get_setting("qbo_realm_id", "")
    action = get_setting("duplicate_action", "review")
    if not client_id or not client_secret or not refresh_token or not realm_id:
        return jsonify(success=False, error="Missing credentials"), 400
    try:
        df, new_refresh = _fetch_qbo_api(
            client_id, client_secret, refresh_token, realm_id
        )
    except Exception as exc:
        log_error(f"QBO sync error: {exc}")
        return jsonify(success=False, error=str(exc)), 500
    if df.empty:
        return jsonify(success=False, error="No data returned"), 400
    conn = get_db()
    df.to_sql("qbo", conn, if_exists="replace", index=False)
    _update_sku_map(conn, df["sku"], "qbo")
    if action in {"shopify", "qbo", "both"}:
        _resolve_duplicates(conn, action)
    created = pd.to_datetime(df["created_at"].astype(str), errors="coerce", format="mixed", utc=True).dt.tz_localize(None)
    last_txn = created.max()
    first_txn = created.min()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "REPLACE INTO meta (source, last_updated, last_transaction, first_transaction, last_synced) VALUES (?, ?, ?, ?, ?)",
        (
            "qbo",
            now,
            last_txn.isoformat() if pd.notna(last_txn) else None,
            first_txn.isoformat() if pd.notna(first_txn) else None,
            now,
        ),
    )
    conn.commit()
    conn.close()
    if new_refresh and new_refresh != refresh_token:
        set_setting("qbo_refresh_token", new_refresh)
    set_setting("qbo_last_sync", now)
    return jsonify(success=True)


@app.route("/test-hubspot", methods=["POST"])
def test_hubspot_connection():
    token = request.form.get("token", "").strip()
    if not token:
        return jsonify(success=False), 400
    try:
        resp = requests.get(
            "https://api.hubapi.com/integrations/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        ok = resp.status_code == 200
    except Exception as exc:
        log_error(f"Test HubSpot connection error: {exc}")
        ok = False
    return jsonify(success=ok)


@app.route("/sync-hubspot", methods=["POST"])
def sync_hubspot_data():
    token = get_setting("hubspot_token", "")
    if not token:
        return jsonify(success=False, error="Missing credentials"), 400
    try:
        resp = requests.get(
            "https://api.hubapi.com/integrations/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as exc:
        log_error(f"HubSpot sync error: {exc}")
        return jsonify(success=False, error=str(exc)), 500
    now = datetime.now(timezone.utc).isoformat()
    set_setting("hubspot_last_sync", now)
    return jsonify(success=True)


@app.route("/logs")
def get_app_logs():
    rows = get_logs(200)
    text = "\n".join(f"[{r['logged_at']}] {r['message']}" for r in rows)
    return text, 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/clear-logs", methods=["POST"])
def clear_app_logs():
    conn = get_db()
    conn.execute("DELETE FROM app_log")
    conn.commit()
    conn.close()
    return jsonify(success=True)


if __name__ == '__main__':
    app.run(debug=True)

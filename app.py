import os
import sqlite3
from datetime import datetime
from io import BytesIO

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, render_template, request, redirect, url_for, flash, send_file

app = Flask(__name__)
app.secret_key = 'secret'
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
    conn.commit()
    conn.close()


init_db()


@app.route('/')
def dashboard():
    conn = get_db()
    meta_df = pd.read_sql_query('SELECT * FROM meta', conn)
    sku_df = pd.read_sql_query(
        'SELECT canonical_sku, COUNT(*)-1 as alias_count FROM sku_map GROUP BY canonical_sku',
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
        shopify = request.files.get('shopify')
        qbo = request.files.get('qbo')
        conn = get_db()

        if shopify:
            df = pd.read_csv(shopify)
            cleaned = df[[
                'Created at', 'Lineitem sku', 'Lineitem name', 'Lineitem quantity',
                'Lineitem price', 'Total'
            ]].copy()
            cleaned.columns = [
                'created_at', 'sku', 'description', 'quantity', 'price', 'total'
            ]
            cleaned.to_sql('shopify', conn, if_exists='replace', index=False)
            conn.execute(
                'REPLACE INTO meta (source, last_updated) VALUES (?, ?)',
                ('shopify', datetime.now().isoformat())
            )
            _update_sku_map(conn, cleaned['sku'])

        if qbo:
            df = pd.read_excel(qbo, skiprows=4)
            df.columns = [
                'deleted_code', 'transaction_date', 'transaction_type',
                'transaction_number', 'customer_name', 'line_description', 'quantity',
                'sales_price', 'amount', 'balance', 'product_service'
            ]
            df = df[df['transaction_date'].notna()]
            cleaned = df[[
                'transaction_date', 'product_service', 'line_description', 'quantity',
                'sales_price', 'amount'
            ]].copy()
            cleaned.columns = [
                'created_at', 'sku', 'description', 'quantity', 'price', 'total'
            ]
            cleaned.to_sql('qbo', conn, if_exists='replace', index=False)
            conn.execute(
                'REPLACE INTO meta (source, last_updated) VALUES (?, ?)',
                ('qbo', datetime.now().isoformat())
            )
            _update_sku_map(conn, cleaned['sku'])

        conn.commit()
        conn.close()
        flash('Files uploaded and data updated.')
        return redirect(url_for('dashboard'))
    return render_template('upload.html')


def _update_sku_map(conn, sku_series):
    aliases = sku_series.dropna().str.lower().str.strip().unique()
    for alias in aliases:
        row = conn.execute('SELECT 1 FROM sku_map WHERE alias=?', (alias,)).fetchone()
        if not row:
            conn.execute(
                'INSERT INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                (alias, alias, 'machine')
            )


@app.route('/sku-map', methods=['GET', 'POST'])
def sku_map_page():
    conn = get_db()
    if request.method == 'POST':
        entries = [k.split('_')[1] for k in request.form.keys() if k.startswith('canonical_')]
        for idx in entries:
            canonical = request.form.get(f'canonical_{idx}', '').lower().strip()
            aliases = request.form.get(f'aliases_{idx}', '')
            type_val = request.form.get(f'type_{idx}', 'machine')
            if not canonical:
                continue
            alias_list = [a.lower().strip() for a in aliases.split(',') if a.strip()]
            alias_set = set(alias_list + [canonical])
            conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (canonical,))
            for alias in alias_set:
                conn.execute(
                    'REPLACE INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                    (alias, canonical, type_val)
                )
        # handle new entry
        canonical_new = request.form.get('canonical_new', '').lower().strip()
        aliases_new = request.form.get('aliases_new', '')
        type_new = request.form.get('type_new', 'machine')
        if canonical_new:
            alias_list = [a.lower().strip() for a in aliases_new.split(',') if a.strip()]
            alias_set = set(alias_list + [canonical_new])
            for alias in alias_set:
                conn.execute(
                    'REPLACE INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                    (alias, canonical_new, type_new)
                )
        conn.commit()
        flash('SKU map updated.')

    rows = conn.execute('SELECT alias, canonical_sku, type FROM sku_map').fetchall()
    conn.close()
    grouped = {}
    for r in rows:
        entry = grouped.setdefault(r['canonical_sku'], {
            'canonical': r['canonical_sku'],
            'aliases': [],
            'type': r['type']
        })
        if r['alias'] != r['canonical_sku']:
            entry['aliases'].append(r['alias'])
    grouped_list = []
    for g in grouped.values():
        g['aliases'] = ', '.join(sorted(g['aliases']))
        grouped_list.append(g)

    return render_template('sku_map.html', grouped=grouped_list)


@app.route('/monthly-report')
def monthly_report():
    year = request.args.get('year', default=datetime.now().year, type=int)
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, total FROM qbo', conn)
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)
    conn.close()

    all_data = pd.concat([shopify, qbo], ignore_index=True)

    all_data = all_data.dropna(subset=['created_at'])

    m = mapping.set_index('alias')
    def map_row(alias, field):
        if isinstance(alias, str):
            key = alias.lower().strip()
            if key in m.index:
                return m.loc[key, field]
            return key if field == 'canonical_sku' else 'machine'
        return alias if field == 'canonical_sku' else 'machine'

    all_data['canonical'] = all_data['sku'].apply(lambda x: map_row(x, 'canonical_sku'))
    all_data['type'] = all_data['sku'].apply(lambda x: map_row(x, 'type'))
    all_data['year'] = all_data['created_at'].dt.year
    all_data['month'] = all_data['created_at'].dt.strftime('%b')
    all_data['month_num'] = all_data['created_at'].dt.month

    summary = all_data.groupby(['year', 'month', 'month_num'])['total'].sum().reset_index()
    cutoff_month = datetime.now().month if year == datetime.now().year else 12

    this_year = summary[summary['year'] == year].set_index('month')
    last_year = summary[summary['year'] == year - 1].set_index('month')

    months_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    rows = []
    for i, month in enumerate(months_order[:cutoff_month], start=1):
        current = this_year['total'].get(month, 0)
        previous = last_year['total'].get(month, 0)
        pct = '-'
        if previous > 0:
            pct = f"{((current-previous)/previous)*100:.1f}%"
        elif current > 0:
            pct = '∞'
        rows.append((month, current, previous, pct))

    machine = all_data[all_data['type']=='machine']
    chem = all_data[all_data['type']!='machine']
    machine_data = {}
    for row in machine.itertuples():
        machine_data.setdefault(row.canonical, {}).setdefault(row.month, 0)
        machine_data[row.canonical][row.month] += row.total
        machine_data[row.canonical]['total'] = machine_data[row.canonical].get('total',0)+row.total
    chem_data = {}
    for row in chem.itertuples():
        chem_data.setdefault(row.canonical, {}).setdefault(row.month, 0)
        chem_data[row.canonical][row.month] += row.total
        chem_data[row.canonical]['total'] = chem_data[row.canonical].get('total',0)+row.total

    years = sorted(summary['year'].unique(), reverse=True)
    return render_template(
        'report.html',
        rows=rows,
        selected_year=year,
        years=years,
        months=months_order[:cutoff_month],
        machine_data=machine_data,
        chem_data=chem_data,
    )


@app.route('/report-chart')
def report_chart():
    year = request.args.get('year', default=datetime.now().year, type=int)
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, total FROM qbo', conn)
    conn.close()

    all_data = pd.concat([shopify, qbo])
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

    months_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    y1 = [this_year['total'].get(m, 0) for m in months_order]
    y2 = [last_year['total'].get(m, 0) for m in months_order]

    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(months_order, y1, label=str(year), marker='o')
    ax.plot(months_order, y2, label=str(year-1), linestyle='--', marker='x')
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


@app.route('/debug')
def debug_summary():
    conn = get_db()
    df = pd.read_sql_query('SELECT * FROM shopify', conn)
    conn.close()
    df['created_at'] = (
        pd.to_datetime(df['created_at'].astype(str), errors='coerce', format='mixed', utc=True)
        .dt.tz_localize(None)
    )
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
    df = df.dropna(subset=['created_at'])
    df['month'] = df['created_at'].dt.strftime('%Y-%m')
    summary = df.groupby('month').agg(count=('total','count'), total=('total','sum')).reset_index()
    summary = summary.sort_values('month')
    return render_template('debug.html', debug_data=summary.itertuples())


if __name__ == '__main__':
    app.run(debug=True)

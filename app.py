import os

from datetime import datetime, timedelta
from io import BytesIO
from difflib import SequenceMatcher
import itertools
import re
from calendar import monthrange

import pandas as pd
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
)
from markupsafe import Markup
from werkzeug.utils import secure_filename
from database import UPLOAD_FOLDER, get_db, get_setting, set_setting

app = Flask(__name__)
app.secret_key = 'secret'

# Height in pixels for the branding logo on exported PDFs
# Reduced slightly so the title and date fit on a single cover page
LOGO_SIZE = 200

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

def format_dt(value):
    """Format ISO timestamp into a readable string like 'May 4th, 2025 - 5:30pm'."""
    try:
        dt = datetime.fromisoformat(value)

        def _suffix(day):
            if 11 <= day % 100 <= 13:
                return "th"
            return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

        month = dt.strftime("%B")
        time_str = dt.strftime("%I:%M%p").lstrip('0').lower()
        return f"{month} {dt.day}{_suffix(dt.day)}, {dt.year} - {time_str}"
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
        return Markup(f"<span class='{color}' style='color:{inline}'>{arrow} {value}</span>")
    except Exception:
        return value

app.jinja_env.filters['trend'] = trend


def fetch_resources(uri, rel):
    """Return local file path for xhtml2pdf resource URIs."""
    if uri.startswith('/static'):
        return os.path.join(app.root_path, uri.lstrip('/'))
    return uri


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


@app.route('/logo.png')
def logo():
    """Serve the ultrasuite application logo."""
    return send_file(os.path.join(app.root_path, 'ultrasuite-logo.png'), mimetype='image/png')


@app.route('/branding-logo.png')
def branding_logo():
    """Serve the uploaded branding logo for reports, falling back to default."""
    custom = get_setting('branding_logo', '')
    if custom:
        path = os.path.join(app.root_path, custom)
        if os.path.exists(path):
            return send_file(path, mimetype='image/png')
    return send_file(os.path.join(app.root_path, 'ultrasuite-logo.png'), mimetype='image/png')


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
        shopify = request.files.get('shopify')
        qbo = request.files.get('qbo')
        conn = get_db()

        if shopify:
            df = pd.read_csv(shopify)
            cleaned = df[[
                'Created at', 'Lineitem sku', 'Lineitem name', 'Lineitem quantity',
                'Lineitem price'
            ]].copy()
            cleaned['Total'] = pd.to_numeric(df['Lineitem price'], errors='coerce').fillna(0) * \
                pd.to_numeric(df['Lineitem quantity'], errors='coerce').fillna(0)
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
                (alias, alias, 'unmapped')
            )


def _save_types(conn, form):
    entries = [k.split('_')[1] for k in form.keys() if k.startswith('canonical_')]
    for idx in entries:
        canonical = form.get(f'canonical_{idx}', '').lower().strip()
        type_val = form.get(f'type_{idx}', 'unmapped')
        if canonical:
            conn.execute(
                'UPDATE sku_map SET type=? WHERE canonical_sku=?',
                (type_val, canonical)
            )


def _clean_sku(text):
    text = text.lower().replace('gal', '')
    return re.sub(r'[^a-z0-9]', '', text)


def _suggest_merges(canonicals, threshold=0.95):
    cleaned = {c: _clean_sku(c) for c in canonicals}
    suggestions = []
    for a, b in itertools.combinations(canonicals, 2):
        ca, cb = cleaned[a], cleaned[b]
        if not ca or not cb or ca[0] != cb[0]:
            continue
        ratio = 1.0 if ca == cb else SequenceMatcher(None, ca, cb).ratio()
        if ratio >= threshold:
            suggestions.append((a, b, ratio))
    suggestions.sort(key=lambda x: -x[2])
    return suggestions

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
                        'SELECT alias FROM sku_map WHERE canonical_sku=?',
                        (canonical,),
                    ).fetchall()
                    for r in rows:
                        conn.execute(
                            'REPLACE INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                            (r['alias'], target, target_type),
                        )
                    if canonical != target:
                        conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (canonical,))
                conn.execute('UPDATE sku_map SET type=? WHERE canonical_sku=?', (target_type, target))
                conn.commit()
                flash('Entries merged.')
        elif 'merge_suggestions' in request.form:
            _save_types(conn, request.form)
            pairs = [k.split('_')[1] for k in request.form if k.startswith('suggest_')]
            for idx in pairs:
                merge_from = request.form.get(f'sug_merge_{idx}', '').lower().strip()
                merge_to = request.form.get(f'sug_target_{idx}', '').lower().strip()
                if merge_from and merge_to:
                    rows = conn.execute(
                        'SELECT alias, type FROM sku_map WHERE canonical_sku=?',
                        (merge_from,),
                    ).fetchall()
                    for r in rows:
                        conn.execute(
                            'REPLACE INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                            (r['alias'], merge_to, r['type']),
                        )
                    if merge_from != merge_to:
                        conn.execute('DELETE FROM sku_map WHERE canonical_sku=?', (merge_from,))
            conn.commit()
            flash('Suggestions merged.')
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
                        'REPLACE INTO sku_map(alias, canonical_sku, type) VALUES(?,?,?)',
                (alias, canonical, type_val)
            )
            canonical_new = request.form.get('canonical_new', '').lower().strip()
            aliases_new = request.form.get('aliases_new', '')
            type_new = request.form.get('type_new', 'unmapped')
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
        conn.close()
        return redirect(url_for('sku_map_page'))

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
        aliases_sorted = sorted(g['aliases'])
        g['alias_count'] = len(aliases_sorted)
        g['aliases'] = ', '.join(aliases_sorted)
        grouped_list.append(g)

    merged_groups = [
        g for g in grouped_list
        if g['alias_count'] > 0 and g['type'] != 'unmapped'
    ]
    mapped_groups = [g for g in grouped_list if g['type'] != 'unmapped']

    # generate merge suggestions
    canonicals = sorted(grouped.keys())
    suggestions = _suggest_merges(canonicals, threshold=0.95)

    return render_template(
        'sku_map.html',
        grouped=grouped_list,
        suggestions=suggestions,
        merged=merged_groups,
        mapped=mapped_groups,
    )


def calculate_report_data(year, month_param=None):
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    mapping = pd.read_sql_query('SELECT alias, canonical_sku, type FROM sku_map', conn)
    conn.close()

    all_data = pd.concat([shopify, qbo], ignore_index=True)

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

    months_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    months_list = [
        {'num': i, 'name': m}
        for i, m in enumerate(months_order, start=1)
    ]
    month_choices = months_list[:cutoff_month]
    rows = []
    for i, month in enumerate(months_order[:cutoff_month], start=1):
        current = this_year["total"].get(month, 0)
        previous = last_year["total"].get(month, 0)
        pct = '-'
        if previous > 0:
            pct = f"{((current-previous)/previous)*100:.1f}%"
        elif current > 0:
            pct = '∞'
        rows.append((month, current, previous, pct))

    # aggregate sales by quarter for the overall report
    quarter_rows = []
    quarter_map = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
    for q, months in quarter_map.items():
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
        pct = '-'
        if prev_total > 0:
            pct = f"{((cur_total - prev_total) / prev_total) * 100:.1f}%"
        elif cur_total > 0:
            pct = '∞'
        quarter_rows.append((f'Q{q}', cur_total, prev_total, pct))

    year_data = all_data[
        (all_data["year"] == year)
        & (all_data["month_num"] <= cutoff_month)
    ]
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
    shopify_avg = shopify_pivot.mean(axis=1)
    shopify_rows = []
    for m in range(1, 13):
        month_name = months_order[m-1]
        raw_vals = [float(shopify_pivot.at[m, y]) if y in shopify_pivot.columns else 0.0 for y in shopify_years]
        diffs = [None] + [raw_vals[i] - raw_vals[i+1] for i in range(len(raw_vals)-1)]
        values = [{'val': raw_vals[i], 'diff': diffs[i]} for i in range(len(raw_vals))]
        shopify_rows.append({'month': month_name, 'values': values, 'avg': float(shopify_avg[m])})

    raw_totals = [float(shopify_pivot[y].sum()) if y in shopify_pivot.columns else 0.0 for y in shopify_years]
    total_diffs = [None] + [raw_totals[i] - raw_totals[i+1] for i in range(len(raw_totals)-1)]
    shopify_totals = [{'val': raw_totals[i], 'diff': total_diffs[i]} for i in range(len(raw_totals))]
    shopify_avg_total = float(sum(raw_totals) / len(raw_totals)) if raw_totals else 0.0

    # Shopify totals by quarter
    shopify_quarters = []
    for q, months in quarter_map.items():
        vals = []
        for y in shopify_years:
            val = shopify_summary[(shopify_summary['year'] == y) & shopify_summary['month_num'].isin(months)]['total'].sum()
            vals.append(float(val))
        diffs = [None] + [vals[i] - vals[i+1] for i in range(len(vals)-1)]
        avg_val = sum(vals) / len(vals) if vals else 0.0
        values = [{'val': vals[i], 'diff': diffs[i]} for i in range(len(vals))]
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
        qtys = totals['quantity']
        totals = totals['total']
        total_cur = totals.sum()
        total_prev = prev['total'].sum()
        vs_last = '-'
        if total_prev > 0:
            vs_last = f"{((total_cur - total_prev) / total_prev) * 100:.1f}%"
        elif total_cur > 0:
            vs_last = '∞'
        overall_cat = summary_type[summary_type['type'] == cat]
        avg_month = overall_cat['total'].mean() if len(overall_cat) else 0
        best_month = overall_cat['total'].max() if len(overall_cat) else 0
        avg_qty = overall_cat['quantity'].mean() if len(overall_cat) else 0
        best_qty = overall_cat['quantity'].max() if len(overall_cat) else 0
        type_rows.append({
            'type': labels.get(cat, cat),
            'total': total_cur,
            'vs_last': vs_last,
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
        cur_qty = cur_month['quantity'].sum()
        prev_total = prev_month['total'].sum()
        vs_last = '-'
        if prev_total > 0:
            vs_last = f"{((cur_total - prev_total) / prev_total) * 100:.1f}%"
        elif cur_total > 0:
            vs_last = '∞'
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
    """Return Shopify monthly totals across years."""
    data = calculate_report_data(datetime.now().year)
    return {
        'years': data['shopify_years'],
        'rows': data['shopify_rows'],
        'totals': data['shopify_totals'],
        'average_total': data['shopify_avg_total'],
    }


def get_shopify_quarterly():
    """Return Shopify quarterly totals across years."""
    data = calculate_report_data(datetime.now().year)
    return {
        'years': data['shopify_years'],
        'rows': data['shopify_quarters'],
    }


@app.route('/monthly-report')
def monthly_report():
    year = request.args.get('year', default=datetime.now().year, type=int)
    month_param = request.args.get('month', type=int)
    data = calculate_report_data(year, month_param)
    return render_template('report.html', **data)


@app.route('/export-report', methods=['GET', 'POST'])
def export_report():
    values = request.values
    if request.method == 'POST' or 'year' in request.args:
        year = int(values.get('year', datetime.now().year))
        month_val = values.get('month')
        month = int(month_val) if month_val and month_val.isdigit() else None
        def _check(name):
            val = values.get(name, '')
            return str(val).lower() in {'on', '1', 'true', 'yes'}

        include_month_summary = _check('include_month_summary')
        include_month_details = _check('include_month_details')
        include_year_overall = _check('include_year_overall')
        include_year_summary = _check('include_year_summary')
        include_shopify = _check('include_shopify')
        detail_types = values.getlist('detail_types')
        if len(detail_types) == 1:
            detail_types = [t.strip() for t in detail_types[0].split(',') if t.strip()]
        if not detail_types:
            detail_types = CATEGORIES
        data = calculate_report_data(year, month)
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
        })
        html = render_template('report_pdf.html', **data, datetime=datetime)
        output = BytesIO()
        pisa.CreatePDF(html, dest=output, link_callback=fetch_resources)
        output.seek(0)
        return send_file(output, download_name='report.pdf', mimetype='application/pdf')

    default_month = get_setting('default_export_month', '')
    month_int = int(default_month) if str(default_month).isdigit() else None
    data = calculate_report_data(datetime.now().year, month_int)
    include_month_summary = get_setting('default_include_month_summary', '1') == '1'
    include_month_details = get_setting('default_include_month_details', '1') == '1'
    include_year_overall = get_setting('default_include_year_overall', '1') == '1'
    include_year_summary = get_setting('default_include_year_summary', '1') == '1'
    include_shopify = get_setting('default_include_shopify', '1') == '1'
    types_default = get_setting('default_detail_types', ','.join(CATEGORIES))
    detail_types = [t for t in types_default.split(',') if t]
    return render_template(
        'export_form.html',
        years=data['years'],
        months=data['months'],
        selected_month=month_int,
        include_month_summary=include_month_summary,
        include_month_details=include_month_details,
        include_year_overall=include_year_overall,
        include_year_summary=include_year_summary,
        include_shopify=include_shopify,
        detail_types=detail_types,
        categories=CATEGORIES,
        labels=CATEGORY_LABELS,
    )


@app.route('/report-chart')
def report_chart():
    year = request.args.get('year', default=datetime.now().year, type=int)
    conn = get_db()
    shopify = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM shopify', conn)
    qbo = pd.read_sql_query('SELECT created_at, sku, quantity, total FROM qbo', conn)
    conn.close()

    all_data = pd.concat([shopify, qbo])
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


@app.route('/sku-details')
def sku_details_page():
    """Show transactions and totals for a SKU across uploads."""
    sku = request.args.get('sku', '').lower().strip()
    source = request.args.get('source', 'both').lower()
    period = request.args.get('period', '')
    start = request.args.get('start')
    end = request.args.get('end')
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
    conn.close()

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

    all_dates = pd.concat([shopify[['created_at']], qbo[['created_at']]])['created_at'].dropna()
    years = sorted(all_dates.dt.year.dropna().unique(), reverse=True)
    month_periods = sorted(all_dates.dt.to_period('M').unique(), reverse=True)
    month_options = [
        {
            'value': f"month-{p.year}-{p.month:02d}",
            'label': p.strftime('%b %Y'),
        }
        for p in month_periods
    ]

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
        else:
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
        df_all = pd.concat(frames, ignore_index=True).sort_values('created_at')
    else:
        df_all = pd.DataFrame(columns=shopify.columns.tolist() + ['source_title'])

    return render_template(
        'sku_details.html',
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
        period=period,
        period_type=period_type,
        period_year=period_year_val,
        period_month=period_month_val,
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

    all_data = pd.concat([shopify, qbo], ignore_index=True)
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



@app.route('/settings', methods=['GET', 'POST'])
def settings_page():
    if request.method == 'POST':
        branding = request.form.get('branding', '').strip()
        set_setting('branding', branding)
        report_title = request.form.get('report_title', '').strip()
        set_setting('report_title', report_title)
        primary_color = request.form.get('primary_color', '').strip()
        highlight_color = request.form.get('highlight_color', '').strip()
        set_setting('branding_primary', primary_color)
        set_setting('branding_highlight', highlight_color)
        include_month_summary = 'include_month_summary' in request.form
        include_month_details = 'include_month_details' in request.form
        include_year_overall = 'include_year_overall' in request.form
        include_year_summary = 'include_year_summary' in request.form
        include_shopify = 'include_shopify' in request.form
        detail_types = request.form.getlist('detail_types')
        set_setting('default_detail_types', ','.join(detail_types))
        set_setting('default_include_month_summary', '1' if include_month_summary else '0')
        set_setting('default_include_month_details', '1' if include_month_details else '0')
        set_setting('default_include_year_overall', '1' if include_year_overall else '0')
        set_setting('default_include_year_summary', '1' if include_year_summary else '0')
        set_setting('default_include_shopify', '1' if include_shopify else '0')
        default_month = request.form.get('default_month', '')
        set_setting('default_export_month', default_month)
        logo_file = request.files.get('logo')
        if logo_file and logo_file.filename:
            filename = secure_filename(logo_file.filename)
            ext = os.path.splitext(filename)[1].lower()
            if ext in {'.png', '.jpg', '.jpeg', '.gif'}:
                save_name = f'branding_logo{ext}'
                path = os.path.join(UPLOAD_FOLDER, save_name)
                logo_file.save(path)
                set_setting('branding_logo', os.path.join(UPLOAD_FOLDER, save_name))
        flash('Settings saved.')
        return redirect(url_for('settings_page'))
    branding = get_setting('branding', '')
    report_title = get_setting('report_title', '')
    primary_color = get_setting('branding_primary', '#1976d2')
    highlight_color = get_setting('branding_highlight', '#bbdefb')
    include_month_summary = get_setting('default_include_month_summary', '1') == '1'
    include_month_details = get_setting('default_include_month_details', '1') == '1'
    include_year_overall = get_setting('default_include_year_overall', '1') == '1'
    include_year_summary = get_setting('default_include_year_summary', '1') == '1'
    include_shopify = get_setting('default_include_shopify', '1') == '1'
    types_default = get_setting('default_detail_types', ','.join(CATEGORIES))
    detail_types = [t for t in types_default.split(',') if t]
    logo_path = get_setting('branding_logo', '')
    month_default = get_setting('default_export_month', '')
    month_int = int(month_default) if str(month_default).isdigit() else None
    months = calculate_report_data(datetime.now().year)['months']
    return render_template(
        'settings.html',
        branding=branding,
        primary_color=primary_color,
        highlight_color=highlight_color,
        report_title=report_title,
        include_month_summary=include_month_summary,
        include_month_details=include_month_details,
        include_year_overall=include_year_overall,
        include_year_summary=include_year_summary,
        include_shopify=include_shopify,
        detail_types=detail_types,
        categories=CATEGORIES,
        labels=CATEGORY_LABELS,
        logo_path=logo_path,
        months=months,
        default_month=month_int,
    )

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

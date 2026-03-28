"""
Data ingestion module for the Vantage Diagnostic Tool.
Handles Excel/CSV parsing, auto-detection of data types, and normalization
to standard schemas used by the analysis engine.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from difflib import SequenceMatcher
import io
import os


# ---------------------------------------------------------------------------
# Normalized Schemas
# ---------------------------------------------------------------------------

NORMALIZED_COST_SCHEMA = {
    'cost_code': str, 'cost_type': str, 'description': str,
    'original_budget': float, 'approved_changes': float, 'revised_budget': float,
    'committed_cost': float, 'actual_cost': float, 'pending_changes': float,
    'forecast_at_completion': float, 'variance': float, 'percent_complete': float
}

NORMALIZED_SCHEDULE_SCHEMA = {
    'activity_id': str, 'activity_name': str, 'wbs_code': str,
    'original_duration': int, 'remaining_duration': int,
    'early_start': 'date', 'early_finish': 'date', 'late_start': 'date', 'late_finish': 'date',
    'actual_start': 'date', 'actual_finish': 'date',
    'total_float': int, 'percent_complete': float, 'is_critical': bool,
    'constraint_type': str, 'predecessors': str
}

NORMALIZED_CHANGE_ORDER_SCHEMA = {
    'co_number': str, 'title': str, 'status': str, 'type': str,
    'created_date': 'date', 'approved_date': 'date', 'amount': float,
    'cost_code': str, 'markup_amount': float, 'direct_cost': float,
    'schedule_impact_days': int
}

# ---------------------------------------------------------------------------
# Column name mappings — common names from Procore, P6, MS Project, Sage, etc.
# ---------------------------------------------------------------------------

COST_COLUMN_ALIASES = {
    'cost_code': [
        'cost code', 'costcode', 'code', 'wbs', 'wbs code', 'account',
        'account code', 'gl code', 'cost account', 'item', 'line item',
        'budget code', 'division', 'csi code', 'csi division'
    ],
    'cost_type': [
        'cost type', 'type', 'category', 'cost category', 'expense type',
        'commitment type'
    ],
    'description': [
        'description', 'desc', 'name', 'item description', 'line description',
        'scope', 'cost description', 'budget item', 'budget description'
    ],
    'original_budget': [
        'original budget', 'orig budget', 'original contract', 'base budget',
        'budget', 'original value', 'contract value', 'original amount',
        'base contract', 'budgeted cost', 'original estimate', 'estimated cost'
    ],
    'approved_changes': [
        'approved changes', 'approved cos', 'change orders', 'co amount',
        'approved change orders', 'changes', 'adjustments', 'budget changes',
        'budget adjustments', 'approved modifications'
    ],
    'revised_budget': [
        'revised budget', 'current budget', 'adjusted budget', 'total budget',
        'revised contract', 'current contract', 'revised estimate',
        'budget + changes', 'total contract'
    ],
    'committed_cost': [
        'committed cost', 'committed', 'commitments', 'committed amount',
        'subcontract value', 'committed costs', 'encumbered', 'encumbrance',
        'purchase orders', 'contracts'
    ],
    'actual_cost': [
        'actual cost', 'actuals', 'actual', 'actual costs', 'cost to date',
        'costs to date', 'acwp', 'actual spend', 'spent', 'expenditure',
        'actual expenditure', 'paid to date', 'invoiced'
    ],
    'pending_changes': [
        'pending changes', 'pending cos', 'pending', 'pending change orders',
        'unapproved changes', 'proposed changes', 'pcco', 'pending cost',
        'anticipated changes', 'trend'
    ],
    'forecast_at_completion': [
        'forecast at completion', 'eac', 'estimate at completion', 'forecast',
        'projected cost', 'estimated final cost', 'projected final',
        'forecast final', 'final forecast', 'anticipated final cost',
        'estimated total cost'
    ],
    'variance': [
        'variance', 'cost variance', 'budget variance', 'over/under',
        'over under', 'gain loss', 'gain/loss', 'delta', 'difference'
    ],
    'percent_complete': [
        'percent complete', '% complete', 'pct complete', 'completion',
        '% done', 'progress', 'physical complete', 'physical % complete'
    ],
}

SCHEDULE_COLUMN_ALIASES = {
    'activity_id': [
        'activity id', 'activityid', 'task id', 'id', 'activity code',
        'task code', 'wbs id', 'unique id', 'uid'
    ],
    'activity_name': [
        'activity name', 'task name', 'name', 'description', 'activity',
        'task', 'activity description', 'task description'
    ],
    'wbs_code': [
        'wbs code', 'wbs', 'wbs path', 'outline code', 'outline level',
        'work breakdown structure'
    ],
    'original_duration': [
        'original duration', 'orig duration', 'planned duration',
        'baseline duration', 'duration', 'dur'
    ],
    'remaining_duration': [
        'remaining duration', 'rem duration', 'remaining dur',
        'remaining', 'rem dur'
    ],
    'early_start': [
        'early start', 'es', 'start', 'planned start', 'scheduled start',
        'start date', 'baseline start'
    ],
    'early_finish': [
        'early finish', 'ef', 'finish', 'planned finish', 'scheduled finish',
        'finish date', 'end date', 'baseline finish'
    ],
    'late_start': [
        'late start', 'ls', 'late start date'
    ],
    'late_finish': [
        'late finish', 'lf', 'late finish date'
    ],
    'actual_start': [
        'actual start', 'act start', 'actual start date', 'as'
    ],
    'actual_finish': [
        'actual finish', 'act finish', 'actual finish date', 'af',
        'actual end', 'actual end date'
    ],
    'total_float': [
        'total float', 'float', 'tf', 'total slack', 'slack',
        'free float', 'ff'
    ],
    'percent_complete': [
        'percent complete', '% complete', 'pct complete', 'completion',
        '% done', 'progress', 'physical % complete', 'physical complete'
    ],
    'is_critical': [
        'critical', 'is critical', 'on critical path', 'critical path',
        'crit'
    ],
    'constraint_type': [
        'constraint type', 'constraint', 'date constraint', 'constraint kind'
    ],
    'predecessors': [
        'predecessors', 'predecessor', 'pred', 'predecessor activities',
        'dependencies', 'depends on'
    ],
}

CHANGE_ORDER_COLUMN_ALIASES = {
    'co_number': [
        'co number', 'co #', 'change order number', 'number', 'co no',
        'change number', 'id', 'co id', 'pco number', 'rco number'
    ],
    'title': [
        'title', 'description', 'co description', 'change description',
        'subject', 'name', 'change order description', 'scope'
    ],
    'status': [
        'status', 'co status', 'change order status', 'state',
        'approval status'
    ],
    'type': [
        'type', 'co type', 'change type', 'category', 'change order type',
        'reason', 'cause', 'origin', 'responsibility'
    ],
    'created_date': [
        'created date', 'date created', 'create date', 'initiated',
        'date initiated', 'date', 'submitted date', 'request date',
        'date submitted', 'open date'
    ],
    'approved_date': [
        'approved date', 'date approved', 'approval date', 'close date',
        'closed date', 'executed date', 'date executed', 'date closed'
    ],
    'amount': [
        'amount', 'total amount', 'co amount', 'value', 'total value',
        'change amount', 'net amount', 'cost', 'total cost', 'contract amount'
    ],
    'cost_code': [
        'cost code', 'code', 'wbs', 'budget code', 'account code',
        'line item'
    ],
    'markup_amount': [
        'markup amount', 'markup', 'oh&p', 'overhead', 'overhead and profit',
        'fee', 'margin', 'gc markup', 'contractor markup'
    ],
    'direct_cost': [
        'direct cost', 'direct', 'base cost', 'net cost', 'cost before markup',
        'labor and material', 'subcontract cost'
    ],
    'schedule_impact_days': [
        'schedule impact', 'schedule impact days', 'time impact',
        'time extension', 'days impact', 'schedule days', 'delay days',
        'time impact days', 'calendar days'
    ],
}

# Fingerprint columns to auto-detect data type
COST_FINGERPRINT = [
    'original_budget', 'actual_cost', 'forecast_at_completion',
    'committed_cost', 'revised_budget'
]
SCHEDULE_FINGERPRINT = [
    'early_start', 'early_finish', 'total_float', 'remaining_duration',
    'predecessors'
]
CHANGE_FINGERPRINT = [
    'co_number', 'approved_date', 'schedule_impact_days', 'markup_amount'
]


def fuzzy_score(a: str, b: str) -> float:
    """Return similarity ratio between two strings (0-1)."""
    a = a.lower().strip().replace('_', ' ')
    b = b.lower().strip().replace('_', ' ')
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def best_match(column_name: str, alias_map: dict, threshold: float = 0.65) -> str | None:
    """Find the best matching normalized column for a raw column name."""
    col_lower = column_name.lower().strip().replace('_', ' ')
    best_field = None
    best_score = threshold

    for field, aliases in alias_map.items():
        for alias in aliases:
            score = fuzzy_score(col_lower, alias)
            if score > best_score:
                best_score = score
                best_field = field
        # Also check against the field name itself
        score = fuzzy_score(col_lower, field.replace('_', ' '))
        if score > best_score:
            best_score = score
            best_field = field

    return best_field


def detect_data_type(df: pd.DataFrame) -> str:
    """Detect whether a dataframe is budget, schedule, or change order data."""
    raw_columns = list(df.columns)
    mapped = {}
    for col in raw_columns:
        for alias_map, label in [
            (COST_COLUMN_ALIASES, 'cost'),
            (SCHEDULE_COLUMN_ALIASES, 'schedule'),
            (CHANGE_ORDER_COLUMN_ALIASES, 'change'),
        ]:
            match = best_match(col, alias_map)
            if match:
                mapped.setdefault(label, set()).add(match)

    # Score by how many fingerprint fields matched
    scores = {}
    for label, fingerprint in [
        ('cost', COST_FINGERPRINT),
        ('schedule', SCHEDULE_FINGERPRINT),
        ('change', CHANGE_FINGERPRINT),
    ]:
        matched = mapped.get(label, set())
        scores[label] = sum(1 for f in fingerprint if f in matched)

    best = max(scores, key=scores.get)
    if scores[best] == 0:
        # Fallback: score by total matched columns
        totals = {k: len(v) for k, v in mapped.items()}
        if totals:
            best = max(totals, key=totals.get)
        else:
            best = 'cost'  # default
    return best


def map_columns(df: pd.DataFrame, alias_map: dict) -> dict:
    """Return a dict mapping normalized field name -> raw column name."""
    result = {}
    used_raw = set()
    for col in df.columns:
        match = best_match(col, alias_map)
        if match and match not in result:
            result[match] = col
            used_raw.add(col)
    return result


def safe_float(series: pd.Series) -> pd.Series:
    """Convert series to float, coercing errors to NaN."""
    return pd.to_numeric(series, errors='coerce')


def safe_date(series: pd.Series) -> pd.Series:
    """Convert series to datetime, coercing errors to NaT."""
    try:
        return pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
    except TypeError:
        # Newer pandas versions removed infer_datetime_format
        return pd.to_datetime(series, errors='coerce', format='mixed')


def safe_int(series: pd.Series) -> pd.Series:
    """Convert series to int via float, coercing errors to NaN."""
    return pd.to_numeric(series, errors='coerce')


def safe_bool(series: pd.Series) -> pd.Series:
    """Convert series to bool — handles yes/no, true/false, 1/0, Y/N."""
    def _convert(val):
        if pd.isna(val):
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        s = str(val).lower().strip()
        return s in ('yes', 'y', 'true', '1', 'critical')
    return series.apply(_convert)


def normalize_cost_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cost/budget dataframe to the standard schema."""
    col_map = map_columns(df, COST_COLUMN_ALIASES)
    normalized = pd.DataFrame()

    for field, dtype in NORMALIZED_COST_SCHEMA.items():
        raw_col = col_map.get(field)
        if raw_col is not None and raw_col in df.columns:
            if dtype == float:
                normalized[field] = safe_float(df[raw_col])
            else:
                normalized[field] = df[raw_col].astype(str).fillna('')
        else:
            if dtype == float:
                normalized[field] = np.nan
            else:
                normalized[field] = ''

    # Compute derived fields if missing
    if normalized['revised_budget'].isna().all() and not normalized['original_budget'].isna().all():
        normalized['revised_budget'] = normalized['original_budget'] + normalized['approved_changes'].fillna(0)

    if normalized['variance'].isna().all() and not normalized['revised_budget'].isna().all():
        normalized['variance'] = normalized['revised_budget'] - normalized['forecast_at_completion'].fillna(
            normalized['actual_cost']
        )

    if normalized['percent_complete'].max() > 1.0:
        normalized['percent_complete'] = normalized['percent_complete'] / 100.0

    # Drop rows that are entirely NaN in numeric columns
    numeric_cols = [c for c, t in NORMALIZED_COST_SCHEMA.items() if t == float]
    normalized = normalized.dropna(subset=numeric_cols, how='all')

    return normalized


def normalize_schedule_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a schedule dataframe to the standard schema."""
    col_map = map_columns(df, SCHEDULE_COLUMN_ALIASES)
    normalized = pd.DataFrame()

    for field, dtype in NORMALIZED_SCHEDULE_SCHEMA.items():
        raw_col = col_map.get(field)
        if raw_col is not None and raw_col in df.columns:
            if dtype == 'date':
                normalized[field] = safe_date(df[raw_col])
            elif dtype == int:
                normalized[field] = safe_int(df[raw_col])
            elif dtype == float:
                normalized[field] = safe_float(df[raw_col])
            elif dtype == bool:
                normalized[field] = safe_bool(df[raw_col])
            else:
                normalized[field] = df[raw_col].astype(str).fillna('')
        else:
            if dtype == 'date':
                normalized[field] = pd.NaT
            elif dtype in (int, float):
                normalized[field] = np.nan
            elif dtype == bool:
                normalized[field] = False
            else:
                normalized[field] = ''

    if normalized['percent_complete'].max() > 1.0:
        normalized['percent_complete'] = normalized['percent_complete'] / 100.0

    # Infer is_critical from float if not provided
    if not normalized['is_critical'].any() and normalized['total_float'].notna().any():
        normalized['is_critical'] = normalized['total_float'] <= 0

    # Drop rows missing both activity_id and activity_name
    normalized = normalized[
        (normalized['activity_id'] != '') | (normalized['activity_name'] != '')
    ]

    return normalized


def normalize_change_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a change order dataframe to the standard schema."""
    col_map = map_columns(df, CHANGE_ORDER_COLUMN_ALIASES)
    normalized = pd.DataFrame()

    for field, dtype in NORMALIZED_CHANGE_ORDER_SCHEMA.items():
        raw_col = col_map.get(field)
        if raw_col is not None and raw_col in df.columns:
            if dtype == 'date':
                normalized[field] = safe_date(df[raw_col])
            elif dtype == float:
                normalized[field] = safe_float(df[raw_col])
            elif dtype == int:
                normalized[field] = safe_int(df[raw_col])
            else:
                normalized[field] = df[raw_col].astype(str).fillna('')
        else:
            if dtype == 'date':
                normalized[field] = pd.NaT
            elif dtype in (int, float):
                normalized[field] = np.nan
            else:
                normalized[field] = ''

    # Compute markup if direct_cost and amount available
    if normalized['markup_amount'].isna().all():
        if normalized['direct_cost'].notna().any() and normalized['amount'].notna().any():
            normalized['markup_amount'] = normalized['amount'] - normalized['direct_cost']

    normalized = normalized.dropna(subset=['amount'], how='all')
    return normalized


def read_file(file_obj, filename: str) -> pd.DataFrame:
    """Read an uploaded file into a pandas DataFrame."""
    ext = os.path.splitext(filename)[1].lower()
    if ext in ('.xlsx', '.xls'):
        df = pd.read_excel(file_obj, engine='openpyxl')
    elif ext == '.csv':
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8', errors='replace')
        df = pd.read_csv(io.StringIO(content))
    else:
        raise ValueError(f"Unsupported file type: {ext}. Please upload .xlsx, .xls, or .csv files.")

    # Strip whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]
    return df


def ingest_file(file_obj, filename: str) -> dict:
    """
    Main ingestion entry point.
    Returns dict with keys:
        - 'type': 'cost' | 'schedule' | 'change'
        - 'data': normalized pd.DataFrame
        - 'raw_columns': list of original column names
        - 'mapped_columns': dict of normalized -> raw column name
        - 'row_count': int
    """
    df = read_file(file_obj, filename)
    data_type = detect_data_type(df)

    if data_type == 'cost':
        normalized = normalize_cost_data(df)
        col_map = map_columns(df, COST_COLUMN_ALIASES)
    elif data_type == 'schedule':
        normalized = normalize_schedule_data(df)
        col_map = map_columns(df, SCHEDULE_COLUMN_ALIASES)
    elif data_type == 'change':
        normalized = normalize_change_data(df)
        col_map = map_columns(df, CHANGE_ORDER_COLUMN_ALIASES)
    else:
        raise ValueError(f"Could not determine data type for {filename}")

    return {
        'type': data_type,
        'data': normalized,
        'raw_columns': list(df.columns),
        'mapped_columns': col_map,
        'row_count': len(normalized),
    }

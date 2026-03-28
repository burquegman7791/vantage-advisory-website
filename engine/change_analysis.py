"""
Change order and RFI analysis module.
Computes CO rates, aging, backlog, type distribution, and acceleration trends.
"""

import pandas as pd
import numpy as np
from datetime import datetime


def analyze_changes(change_df: pd.DataFrame, cost_df: pd.DataFrame = None) -> dict:
    """
    Perform comprehensive change order analysis.
    Returns a dict of metrics.
    """
    results = {}

    if change_df is None or change_df.empty:
        return results

    total_cos = len(change_df)
    results['total_cos'] = total_cos

    # --- Original contract value (from cost data if available) ---
    original_contract = 0
    if cost_df is not None and not cost_df.empty:
        original_contract = cost_df['original_budget'].sum()
    results['original_contract'] = original_contract

    # --- Total CO amounts ---
    total_co_amount = change_df['amount'].fillna(0).sum()
    results['total_co_amount'] = total_co_amount

    # --- CO Rate (% of original contract) ---
    if original_contract > 0:
        results['co_rate'] = abs(total_co_amount) / original_contract * 100
    else:
        results['co_rate'] = None

    # --- Status distribution ---
    status_counts = {}
    for _, row in change_df.iterrows():
        status = str(row['status']).lower().strip()
        if status in ('approved', 'executed', 'closed', 'accepted'):
            status_counts['approved'] = status_counts.get('approved', 0) + 1
        elif status in ('pending', 'submitted', 'under review', 'open', 'in review'):
            status_counts['pending'] = status_counts.get('pending', 0) + 1
        elif status in ('rejected', 'denied', 'void', 'voided'):
            status_counts['rejected'] = status_counts.get('rejected', 0) + 1
        elif status in ('draft', 'proposed'):
            status_counts['draft'] = status_counts.get('draft', 0) + 1
        else:
            status_counts['other'] = status_counts.get('other', 0) + 1

    results['status_distribution'] = status_counts
    results['approved_count'] = status_counts.get('approved', 0)
    results['pending_count'] = status_counts.get('pending', 0)

    # --- Approved vs Pending amounts ---
    approved_mask = change_df['status'].str.lower().isin(['approved', 'executed', 'closed', 'accepted'])
    pending_mask = change_df['status'].str.lower().isin(['pending', 'submitted', 'under review', 'open', 'in review'])

    results['approved_amount'] = change_df.loc[approved_mask, 'amount'].fillna(0).sum()
    results['pending_amount'] = change_df.loc[pending_mask, 'amount'].fillna(0).sum()

    # --- Pending CO backlog as % of contract ---
    if original_contract > 0:
        results['pending_backlog_pct'] = abs(results['pending_amount']) / original_contract * 100
    else:
        results['pending_backlog_pct'] = None

    # --- CO Aging (days pending) ---
    today = pd.Timestamp.now()
    pending_cos = change_df[pending_mask].copy()
    if not pending_cos.empty and pending_cos['created_date'].notna().any():
        pending_cos['age_days'] = (today - pending_cos['created_date']).dt.days
        results['avg_pending_age'] = pending_cos['age_days'].mean()
        results['max_pending_age'] = pending_cos['age_days'].max()
        results['cos_over_30_days'] = int((pending_cos['age_days'] > 30).sum())
        results['cos_over_60_days'] = int((pending_cos['age_days'] > 60).sum())
    else:
        results['avg_pending_age'] = 0
        results['max_pending_age'] = 0
        results['cos_over_30_days'] = 0
        results['cos_over_60_days'] = 0

    # --- Approved CO processing time ---
    approved_cos = change_df[approved_mask].copy()
    if not approved_cos.empty:
        has_dates = approved_cos['created_date'].notna() & approved_cos['approved_date'].notna()
        if has_dates.any():
            approved_cos.loc[has_dates, 'processing_days'] = (
                approved_cos.loc[has_dates, 'approved_date'] - approved_cos.loc[has_dates, 'created_date']
            ).dt.days
            results['avg_processing_days'] = approved_cos['processing_days'].mean()
        else:
            results['avg_processing_days'] = None
    else:
        results['avg_processing_days'] = None

    # --- CO Type Distribution ---
    type_dist = {}
    type_amounts = {}
    for _, row in change_df.iterrows():
        co_type = str(row['type']).lower().strip()
        if co_type in ('owner', 'owner change', 'owner directed', 'ocd'):
            key = 'owner_directed'
        elif co_type in ('design error', 'design', 'architect', 'design deficiency', 'a/e error'):
            key = 'design_error'
        elif co_type in ('unforeseen', 'differing site conditions', 'concealed', 'hidden', 'unforeseen conditions'):
            key = 'unforeseen'
        elif co_type in ('contractor', 'gc error', 'construction error', 'means and methods'):
            key = 'contractor'
        elif co_type in ('regulatory', 'code change', 'permit'):
            key = 'regulatory'
        else:
            key = 'other'
        type_dist[key] = type_dist.get(key, 0) + 1
        type_amounts[key] = type_amounts.get(key, 0) + float(row['amount'] or 0)

    results['type_distribution'] = type_dist
    results['type_amounts'] = type_amounts

    # Design error CO ratio
    total_non_owner = sum(v for k, v in type_amounts.items() if k != 'owner_directed')
    design_error_amount = type_amounts.get('design_error', 0)
    if total_non_owner > 0:
        results['design_error_ratio'] = abs(design_error_amount) / abs(total_non_owner)
    else:
        results['design_error_ratio'] = 0

    # --- CO Frequency Trend ---
    if change_df['created_date'].notna().any():
        dated = change_df[change_df['created_date'].notna()].copy()
        dated['month'] = dated['created_date'].dt.to_period('M')
        monthly = dated.groupby('month').agg(
            count=('co_number', 'count'),
            amount=('amount', 'sum')
        ).reset_index()
        monthly['month'] = monthly['month'].astype(str)
        results['monthly_trend'] = monthly.to_dict('records')

        # CO acceleration: is the rate increasing?
        if len(monthly) >= 3:
            counts = monthly['count'].values
            first_half = counts[:len(counts)//2].mean()
            second_half = counts[len(counts)//2:].mean()
            if first_half > 0:
                results['co_acceleration'] = second_half / first_half
            else:
                results['co_acceleration'] = None
        else:
            results['co_acceleration'] = None
    else:
        results['monthly_trend'] = []
        results['co_acceleration'] = None

    # --- Schedule Impact ---
    if change_df['schedule_impact_days'].notna().any():
        results['total_schedule_impact'] = int(change_df['schedule_impact_days'].fillna(0).sum())
        results['avg_schedule_impact'] = change_df['schedule_impact_days'].fillna(0).mean()
        results['cos_with_schedule_impact'] = int((change_df['schedule_impact_days'].fillna(0) > 0).sum())
    else:
        results['total_schedule_impact'] = 0
        results['avg_schedule_impact'] = 0
        results['cos_with_schedule_impact'] = 0

    # --- Markup Analysis ---
    if change_df['markup_amount'].notna().any() and change_df['direct_cost'].notna().any():
        has_markup = change_df['markup_amount'].notna() & change_df['direct_cost'].notna() & (change_df['direct_cost'] != 0)
        if has_markup.any():
            markup_pcts = change_df.loc[has_markup, 'markup_amount'] / change_df.loc[has_markup, 'direct_cost'] * 100
            results['avg_markup_pct'] = markup_pcts.mean()
            results['max_markup_pct'] = markup_pcts.max()
        else:
            results['avg_markup_pct'] = None
            results['max_markup_pct'] = None
    else:
        results['avg_markup_pct'] = None
        results['max_markup_pct'] = None

    return results

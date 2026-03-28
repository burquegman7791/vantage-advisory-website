"""
Budget / Earned Value analysis module.
Computes EVM metrics, cost analysis, Pareto, and contingency burn rate.
"""

import pandas as pd
import numpy as np


def analyze_budget(cost_df: pd.DataFrame) -> dict:
    """
    Perform comprehensive budget analysis on normalized cost data.
    Returns a dict of metrics.
    """
    results = {}

    if cost_df is None or cost_df.empty:
        return results

    # --- Project-level aggregations ---
    total_original_budget = cost_df['original_budget'].sum()
    total_approved_changes = cost_df['approved_changes'].fillna(0).sum()
    total_revised_budget = cost_df['revised_budget'].sum()
    total_actual_cost = cost_df['actual_cost'].fillna(0).sum()
    total_committed = cost_df['committed_cost'].fillna(0).sum()
    total_pending = cost_df['pending_changes'].fillna(0).sum()
    total_forecast = cost_df['forecast_at_completion'].fillna(0).sum()

    results['total_original_budget'] = total_original_budget
    results['total_approved_changes'] = total_approved_changes
    results['total_revised_budget'] = total_revised_budget
    results['total_actual_cost'] = total_actual_cost
    results['total_committed'] = total_committed
    results['total_pending'] = total_pending
    results['total_forecast'] = total_forecast

    # --- Earned Value Metrics ---
    # EV per line item = percent_complete * revised_budget
    cost_df = cost_df.copy()
    cost_df['ev'] = cost_df['percent_complete'].fillna(0) * cost_df['revised_budget'].fillna(0)
    cost_df['pv'] = cost_df['revised_budget'].fillna(0)  # simplified PV = budget (time-based PV needs schedule)

    total_ev = cost_df['ev'].sum()
    total_ac = total_actual_cost
    total_pv = total_revised_budget  # simplified

    results['total_ev'] = total_ev
    results['total_ac'] = total_ac
    results['total_pv'] = total_pv

    # CPI
    if total_ac > 0:
        results['cpi'] = total_ev / total_ac
    else:
        results['cpi'] = None

    # SPI (simplified — ideally uses time-based PV)
    weighted_pct_complete = total_ev / total_revised_budget if total_revised_budget > 0 else 0
    results['weighted_percent_complete'] = weighted_pct_complete

    # For SPI we use a simplified model: EV / (project pct elapsed * budget)
    # Without schedule data, we approximate using weighted completion
    if total_pv > 0:
        results['spi'] = total_ev / total_pv if total_pv > 0 else None
    else:
        results['spi'] = None

    # Cost Variance
    results['cv'] = total_ev - total_ac
    if total_ev > 0:
        results['cv_pct'] = (total_ev - total_ac) / total_ev * 100
    else:
        results['cv_pct'] = 0.0

    # Schedule Variance
    results['sv'] = total_ev - total_pv
    if total_pv > 0:
        results['sv_pct'] = (total_ev - total_pv) / total_pv * 100
    else:
        results['sv_pct'] = 0.0

    # EAC — multiple methods
    cpi = results.get('cpi')
    bac = total_revised_budget

    if cpi and cpi > 0:
        results['eac_cpi'] = bac / cpi  # EAC = BAC / CPI
    else:
        results['eac_cpi'] = None

    results['eac_actual_forecast'] = total_forecast  # from the data
    results['eac_ac_plus_remaining'] = total_ac + (bac - total_ev)  # EAC = AC + (BAC - EV)

    # VAC
    if results['eac_cpi']:
        results['vac'] = bac - results['eac_cpi']
    else:
        results['vac'] = bac - total_forecast if total_forecast > 0 else None

    # TCPI
    remaining_work = bac - total_ev
    remaining_budget = bac - total_ac
    if remaining_budget > 0:
        results['tcpi'] = remaining_work / remaining_budget
    else:
        results['tcpi'] = None

    # --- Contingency Analysis ---
    # Contingency = approved_changes consumed relative to progress
    if total_original_budget > 0 and total_approved_changes > 0:
        # Contingency burn rate = (approved changes / original contingency allocation)
        # Approximate contingency as % of original budget
        contingency_pct = total_approved_changes / total_original_budget
        if weighted_pct_complete > 0:
            results['contingency_burn_ratio'] = contingency_pct / weighted_pct_complete
        else:
            results['contingency_burn_ratio'] = contingency_pct * 10  # early project, high signal
    else:
        results['contingency_burn_ratio'] = 0.0

    # --- Per-Division Analysis ---
    division_data = []
    if cost_df['cost_code'].notna().any() and (cost_df['cost_code'] != '').any():
        grouped = cost_df.groupby('cost_code').agg({
            'original_budget': 'sum',
            'revised_budget': 'sum',
            'actual_cost': 'sum',
            'ev': 'sum',
            'forecast_at_completion': 'sum',
            'percent_complete': 'mean',
        }).reset_index()

        for _, row in grouped.iterrows():
            div = {
                'cost_code': row['cost_code'],
                'original_budget': row['original_budget'],
                'revised_budget': row['revised_budget'],
                'actual_cost': row['actual_cost'],
                'ev': row['ev'],
                'forecast': row['forecast_at_completion'],
                'percent_complete': row['percent_complete'],
            }
            if row['actual_cost'] > 0:
                div['cpi'] = row['ev'] / row['actual_cost']
            else:
                div['cpi'] = None
            div['variance'] = row['revised_budget'] - row['forecast_at_completion']
            division_data.append(div)

    results['divisions'] = sorted(division_data, key=lambda d: d.get('variance', 0))

    # --- Pareto of overruns ---
    overruns = [d for d in division_data if d.get('variance', 0) < 0]
    total_overrun = sum(abs(d['variance']) for d in overruns)
    cumulative = 0
    pareto = []
    for d in sorted(overruns, key=lambda x: x['variance']):
        cumulative += abs(d['variance'])
        pareto.append({
            'cost_code': d['cost_code'],
            'overrun': abs(d['variance']),
            'cumulative_pct': cumulative / total_overrun * 100 if total_overrun > 0 else 0,
        })
    results['pareto'] = pareto

    # --- Line-item level CPI for early deficit detection ---
    items_with_cpi = cost_df[cost_df['actual_cost'] > 0].copy()
    if not items_with_cpi.empty:
        items_with_cpi['line_cpi'] = items_with_cpi['ev'] / items_with_cpi['actual_cost']
        results['min_line_cpi'] = items_with_cpi['line_cpi'].min()
        results['median_line_cpi'] = items_with_cpi['line_cpi'].median()
        results['items_below_cpi_90'] = (items_with_cpi['line_cpi'] < 0.90).sum()
        results['items_below_cpi_80'] = (items_with_cpi['line_cpi'] < 0.80).sum()
        results['total_items_with_cost'] = len(items_with_cpi)
    else:
        results['min_line_cpi'] = None
        results['median_line_cpi'] = None
        results['items_below_cpi_90'] = 0
        results['items_below_cpi_80'] = 0
        results['total_items_with_cost'] = 0

    return results

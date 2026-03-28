"""
Operations efficiency analysis module.
Computes productivity factors, overtime, rework indicators, and equipment utilization.
"""

import pandas as pd
import numpy as np


def analyze_operations(cost_df: pd.DataFrame = None, schedule_df: pd.DataFrame = None,
                       budget_results: dict = None, schedule_results: dict = None) -> dict:
    """
    Perform operations efficiency analysis using available data.
    Returns a dict of metrics.
    """
    results = {}

    # --- Productivity Factor ---
    # Derived from budget performance: EV / AC for labor-intensive codes
    if budget_results:
        cpi = budget_results.get('cpi')
        if cpi is not None:
            # Productivity factor approximated from CPI
            results['productivity_factor'] = cpi
        else:
            results['productivity_factor'] = None

        # Labor items with poor CPI indicate productivity issues
        results['items_below_cpi_80'] = budget_results.get('items_below_cpi_80', 0)
        results['total_items_with_cost'] = budget_results.get('total_items_with_cost', 0)
    else:
        results['productivity_factor'] = None

    # --- Rework Indicators ---
    # Rework is indicated by: actual cost exceeding committed, negative progress,
    # or repeated cost code charges
    rework_score = 0
    rework_indicators = []

    if cost_df is not None and not cost_df.empty:
        # Items where actual exceeds committed (possible rework / re-do)
        over_committed = cost_df[
            (cost_df['actual_cost'] > cost_df['committed_cost']) &
            (cost_df['committed_cost'] > 0)
        ]
        over_committed_ratio = len(over_committed) / len(cost_df) if len(cost_df) > 0 else 0
        results['over_committed_ratio'] = over_committed_ratio

        if over_committed_ratio > 0.15:
            rework_indicators.append('High ratio of actual costs exceeding commitments')
            rework_score += 0.3

        # Items with low completion but high cost burn
        if budget_results:
            weighted_pct = budget_results.get('weighted_percent_complete', 0)
            if weighted_pct > 0:
                cost_burn_ratio = (cost_df['actual_cost'].sum() /
                                   cost_df['revised_budget'].sum()
                                   if cost_df['revised_budget'].sum() > 0 else 0)
                results['cost_burn_ratio'] = cost_burn_ratio
                if cost_burn_ratio > weighted_pct * 1.15:
                    rework_indicators.append('Cost burn rate exceeds physical progress')
                    rework_score += 0.3

        # Forecast exceeding revised budget indicates rework / inefficiency
        forecast_overrun = cost_df[
            cost_df['forecast_at_completion'] > cost_df['revised_budget'] * 1.05
        ]
        forecast_overrun_ratio = len(forecast_overrun) / len(cost_df) if len(cost_df) > 0 else 0
        results['forecast_overrun_ratio'] = forecast_overrun_ratio
        if forecast_overrun_ratio > 0.2:
            rework_indicators.append('Many items forecast over budget')
            rework_score += 0.2
    else:
        results['over_committed_ratio'] = 0
        results['forecast_overrun_ratio'] = 0

    # --- Schedule-based Operations Metrics ---
    if schedule_results:
        # BEI as operations indicator
        bei = schedule_results.get('bei')
        results['bei'] = bei

        # Out of sequence work
        oos_ratio = schedule_results.get('out_of_sequence_ratio', 0)
        results['out_of_sequence_ratio'] = oos_ratio
        if oos_ratio > 0.05:
            rework_indicators.append('Out-of-sequence work detected')
            rework_score += 0.2

        # Long activities may indicate poor planning or inefficiency
        long_ratio = schedule_results.get('long_activity_ratio', 0)
        results['long_activity_ratio'] = long_ratio
    else:
        results['bei'] = None
        results['out_of_sequence_ratio'] = 0

    results['rework_indicators'] = rework_indicators
    results['rework_score'] = min(rework_score, 1.0)

    # --- Overtime Indicator ---
    # Without direct labor data, we estimate from schedule pressure
    overtime_score = 0
    if schedule_results:
        bei = schedule_results.get('bei')
        neg_float = schedule_results.get('negative_float_count', 0)
        critical_ratio = schedule_results.get('critical_ratio', 0)

        # Schedule pressure indicators suggest overtime
        if bei is not None and bei < 0.80:
            overtime_score += 0.3
        if neg_float > 5:
            overtime_score += 0.3
        if critical_ratio > 0.30:
            overtime_score += 0.2

    results['overtime_indicator'] = min(overtime_score, 1.0)

    # --- Equipment Utilization Proxy ---
    # Without direct equipment data, we use schedule efficiency
    results['equipment_utilization'] = None  # requires specific data

    # --- Overall Operations Health ---
    factors = []
    if results.get('productivity_factor') is not None:
        factors.append(results['productivity_factor'])
    if results.get('bei') is not None:
        factors.append(results['bei'])

    if factors:
        results['operations_health'] = np.mean(factors)
    else:
        results['operations_health'] = None

    return results

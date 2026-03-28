"""
Conflict of Agency analysis module.
Detects indicators of misaligned incentives between contractor and owner.
"""

import pandas as pd
import numpy as np


def analyze_agency(cost_df: pd.DataFrame = None, change_df: pd.DataFrame = None,
                   budget_results: dict = None, change_results: dict = None) -> dict:
    """
    Perform conflict of agency analysis.
    Returns a dict of metrics.
    """
    results = {}

    # --- GC General Conditions Burn Ratio ---
    # Compare GC conditions spend vs overall project completion
    if cost_df is not None and not cost_df.empty:
        # Find general conditions line items (Division 01 or matching keywords)
        gc_mask = (
            cost_df['cost_code'].str.startswith('01', na=False) |
            cost_df['description'].str.lower().str.contains('general condition', na=False) |
            cost_df['description'].str.lower().str.contains('general requirement', na=False) |
            cost_df['cost_code'].str.lower().str.contains('gc', na=False)
        )
        gc_items = cost_df[gc_mask]

        if not gc_items.empty:
            gc_budget = gc_items['revised_budget'].sum()
            gc_actual = gc_items['actual_cost'].sum()
            gc_pct_spent = gc_actual / gc_budget if gc_budget > 0 else 0

            overall_pct = budget_results.get('weighted_percent_complete', 0) if budget_results else 0

            results['gc_budget'] = gc_budget
            results['gc_actual'] = gc_actual
            results['gc_pct_spent'] = gc_pct_spent

            if overall_pct > 0:
                results['gc_burn_ratio'] = gc_pct_spent / overall_pct
            else:
                results['gc_burn_ratio'] = gc_pct_spent * 5 if gc_pct_spent > 0 else None
        else:
            results['gc_burn_ratio'] = None
    else:
        results['gc_burn_ratio'] = None

    # --- CO Markup Patterns ---
    if change_results:
        avg_markup = change_results.get('avg_markup_pct')
        results['avg_co_markup_pct'] = avg_markup
        max_markup = change_results.get('max_markup_pct')
        results['max_co_markup_pct'] = max_markup

        # Flag excessive markup (> 15% is standard in most contracts)
        if avg_markup is not None:
            results['markup_excess'] = max(0, avg_markup - 15)
        else:
            results['markup_excess'] = None
    else:
        results['avg_co_markup_pct'] = None
        results['max_co_markup_pct'] = None
        results['markup_excess'] = None

    # --- Self-Perform CO Bias ---
    # Check if contractor-caused changes are disproportionately self-performed
    if change_results:
        type_dist = change_results.get('type_distribution', {})
        type_amounts = change_results.get('type_amounts', {})

        contractor_cos = type_dist.get('contractor', 0)
        total_cos = change_results.get('total_cos', 0)

        if total_cos > 0:
            results['contractor_co_ratio'] = contractor_cos / total_cos
        else:
            results['contractor_co_ratio'] = 0

        # Check if contractor-type COs have higher markup
        results['self_perform_bias_flag'] = False
        if change_df is not None and not change_df.empty:
            contractor_mask = change_df['type'].str.lower().isin(
                ['contractor', 'gc error', 'construction error', 'means and methods']
            )
            if contractor_mask.any() and change_df['markup_amount'].notna().any():
                contractor_markup = change_df.loc[contractor_mask, 'markup_amount'].mean()
                other_markup = change_df.loc[~contractor_mask, 'markup_amount'].mean()
                if pd.notna(contractor_markup) and pd.notna(other_markup) and other_markup > 0:
                    if contractor_markup > other_markup * 1.2:
                        results['self_perform_bias_flag'] = True
    else:
        results['contractor_co_ratio'] = 0
        results['self_perform_bias_flag'] = False

    # --- CO Fragmentation Near Approval Thresholds ---
    # Detect pattern of COs clustered just below approval thresholds
    if change_df is not None and not change_df.empty:
        amounts = change_df['amount'].dropna().abs()
        common_thresholds = [5000, 10000, 25000, 50000, 100000, 250000]

        fragmentation_flags = []
        for threshold in common_thresholds:
            # Count COs between 80-100% of threshold
            window_low = threshold * 0.80
            window_high = threshold * 1.0
            in_window = ((amounts >= window_low) & (amounts <= window_high)).sum()
            above = (amounts > threshold).sum()

            if in_window >= 3 and above < in_window:
                fragmentation_flags.append({
                    'threshold': threshold,
                    'count_near': int(in_window),
                    'count_above': int(above),
                })

        results['fragmentation_flags'] = fragmentation_flags
        results['fragmentation_detected'] = len(fragmentation_flags) > 0
    else:
        results['fragmentation_flags'] = []
        results['fragmentation_detected'] = False

    # --- Billing Rate Analysis (T&M proxy) ---
    # Without direct T&M data, flag if GC conditions overrun suggests billing issues
    if results.get('gc_burn_ratio') is not None and results['gc_burn_ratio'] > 1.3:
        results['billing_rate_flag'] = True
    else:
        results['billing_rate_flag'] = False

    # --- Fee Acceleration ---
    # Check if contractor fee/profit is growing faster than project progress
    if cost_df is not None and not cost_df.empty and budget_results:
        fee_mask = (
            cost_df['description'].str.lower().str.contains('fee', na=False) |
            cost_df['description'].str.lower().str.contains('profit', na=False) |
            cost_df['description'].str.lower().str.contains('overhead', na=False) |
            cost_df['cost_type'].str.lower().str.contains('fee', na=False)
        )
        fee_items = cost_df[fee_mask]

        if not fee_items.empty:
            fee_budget = fee_items['revised_budget'].sum()
            fee_actual = fee_items['actual_cost'].sum()
            overall_pct = budget_results.get('weighted_percent_complete', 0)

            if fee_budget > 0 and overall_pct > 0:
                fee_burn_pct = fee_actual / fee_budget
                results['fee_acceleration'] = fee_burn_pct / overall_pct
            else:
                results['fee_acceleration'] = None
        else:
            results['fee_acceleration'] = None
    else:
        results['fee_acceleration'] = None

    # --- Round Number Timesheet Flag ---
    # Not directly measurable from budget data, but flag if GC conditions
    # show suspiciously even billing patterns
    results['round_number_timesheet_flag'] = False

    # --- Sole Source Indicator ---
    # Check if committed costs are concentrated
    if cost_df is not None and not cost_df.empty:
        committed = cost_df[cost_df['committed_cost'] > 0]
        if len(committed) > 0:
            total_committed = committed['committed_cost'].sum()
            max_single = committed['committed_cost'].max()
            if total_committed > 0:
                results['max_commitment_concentration'] = max_single / total_committed
            else:
                results['max_commitment_concentration'] = 0
        else:
            results['max_commitment_concentration'] = 0
    else:
        results['max_commitment_concentration'] = 0

    return results

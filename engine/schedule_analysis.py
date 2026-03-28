"""
Schedule analysis module.
Computes critical path metrics, float analysis, logic quality, BEI, etc.
"""

import pandas as pd
import numpy as np
from datetime import datetime


def analyze_schedule(schedule_df: pd.DataFrame) -> dict:
    """
    Perform comprehensive schedule analysis on normalized schedule data.
    Returns a dict of metrics.
    """
    results = {}

    if schedule_df is None or schedule_df.empty:
        return results

    total_activities = len(schedule_df)
    results['total_activities'] = total_activities

    # --- Critical Activity Ratio ---
    critical_count = schedule_df['is_critical'].sum()
    results['critical_count'] = int(critical_count)
    results['critical_ratio'] = critical_count / total_activities if total_activities > 0 else 0

    # --- Near-Critical Ratio (float < 10 days) ---
    has_float = schedule_df['total_float'].notna()
    if has_float.any():
        near_critical = ((schedule_df['total_float'] >= 0) & (schedule_df['total_float'] < 10) & ~schedule_df['is_critical'])
        results['near_critical_count'] = int(near_critical.sum())
        results['near_critical_ratio'] = near_critical.sum() / total_activities
    else:
        results['near_critical_count'] = 0
        results['near_critical_ratio'] = None

    # --- Negative Float ---
    if has_float.any():
        negative_float = schedule_df['total_float'] < 0
        results['negative_float_count'] = int(negative_float.sum())
        if negative_float.any():
            results['max_negative_float'] = int(schedule_df.loc[negative_float, 'total_float'].min())
        else:
            results['max_negative_float'] = 0
    else:
        results['negative_float_count'] = 0
        results['max_negative_float'] = 0

    # --- Logic Density (relationships per activity) ---
    has_preds = schedule_df['predecessors'].notna() & (schedule_df['predecessors'] != '')
    pred_count = 0
    for _, row in schedule_df.iterrows():
        preds = str(row['predecessors']).strip()
        if preds and preds != '' and preds.lower() != 'nan':
            # Count comma or semicolon separated predecessors
            parts = [p.strip() for p in preds.replace(';', ',').split(',') if p.strip()]
            pred_count += len(parts)
    results['total_relationships'] = pred_count
    results['logic_density'] = pred_count / total_activities if total_activities > 0 else 0

    # --- Missing Predecessors/Successors ---
    missing_preds = (~has_preds).sum()
    results['missing_predecessors_count'] = int(missing_preds)
    results['missing_predecessors_ratio'] = missing_preds / total_activities if total_activities > 0 else 0

    # Missing successors: activities not referenced as predecessors by any other activity
    all_pred_ids = set()
    for _, row in schedule_df.iterrows():
        preds = str(row['predecessors']).strip()
        if preds and preds.lower() != 'nan':
            for p in preds.replace(';', ',').split(','):
                # Extract just the ID (remove FS, SS, FF, SF suffixes and lag)
                pid = p.strip().split('F')[0].split('S')[0].strip()
                if pid:
                    all_pred_ids.add(pid)

    activity_ids = set(schedule_df['activity_id'].astype(str))
    # Activities that are never someone's predecessor = missing successors
    missing_succ = activity_ids - all_pred_ids
    results['missing_successors_count'] = len(missing_succ)
    results['missing_successors_ratio'] = len(missing_succ) / total_activities if total_activities > 0 else 0

    # --- Hard Constraint Ratio ---
    has_constraints = schedule_df['constraint_type'].notna() & (schedule_df['constraint_type'] != '') & (schedule_df['constraint_type'].str.lower() != 'none')
    results['hard_constraint_count'] = int(has_constraints.sum())
    results['hard_constraint_ratio'] = has_constraints.sum() / total_activities if total_activities > 0 else 0

    # --- Long Activity Ratio (> 20 days) ---
    if schedule_df['original_duration'].notna().any():
        long_activities = schedule_df['original_duration'] > 20
        results['long_activity_count'] = int(long_activities.sum())
        results['long_activity_ratio'] = long_activities.sum() / total_activities
    else:
        results['long_activity_count'] = 0
        results['long_activity_ratio'] = 0

    # --- Out-of-Sequence Detection ---
    # Activities with actual_start before early_start by more than predecessor logic allows
    oos_count = 0
    if schedule_df['actual_start'].notna().any() and schedule_df['early_start'].notna().any():
        for _, row in schedule_df.iterrows():
            if pd.notna(row['actual_start']) and pd.notna(row['early_start']):
                if row['actual_start'] < row['early_start'] and str(row['predecessors']).strip() not in ('', 'nan'):
                    oos_count += 1
    results['out_of_sequence_count'] = oos_count
    results['out_of_sequence_ratio'] = oos_count / total_activities if total_activities > 0 else 0

    # --- Baseline Execution Index (BEI) ---
    # Tasks that should be complete (early_finish <= today) vs tasks actually complete
    today = pd.Timestamp.now()
    should_be_complete = schedule_df[
        schedule_df['early_finish'].notna() & (schedule_df['early_finish'] <= today)
    ]
    if len(should_be_complete) > 0:
        actually_complete = should_be_complete[
            (should_be_complete['percent_complete'] >= 0.95) |
            (should_be_complete['actual_finish'].notna())
        ]
        results['bei'] = len(actually_complete) / len(should_be_complete)
        results['should_be_complete'] = len(should_be_complete)
        results['actually_complete'] = len(actually_complete)
    else:
        results['bei'] = None
        results['should_be_complete'] = 0
        results['actually_complete'] = 0

    # --- Float Distribution ---
    if has_float.any():
        float_data = schedule_df['total_float'].dropna()
        results['avg_float'] = float_data.mean()
        results['median_float'] = float_data.median()
        results['min_float'] = float_data.min()
        results['max_float'] = float_data.max()

        # Float buckets
        results['float_negative'] = int((float_data < 0).sum())
        results['float_0_5'] = int(((float_data >= 0) & (float_data <= 5)).sum())
        results['float_5_10'] = int(((float_data > 5) & (float_data <= 10)).sum())
        results['float_10_20'] = int(((float_data > 10) & (float_data <= 20)).sum())
        results['float_20_plus'] = int((float_data > 20).sum())
    else:
        results['avg_float'] = None
        results['median_float'] = None

    # --- Completion summary ---
    results['avg_percent_complete'] = schedule_df['percent_complete'].mean()
    complete = schedule_df['percent_complete'] >= 0.95
    results['complete_count'] = int(complete.sum())
    in_progress = (schedule_df['percent_complete'] > 0) & (schedule_df['percent_complete'] < 0.95)
    results['in_progress_count'] = int(in_progress.sum())
    not_started = schedule_df['percent_complete'] <= 0
    results['not_started_count'] = int(not_started.sum())

    return results

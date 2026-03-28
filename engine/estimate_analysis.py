"""
Estimate quality analysis module.
Checks scope coverage, unit cost outliers, division distribution,
and missing common scope items.
"""

import pandas as pd
import numpy as np


# Benchmark division distribution for commercial construction (CSI-based)
# These represent typical % of total project cost per division
BENCHMARK_DISTRIBUTION = {
    '01': {'name': 'General Requirements', 'low': 0.05, 'typical': 0.10, 'high': 0.15},
    '02': {'name': 'Existing Conditions', 'low': 0.01, 'typical': 0.03, 'high': 0.06},
    '03': {'name': 'Concrete', 'low': 0.05, 'typical': 0.10, 'high': 0.18},
    '04': {'name': 'Masonry', 'low': 0.01, 'typical': 0.04, 'high': 0.08},
    '05': {'name': 'Metals', 'low': 0.03, 'typical': 0.07, 'high': 0.12},
    '06': {'name': 'Wood/Plastics/Composites', 'low': 0.02, 'typical': 0.05, 'high': 0.10},
    '07': {'name': 'Thermal & Moisture', 'low': 0.02, 'typical': 0.05, 'high': 0.08},
    '08': {'name': 'Openings', 'low': 0.03, 'typical': 0.06, 'high': 0.10},
    '09': {'name': 'Finishes', 'low': 0.05, 'typical': 0.10, 'high': 0.15},
    '10': {'name': 'Specialties', 'low': 0.01, 'typical': 0.02, 'high': 0.04},
    '11': {'name': 'Equipment', 'low': 0.01, 'typical': 0.03, 'high': 0.08},
    '12': {'name': 'Furnishings', 'low': 0.01, 'typical': 0.02, 'high': 0.05},
    '13': {'name': 'Special Construction', 'low': 0.00, 'typical': 0.02, 'high': 0.05},
    '14': {'name': 'Conveying Systems', 'low': 0.01, 'typical': 0.03, 'high': 0.06},
    '21': {'name': 'Fire Suppression', 'low': 0.01, 'typical': 0.03, 'high': 0.05},
    '22': {'name': 'Plumbing', 'low': 0.03, 'typical': 0.06, 'high': 0.10},
    '23': {'name': 'HVAC', 'low': 0.06, 'typical': 0.12, 'high': 0.18},
    '26': {'name': 'Electrical', 'low': 0.08, 'typical': 0.14, 'high': 0.20},
    '27': {'name': 'Communications', 'low': 0.01, 'typical': 0.03, 'high': 0.06},
    '28': {'name': 'Electronic Safety', 'low': 0.01, 'typical': 0.02, 'high': 0.04},
    '31': {'name': 'Earthwork', 'low': 0.02, 'typical': 0.05, 'high': 0.10},
    '32': {'name': 'Exterior Improvements', 'low': 0.02, 'typical': 0.04, 'high': 0.08},
    '33': {'name': 'Utilities', 'low': 0.02, 'typical': 0.04, 'high': 0.08},
}

# Common scope items that should appear in most commercial projects
COMMON_SCOPE_ITEMS = [
    'general_conditions', 'sitework', 'concrete', 'structural_steel',
    'roofing', 'electrical', 'plumbing', 'hvac', 'fire_protection',
    'drywall', 'painting', 'flooring', 'doors_hardware', 'elevator',
    'demolition', 'earthwork', 'landscaping', 'utilities'
]

SCOPE_ITEM_KEYWORDS = {
    'general_conditions': ['general conditions', 'general requirements', 'div 01', 'division 01'],
    'sitework': ['sitework', 'site work', 'site preparation', 'grading'],
    'concrete': ['concrete', 'foundations', 'footings', 'slab'],
    'structural_steel': ['structural steel', 'steel', 'metals', 'structural'],
    'roofing': ['roofing', 'roof', 'waterproofing', 'membrane'],
    'electrical': ['electrical', 'power', 'lighting', 'div 26', 'division 26'],
    'plumbing': ['plumbing', 'piping', 'domestic water', 'sanitary'],
    'hvac': ['hvac', 'mechanical', 'heating', 'cooling', 'air conditioning', 'ventilation'],
    'fire_protection': ['fire protection', 'fire suppression', 'sprinkler', 'fire alarm'],
    'drywall': ['drywall', 'gypsum', 'framing', 'metal studs', 'partitions'],
    'painting': ['painting', 'coatings', 'wall covering'],
    'flooring': ['flooring', 'carpet', 'tile', 'terrazzo', 'resilient'],
    'doors_hardware': ['doors', 'hardware', 'frames', 'openings'],
    'elevator': ['elevator', 'conveying', 'escalator', 'lift'],
    'demolition': ['demolition', 'demo', 'abatement', 'removal'],
    'earthwork': ['earthwork', 'excavation', 'backfill', 'grading'],
    'landscaping': ['landscaping', 'landscape', 'irrigation', 'planting'],
    'utilities': ['utilities', 'underground', 'storm drain', 'sanitary sewer', 'water main'],
}


def analyze_estimate(cost_df: pd.DataFrame) -> dict:
    """
    Perform estimate quality analysis on normalized cost data.
    Returns a dict of metrics.
    """
    results = {}

    if cost_df is None or cost_df.empty:
        return results

    total_budget = cost_df['original_budget'].sum()
    results['total_budget'] = total_budget

    # --- Scope Coverage Ratio ---
    # Check how many common scope items are present in the cost data
    descriptions = ' '.join(
        cost_df['description'].fillna('').str.lower().tolist() +
        cost_df['cost_code'].fillna('').str.lower().tolist()
    )

    found_items = []
    missing_items = []
    for item, keywords in SCOPE_ITEM_KEYWORDS.items():
        found = any(kw in descriptions for kw in keywords)
        if found:
            found_items.append(item)
        else:
            missing_items.append(item)

    results['scope_items_found'] = found_items
    results['scope_items_missing'] = missing_items
    results['scope_coverage_ratio'] = len(found_items) / len(COMMON_SCOPE_ITEMS) if COMMON_SCOPE_ITEMS else 1.0

    # --- Division Cost Distribution vs Benchmarks ---
    division_analysis = []
    if cost_df['cost_code'].notna().any():
        grouped = cost_df.groupby('cost_code')['original_budget'].sum()
        for code, amount in grouped.items():
            pct = amount / total_budget if total_budget > 0 else 0
            # Try to match to CSI division
            code_str = str(code).strip()
            div_code = code_str[:2] if len(code_str) >= 2 else code_str
            benchmark = BENCHMARK_DISTRIBUTION.get(div_code)

            entry = {
                'cost_code': code_str,
                'amount': amount,
                'pct_of_total': pct,
            }

            if benchmark:
                entry['benchmark_name'] = benchmark['name']
                entry['benchmark_low'] = benchmark['low']
                entry['benchmark_typical'] = benchmark['typical']
                entry['benchmark_high'] = benchmark['high']
                if pct < benchmark['low']:
                    entry['flag'] = 'below_range'
                elif pct > benchmark['high']:
                    entry['flag'] = 'above_range'
                else:
                    entry['flag'] = 'in_range'
            else:
                entry['flag'] = 'no_benchmark'

            division_analysis.append(entry)

    results['division_analysis'] = division_analysis

    # Count outliers
    outlier_count = sum(1 for d in division_analysis if d['flag'] in ('below_range', 'above_range'))
    results['division_outlier_count'] = outlier_count
    results['division_outlier_ratio'] = outlier_count / len(division_analysis) if division_analysis else 0

    # --- Unit Cost Outlier Detection (within each cost type) ---
    # Detect line items where cost/budget is significantly different from peers
    if cost_df['cost_type'].notna().any() and (cost_df['cost_type'] != '').any():
        outlier_items = []
        for cost_type, group in cost_df.groupby('cost_type'):
            if len(group) < 3:
                continue
            budgets = group['original_budget'].dropna()
            if budgets.empty:
                continue
            mean_val = budgets.mean()
            std_val = budgets.std()
            if std_val > 0:
                z_scores = (budgets - mean_val) / std_val
                outliers = group[z_scores.abs() > 2]
                for _, row in outliers.iterrows():
                    outlier_items.append({
                        'cost_code': row['cost_code'],
                        'description': row['description'],
                        'amount': row['original_budget'],
                        'cost_type': cost_type,
                        'z_score': float((row['original_budget'] - mean_val) / std_val),
                    })
        results['unit_cost_outliers'] = outlier_items
    else:
        results['unit_cost_outliers'] = []

    # --- Line item count quality ---
    results['total_line_items'] = len(cost_df)
    results['avg_line_item_value'] = total_budget / len(cost_df) if len(cost_df) > 0 else 0

    # Check for suspiciously large line items (> 15% of total)
    if total_budget > 0:
        large_items = cost_df[cost_df['original_budget'] / total_budget > 0.15]
        results['large_line_items'] = len(large_items)
    else:
        results['large_line_items'] = 0

    # Check for round number prevalence (indicator of rough estimates)
    round_numbers = cost_df['original_budget'].apply(
        lambda x: x > 0 and (x % 1000 == 0 or x % 5000 == 0 or x % 10000 == 0)
    )
    results['round_number_count'] = int(round_numbers.sum())
    results['round_number_ratio'] = round_numbers.sum() / len(cost_df) if len(cost_df) > 0 else 0

    return results

"""
Composite failure mode scoring engine.
Maps raw metrics to 0-100 severity scores for each of the four failure modes.
"""

import numpy as np


def normalize(value, green_threshold, yellow_threshold, red_threshold, critical_threshold,
              invert=False):
    """
    Map a metric value to a 0-1 severity scale.
    0.0 = Green, 0.33 = Yellow, 0.67 = Red, 1.0 = Critical

    If invert=True, higher values are better (e.g., CPI, BEI).
    If invert=False, higher values are worse (e.g., CO rate, aging).
    """
    if value is None:
        return None

    if invert:
        # Higher is better: Green > green_threshold
        if value >= green_threshold:
            return 0.0
        elif value >= yellow_threshold:
            # Interpolate between 0.0 and 0.33
            return 0.33 * (green_threshold - value) / max(green_threshold - yellow_threshold, 0.001)
        elif value >= red_threshold:
            return 0.33 + 0.34 * (yellow_threshold - value) / max(yellow_threshold - red_threshold, 0.001)
        elif value >= critical_threshold:
            return 0.67 + 0.33 * (red_threshold - value) / max(red_threshold - critical_threshold, 0.001)
        else:
            return 1.0
    else:
        # Higher is worse: Green < green_threshold
        if value <= green_threshold:
            return 0.0
        elif value <= yellow_threshold:
            return 0.33 * (value - green_threshold) / max(yellow_threshold - green_threshold, 0.001)
        elif value <= red_threshold:
            return 0.33 + 0.34 * (value - yellow_threshold) / max(red_threshold - yellow_threshold, 0.001)
        elif value <= critical_threshold:
            return 0.67 + 0.33 * (value - red_threshold) / max(critical_threshold - red_threshold, 0.001)
        else:
            return 1.0


def severity_label(score_0_1):
    """Convert 0-1 score to severity label."""
    if score_0_1 is None:
        return 'N/A'
    if score_0_1 < 0.17:
        return 'Green'
    elif score_0_1 < 0.50:
        return 'Yellow'
    elif score_0_1 < 0.83:
        return 'Red'
    else:
        return 'Critical'


def weighted_score(components: list) -> float:
    """
    Compute weighted score from components.
    Each component is (weight, normalized_value_or_None).
    Missing values (None) have their weight redistributed proportionally.
    Returns score on 0-100 scale.
    """
    available = [(w, v) for w, v in components if v is not None]
    if not available:
        return 0.0

    total_weight = sum(w for w, _ in available)
    if total_weight <= 0:
        return 0.0

    raw = sum(w * v for w, v in available) / total_weight
    return round(raw * 100, 1)


def score_bad_estimate(budget_results: dict, change_results: dict,
                       estimate_results: dict, schedule_results: dict) -> dict:
    """
    Score the 'Bad Estimate at Origin' failure mode.
    """
    components = []
    details = []

    # Early CPI deficit (weight 0.25)
    cpi = budget_results.get('cpi')
    cpi_norm = normalize(cpi, 0.95, 0.90, 0.80, 0.70, invert=True) if cpi else None
    components.append((0.25, cpi_norm))
    if cpi is not None:
        details.append({
            'metric': 'Cost Performance Index (CPI)',
            'value': round(cpi, 3),
            'threshold': 'Green > 0.95, Yellow 0.90-0.95, Red 0.80-0.90, Critical < 0.80',
            'severity': severity_label(cpi_norm),
            'normalized': cpi_norm,
            'explanation': f'CPI of {cpi:.3f} means for every $1 spent, only ${cpi:.2f} of value is earned.'
        })

    # Unit cost vs benchmark gap (weight 0.20)
    outlier_ratio = estimate_results.get('division_outlier_ratio', 0)
    outlier_norm = normalize(outlier_ratio, 0.10, 0.20, 0.35, 0.50)
    components.append((0.20, outlier_norm))
    if estimate_results:
        details.append({
            'metric': 'Division Cost Outlier Ratio',
            'value': f'{outlier_ratio:.0%}',
            'threshold': 'Green < 10%, Yellow 10-20%, Red 20-35%, Critical > 50%',
            'severity': severity_label(outlier_norm),
            'normalized': outlier_norm,
            'explanation': f'{outlier_ratio:.0%} of cost divisions fall outside benchmark ranges.'
        })

    # Missing scope items (weight 0.20)
    scope_ratio = estimate_results.get('scope_coverage_ratio', 1.0)
    scope_norm = normalize(scope_ratio, 0.90, 0.80, 0.70, 0.55, invert=True)
    components.append((0.20, scope_norm))
    if estimate_results:
        missing = estimate_results.get('scope_items_missing', [])
        details.append({
            'metric': 'Scope Coverage Ratio',
            'value': f'{scope_ratio:.0%}',
            'threshold': 'Green > 90%, Yellow 80-90%, Red 70-80%, Critical < 55%',
            'severity': severity_label(scope_norm),
            'normalized': scope_norm,
            'explanation': f'{len(missing)} common scope items not found in budget: {", ".join(missing[:5])}'
            if missing else 'All common scope items present.'
        })

    # RFI rate excess (weight 0.15) — proxy via design error COs
    design_err = change_results.get('design_error_ratio', 0)
    rfi_norm = normalize(design_err, 0.10, 0.20, 0.35, 0.50)
    components.append((0.15, rfi_norm if change_results else None))
    if change_results:
        details.append({
            'metric': 'Design Error Change Order Ratio',
            'value': f'{design_err:.0%}',
            'threshold': 'Green < 10%, Yellow 10-20%, Red 20-35%, Critical > 50%',
            'severity': severity_label(rfi_norm),
            'normalized': rfi_norm,
            'explanation': f'{design_err:.0%} of non-owner change orders are due to design errors.'
        })

    # Design error CO ratio (weight 0.10) — already captured, use CO rate
    co_rate = change_results.get('co_rate', 0)
    co_norm = normalize(co_rate, 5, 10, 20, 30) if co_rate is not None else None
    components.append((0.10, co_norm))
    if co_rate is not None:
        details.append({
            'metric': 'Change Order Rate',
            'value': f'{co_rate:.1f}%',
            'threshold': 'Green < 5%, Yellow 5-10%, Red 10-20%, Critical > 20%',
            'severity': severity_label(co_norm),
            'normalized': co_norm,
            'explanation': f'Change orders total {co_rate:.1f}% of original contract value.'
        })

    # Round number estimate indicator (weight 0.10)
    round_ratio = estimate_results.get('round_number_ratio', 0)
    round_norm = normalize(round_ratio, 0.30, 0.50, 0.70, 0.85)
    components.append((0.10, round_norm if estimate_results else None))
    if estimate_results:
        details.append({
            'metric': 'Round Number Estimate Ratio',
            'value': f'{round_ratio:.0%}',
            'threshold': 'Green < 30%, Yellow 30-50%, Red 50-70%, Critical > 85%',
            'severity': severity_label(round_norm),
            'normalized': round_norm,
            'explanation': f'{round_ratio:.0%} of line items have round number budgets, suggesting rough estimates.'
        })

    score = weighted_score(components)
    return {
        'score': score,
        'label': severity_label(score / 100),
        'details': details,
    }


def score_inefficient_ops(budget_results: dict, schedule_results: dict,
                          operations_results: dict) -> dict:
    """
    Score the 'Inefficient Operations' failure mode.
    """
    components = []
    details = []

    # Productivity factor deficit (weight 0.25)
    prod = operations_results.get('productivity_factor')
    prod_norm = normalize(prod, 0.95, 0.85, 0.75, 0.65, invert=True) if prod else None
    components.append((0.25, prod_norm))
    if prod is not None:
        details.append({
            'metric': 'Productivity Factor (CPI proxy)',
            'value': round(prod, 3),
            'threshold': 'Green > 0.95, Yellow 0.85-0.95, Red 0.75-0.85, Critical < 0.65',
            'severity': severity_label(prod_norm),
            'normalized': prod_norm,
            'explanation': f'Productivity factor of {prod:.3f} indicates '
                           f'{"efficient" if prod > 0.95 else "inefficient"} field operations.'
        })

    # Rework rate (weight 0.20)
    rework = operations_results.get('rework_score', 0)
    rework_norm = normalize(rework, 0.10, 0.25, 0.50, 0.75)
    components.append((0.20, rework_norm))
    details.append({
        'metric': 'Rework Indicator Score',
        'value': f'{rework:.0%}',
        'threshold': 'Green < 10%, Yellow 10-25%, Red 25-50%, Critical > 75%',
        'severity': severity_label(rework_norm),
        'normalized': rework_norm,
        'explanation': '; '.join(operations_results.get('rework_indicators', ['No rework indicators detected.']))
    })

    # Overtime indicator (weight 0.15)
    overtime = operations_results.get('overtime_indicator', 0)
    ot_norm = normalize(overtime, 0.10, 0.25, 0.50, 0.75)
    components.append((0.15, ot_norm))
    details.append({
        'metric': 'Overtime Pressure Indicator',
        'value': f'{overtime:.0%}',
        'threshold': 'Green < 10%, Yellow 10-25%, Red 25-50%, Critical > 75%',
        'severity': severity_label(ot_norm),
        'normalized': ot_norm,
        'explanation': 'Schedule pressure indicators suggest '
                       f'{"significant" if overtime > 0.5 else "moderate" if overtime > 0.25 else "low"} overtime.'
    })

    # BEI deficit (weight 0.15)
    bei = schedule_results.get('bei')
    bei_norm = normalize(bei, 0.90, 0.80, 0.70, 0.60, invert=True) if bei else None
    components.append((0.15, bei_norm))
    if bei is not None:
        details.append({
            'metric': 'Baseline Execution Index (BEI)',
            'value': round(bei, 3),
            'threshold': 'Green > 0.90, Yellow 0.80-0.90, Red 0.70-0.80, Critical < 0.70',
            'severity': severity_label(bei_norm),
            'normalized': bei_norm,
            'explanation': f'BEI of {bei:.3f}: {bei:.0%} of tasks due are completed on time.'
        })

    # Equipment idle rate (weight 0.10) — often unavailable
    components.append((0.10, None))  # usually no data

    # Out of sequence ratio (weight 0.05)
    oos = schedule_results.get('out_of_sequence_ratio', 0)
    oos_norm = normalize(oos, 0.02, 0.05, 0.10, 0.20)
    components.append((0.05, oos_norm if schedule_results else None))
    if schedule_results:
        details.append({
            'metric': 'Out-of-Sequence Activity Ratio',
            'value': f'{oos:.0%}',
            'threshold': 'Green < 2%, Yellow 2-5%, Red 5-10%, Critical > 20%',
            'severity': severity_label(oos_norm),
            'normalized': oos_norm,
            'explanation': f'{oos:.0%} of activities started out of logical sequence.'
        })

    # Weather excess impact (weight 0.10) — usually no data
    components.append((0.10, None))

    score = weighted_score(components)
    return {
        'score': score,
        'label': severity_label(score / 100),
        'details': details,
    }


def score_failure_to_capture_change(budget_results: dict, change_results: dict) -> dict:
    """
    Score the 'Failure to Capture Change' failure mode.
    """
    components = []
    details = []

    # Pending CO backlog (weight 0.25)
    backlog = change_results.get('pending_backlog_pct')
    backlog_norm = normalize(backlog, 2, 5, 10, 20) if backlog is not None else None
    components.append((0.25, backlog_norm))
    if backlog is not None:
        details.append({
            'metric': 'Pending CO Backlog',
            'value': f'{backlog:.1f}%',
            'threshold': 'Green < 2%, Yellow 2-5%, Red 5-10%, Critical > 10%',
            'severity': severity_label(backlog_norm),
            'normalized': backlog_norm,
            'explanation': f'Pending change orders total {backlog:.1f}% of original contract.'
        })

    # Cost creep without COs (weight 0.20)
    # Compare actual cost growth vs approved change growth
    if budget_results and change_results:
        total_ac = budget_results.get('total_actual_cost', 0)
        total_budget = budget_results.get('total_original_budget', 0)
        approved_changes = budget_results.get('total_approved_changes', 0)

        if total_budget > 0:
            cost_growth = (total_ac - total_budget) / total_budget if total_ac > total_budget else 0
            change_coverage = approved_changes / total_budget if approved_changes > 0 else 0
            cost_creep = max(0, cost_growth - change_coverage)
            creep_norm = normalize(cost_creep, 0.02, 0.05, 0.10, 0.20)
            components.append((0.20, creep_norm))
            details.append({
                'metric': 'Uncovered Cost Creep',
                'value': f'{cost_creep:.1%}',
                'threshold': 'Green < 2%, Yellow 2-5%, Red 5-10%, Critical > 20%',
                'severity': severity_label(creep_norm),
                'normalized': creep_norm,
                'explanation': f'Cost growth of {cost_growth:.1%} exceeds approved change coverage of {change_coverage:.1%}.'
            })
        else:
            components.append((0.20, None))
    else:
        components.append((0.20, None))

    # CO conversion time (weight 0.20)
    avg_age = change_results.get('avg_pending_age', 0)
    age_norm = normalize(avg_age, 21, 30, 60, 90)
    components.append((0.20, age_norm if change_results else None))
    if change_results:
        details.append({
            'metric': 'Average Pending CO Age',
            'value': f'{avg_age:.0f} days',
            'threshold': 'Green < 21 days, Yellow 21-30, Red 30-60, Critical > 60',
            'severity': severity_label(age_norm),
            'normalized': age_norm,
            'explanation': f'Pending COs average {avg_age:.0f} days old.'
        })

    # CO aging excess (weight 0.15)
    cos_over_60 = change_results.get('cos_over_60_days', 0)
    total_cos = change_results.get('total_cos', 1)
    aged_ratio = cos_over_60 / total_cos if total_cos > 0 else 0
    aged_norm = normalize(aged_ratio, 0.05, 0.10, 0.20, 0.35)
    components.append((0.15, aged_norm if change_results else None))
    if change_results:
        details.append({
            'metric': 'Severely Aged COs (>60 days)',
            'value': f'{cos_over_60} of {total_cos}',
            'threshold': 'Green < 5%, Yellow 5-10%, Red 10-20%, Critical > 35%',
            'severity': severity_label(aged_norm),
            'normalized': aged_norm,
            'explanation': f'{cos_over_60} change orders have been pending over 60 days.'
        })

    # Contingency burn ratio excess (weight 0.10)
    contingency = budget_results.get('contingency_burn_ratio', 0)
    cont_norm = normalize(contingency, 1.2, 1.5, 2.0, 3.0) if budget_results else None
    components.append((0.10, cont_norm))
    if budget_results and contingency > 0:
        details.append({
            'metric': 'Contingency Burn Ratio',
            'value': f'{contingency:.2f}',
            'threshold': 'Green < 1.2, Yellow 1.2-1.5, Red 1.5-2.0, Critical > 2.0',
            'severity': severity_label(cont_norm),
            'normalized': cont_norm,
            'explanation': f'Contingency is being consumed {contingency:.1f}x faster than project progress.'
        })

    # Verbal direction ratio (weight 0.10) — usually no data
    components.append((0.10, None))

    score = weighted_score(components)
    return {
        'score': score,
        'label': severity_label(score / 100),
        'details': details,
    }


def score_conflict_of_agency(agency_results: dict, change_results: dict,
                              budget_results: dict) -> dict:
    """
    Score the 'Conflict of Agency' failure mode.
    """
    components = []
    details = []

    # Billing rate premium (weight 0.20) — proxy via GC burn
    gc_burn = agency_results.get('gc_burn_ratio')
    billing_norm = normalize(gc_burn, 1.1, 1.3, 1.6, 2.0) if gc_burn else None
    components.append((0.20, billing_norm))
    if gc_burn is not None:
        details.append({
            'metric': 'GC Conditions Burn Rate',
            'value': f'{gc_burn:.2f}',
            'threshold': 'Green < 1.1, Yellow 1.1-1.3, Red 1.3-1.6, Critical > 2.0',
            'severity': severity_label(billing_norm),
            'normalized': billing_norm,
            'explanation': f'GC conditions spending at {gc_burn:.1f}x the rate of project progress.'
        })

    # GC conditions burn excess (weight 0.15) — same metric, different angle
    components.append((0.15, billing_norm))

    # CO markup excess (weight 0.15)
    markup = agency_results.get('avg_co_markup_pct')
    markup_norm = normalize(markup, 12, 15, 20, 30) if markup else None
    components.append((0.15, markup_norm))
    if markup is not None:
        details.append({
            'metric': 'Average CO Markup',
            'value': f'{markup:.1f}%',
            'threshold': 'Green < 12%, Yellow 12-15%, Red 15-20%, Critical > 30%',
            'severity': severity_label(markup_norm),
            'normalized': markup_norm,
            'explanation': f'Average change order markup of {markup:.1f}% applied to direct costs.'
        })

    # Self-perform CO bias (weight 0.15)
    contractor_ratio = agency_results.get('contractor_co_ratio', 0)
    self_norm = normalize(contractor_ratio, 0.10, 0.20, 0.35, 0.50)
    components.append((0.15, self_norm))
    details.append({
        'metric': 'Contractor-Caused CO Ratio',
        'value': f'{contractor_ratio:.0%}',
        'threshold': 'Green < 10%, Yellow 10-20%, Red 20-35%, Critical > 50%',
        'severity': severity_label(self_norm),
        'normalized': self_norm,
        'explanation': f'{contractor_ratio:.0%} of COs are contractor-originated.'
    })

    # Sole source excess (weight 0.10)
    concentration = agency_results.get('max_commitment_concentration', 0)
    sole_norm = normalize(concentration, 0.20, 0.30, 0.45, 0.60)
    components.append((0.10, sole_norm))
    details.append({
        'metric': 'Max Commitment Concentration',
        'value': f'{concentration:.0%}',
        'threshold': 'Green < 20%, Yellow 20-30%, Red 30-45%, Critical > 60%',
        'severity': severity_label(sole_norm),
        'normalized': sole_norm,
        'explanation': f'Largest single commitment is {concentration:.0%} of total committed costs.'
    })

    # Fee acceleration (weight 0.10)
    fee_accel = agency_results.get('fee_acceleration')
    fee_norm = normalize(fee_accel, 1.1, 1.3, 1.6, 2.0) if fee_accel else None
    components.append((0.10, fee_norm))
    if fee_accel is not None:
        details.append({
            'metric': 'Fee Acceleration Factor',
            'value': f'{fee_accel:.2f}',
            'threshold': 'Green < 1.1, Yellow 1.1-1.3, Red 1.3-1.6, Critical > 2.0',
            'severity': severity_label(fee_norm),
            'normalized': fee_norm,
            'explanation': f'Contractor fees billing at {fee_accel:.1f}x the rate of project progress.'
        })

    # CO fragmentation (weight 0.10)
    frag = agency_results.get('fragmentation_detected', False)
    frag_norm = 0.67 if frag else 0.0
    components.append((0.10, frag_norm))
    details.append({
        'metric': 'CO Fragmentation Pattern',
        'value': 'Detected' if frag else 'Not Detected',
        'threshold': 'Pattern of COs clustering below approval thresholds',
        'severity': 'Red' if frag else 'Green',
        'normalized': frag_norm,
        'explanation': 'Change orders show clustering below approval thresholds.'
        if frag else 'No suspicious CO fragmentation patterns detected.'
    })

    # Round number timesheet (weight 0.05) — usually no data
    components.append((0.05, None))

    score = weighted_score(components)
    return {
        'score': score,
        'label': severity_label(score / 100),
        'details': details,
    }


def compute_all_scores(budget_results, schedule_results, change_results,
                       estimate_results, operations_results, agency_results) -> dict:
    """
    Compute all four failure mode scores and overall project health.
    """
    bad_estimate = score_bad_estimate(
        budget_results or {}, change_results or {},
        estimate_results or {}, schedule_results or {}
    )
    inefficient_ops = score_inefficient_ops(
        budget_results or {}, schedule_results or {},
        operations_results or {}
    )
    failure_capture = score_failure_to_capture_change(
        budget_results or {}, change_results or {}
    )
    conflict_agency = score_conflict_of_agency(
        agency_results or {}, change_results or {},
        budget_results or {}
    )

    scores = [
        bad_estimate['score'],
        inefficient_ops['score'],
        failure_capture['score'],
        conflict_agency['score'],
    ]

    # Overall = weighted average with emphasis on the worst
    avg_score = np.mean(scores)
    max_score = max(scores)
    overall = 0.6 * avg_score + 0.4 * max_score

    if overall < 25:
        overall_label = 'Green'
    elif overall < 50:
        overall_label = 'Yellow'
    elif overall < 75:
        overall_label = 'Red'
    else:
        overall_label = 'Critical'

    return {
        'overall_score': round(overall, 1),
        'overall_label': overall_label,
        'bad_estimate': bad_estimate,
        'inefficient_ops': inefficient_ops,
        'failure_to_capture_change': failure_capture,
        'conflict_of_agency': conflict_agency,
    }

"""
Report generation module.
Assembles all analysis results into a structured Red Flag Report.
"""

from datetime import datetime


def generate_report(scores: dict, budget_results: dict, schedule_results: dict,
                    change_results: dict, estimate_results: dict,
                    operations_results: dict, agency_results: dict,
                    files_uploaded: list) -> dict:
    """
    Generate a complete Red Flag Report from analysis results.
    Returns a structured dict for template rendering.
    """

    report = {
        'generated_at': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'files_analyzed': files_uploaded,
        'overall_score': scores['overall_score'],
        'overall_label': scores['overall_label'],
    }

    # --- Failure Mode Scores ---
    report['failure_modes'] = [
        {
            'name': 'Bad Estimate at Origin',
            'short_name': 'bad_estimate',
            'icon': 'clipboard-list',
            'score': scores['bad_estimate']['score'],
            'label': scores['bad_estimate']['label'],
            'details': scores['bad_estimate']['details'],
            'description': 'Indicates the original estimate or budget had gaps, incorrect pricing, '
                          'or missing scope that is now manifesting as cost overruns and changes.',
        },
        {
            'name': 'Inefficient Operations',
            'short_name': 'inefficient_ops',
            'icon': 'cogs',
            'score': scores['inefficient_ops']['score'],
            'label': scores['inefficient_ops']['label'],
            'details': scores['inefficient_ops']['details'],
            'description': 'Indicates field operations are underperforming through low productivity, '
                          'rework, schedule slippage, or resource management issues.',
        },
        {
            'name': 'Failure to Capture Change',
            'short_name': 'failure_capture',
            'icon': 'file-invoice-dollar',
            'score': scores['failure_to_capture_change']['score'],
            'label': scores['failure_to_capture_change']['label'],
            'details': scores['failure_to_capture_change']['details'],
            'description': 'Indicates scope changes are occurring but not being formally documented, '
                          'priced, and approved through the change order process.',
        },
        {
            'name': 'Conflict of Agency',
            'short_name': 'conflict_agency',
            'icon': 'balance-scale',
            'score': scores['conflict_of_agency']['score'],
            'label': scores['conflict_of_agency']['label'],
            'details': scores['conflict_of_agency']['details'],
            'description': 'Indicates the contractor may have misaligned financial incentives '
                          'that benefit their profit at the expense of the project.',
        },
    ]

    # --- Top Findings (sorted by severity) ---
    all_findings = []
    for fm in report['failure_modes']:
        for detail in fm['details']:
            all_findings.append({
                'failure_mode': fm['name'],
                'metric': detail['metric'],
                'value': detail['value'],
                'severity': detail['severity'],
                'explanation': detail['explanation'],
                'normalized': detail.get('normalized', 0),
            })

    # Sort by normalized score descending
    all_findings.sort(key=lambda x: x.get('normalized', 0) or 0, reverse=True)
    report['top_findings'] = all_findings[:10]
    report['all_findings'] = all_findings

    # --- Recommendations ---
    recommendations = []

    for finding in report['top_findings']:
        rec = generate_recommendation(finding)
        if rec:
            recommendations.append(rec)

    report['recommendations'] = recommendations

    # --- Summary Metrics ---
    report['budget_summary'] = {
        'total_original_budget': budget_results.get('total_original_budget'),
        'total_revised_budget': budget_results.get('total_revised_budget'),
        'total_actual_cost': budget_results.get('total_actual_cost'),
        'total_forecast': budget_results.get('total_forecast') or budget_results.get('eac_actual_forecast'),
        'cpi': budget_results.get('cpi'),
        'spi': budget_results.get('spi'),
        'cv': budget_results.get('cv'),
        'cv_pct': budget_results.get('cv_pct'),
        'tcpi': budget_results.get('tcpi'),
        'weighted_percent_complete': budget_results.get('weighted_percent_complete'),
    } if budget_results else None

    report['schedule_summary'] = {
        'total_activities': schedule_results.get('total_activities'),
        'critical_ratio': schedule_results.get('critical_ratio'),
        'near_critical_ratio': schedule_results.get('near_critical_ratio'),
        'bei': schedule_results.get('bei'),
        'negative_float_count': schedule_results.get('negative_float_count'),
        'missing_predecessors_ratio': schedule_results.get('missing_predecessors_ratio'),
        'logic_density': schedule_results.get('logic_density'),
        'avg_float': schedule_results.get('avg_float'),
        'complete_count': schedule_results.get('complete_count'),
        'in_progress_count': schedule_results.get('in_progress_count'),
        'not_started_count': schedule_results.get('not_started_count'),
    } if schedule_results else None

    report['change_summary'] = {
        'total_cos': change_results.get('total_cos'),
        'total_co_amount': change_results.get('total_co_amount'),
        'co_rate': change_results.get('co_rate'),
        'approved_count': change_results.get('approved_count'),
        'pending_count': change_results.get('pending_count'),
        'avg_pending_age': change_results.get('avg_pending_age'),
        'pending_backlog_pct': change_results.get('pending_backlog_pct'),
        'total_schedule_impact': change_results.get('total_schedule_impact'),
        'type_distribution': change_results.get('type_distribution'),
    } if change_results else None

    # --- Data Quality Notes ---
    data_notes = []
    if not budget_results:
        data_notes.append('No budget/cost data was uploaded. Budget-related metrics are unavailable.')
    if not schedule_results:
        data_notes.append('No schedule data was uploaded. Schedule-related metrics are unavailable.')
    if not change_results:
        data_notes.append('No change order data was uploaded. Change-related metrics are unavailable.')
    if budget_results and budget_results.get('cpi') is None:
        data_notes.append('CPI could not be calculated — check that actual cost data is present.')
    if schedule_results and schedule_results.get('bei') is None:
        data_notes.append('BEI could not be calculated — check that schedule dates include finish dates.')

    report['data_notes'] = data_notes

    return report


def generate_recommendation(finding: dict) -> dict | None:
    """Generate a recommendation for a finding based on its severity and metric."""
    severity = finding.get('severity', 'Green')
    if severity == 'Green':
        return None

    metric = finding['metric']
    fm = finding['failure_mode']

    rec_map = {
        'Cost Performance Index (CPI)': {
            'Yellow': 'Review the top 5 cost codes contributing to CPI deficit. Validate that percent complete figures are accurate and not inflated.',
            'Red': 'Conduct a detailed cost-to-complete review on all active cost codes. Consider an independent estimate to validate remaining scope pricing.',
            'Critical': 'Immediate cost intervention required. Commission an independent cost audit and prepare a recovery plan with specific corrective actions for each overrunning division.',
        },
        'Baseline Execution Index (BEI)': {
            'Yellow': 'Review schedule update process and ensure progress is being captured accurately. Identify the top 3 activities dragging BEI down.',
            'Red': 'The schedule is losing credibility as a management tool. Conduct a schedule recovery workshop and consider a re-baseline if the current baseline is no longer achievable.',
            'Critical': 'Schedule is severely detached from reality. An immediate schedule recovery analysis is required with resource-loaded corrective actions.',
        },
        'Pending CO Backlog': {
            'Yellow': 'Accelerate the CO review cycle. Establish weekly CO status meetings between owner and contractor.',
            'Red': 'Significant financial exposure in pending COs. Prioritize resolution of all COs over 30 days old. Consider engaging a claims consultant.',
            'Critical': 'Critical CO backlog threatening project financial integrity. Escalate to executive level for resolution. Consider dispute resolution mechanisms.',
        },
        'Average Pending CO Age': {
            'Yellow': 'Implement a 21-day target for CO resolution. Track aging weekly in OAC meetings.',
            'Red': 'CO processing is significantly delayed. Review the approval workflow for bottlenecks. Consider delegating approval authority for smaller COs.',
            'Critical': 'Severely aged COs suggest systemic approval failures. Implement an expedited review process and escalation protocol.',
        },
        'GC Conditions Burn Rate': {
            'Yellow': 'Monitor GC conditions billing more closely. Request detailed backup for all general conditions invoices.',
            'Red': 'GC conditions are being consumed faster than project progress warrants. Request a detailed staffing plan and compare billed vs. planned GC resources.',
            'Critical': 'GC conditions burn rate suggests potential overbilling. Conduct an audit of general conditions charges against contractual entitlement.',
        },
        'Change Order Rate': {
            'Yellow': 'Track CO trends monthly. Investigate root causes of the most frequent CO types.',
            'Red': 'High CO rate may indicate estimate issues or scope management problems. Conduct a root cause analysis on all COs to date.',
            'Critical': 'Excessive change rate indicates fundamental project issues. Assess whether the project scope, budget, or delivery method needs restructuring.',
        },
        'Scope Coverage Ratio': {
            'Yellow': 'Review the identified missing scope items and confirm they are either included under other line items or genuinely not applicable.',
            'Red': 'Multiple common scope items appear missing from the budget. Conduct a scope reconciliation against contract documents.',
            'Critical': 'Significant scope gaps detected. Commission a detailed scope validation audit before approving further expenditures.',
        },
        'Division Cost Outlier Ratio': {
            'Yellow': 'Review divisions flagged as outliers. Validate that unusual distributions are justified by project-specific conditions.',
            'Red': 'Several divisions show costs well outside benchmark ranges, suggesting pricing errors or scope imbalances.',
            'Critical': 'Cost distribution is highly irregular. An independent estimate review is strongly recommended.',
        },
        'Average CO Markup': {
            'Yellow': 'Review CO markup rates against contract terms. Ensure markups comply with the agreed-upon fee structure.',
            'Red': 'CO markups appear to exceed industry norms. Audit markup calculations and compare against contractual entitlements.',
            'Critical': 'Excessive CO markups detected. Engage a cost consultant to review all CO pricing and markup applications.',
        },
        'Contingency Burn Ratio': {
            'Yellow': 'Contingency is being consumed somewhat faster than progress. Review remaining contingency against anticipated risks.',
            'Red': 'Contingency is significantly over-consumed relative to progress. Conduct a risk assessment to determine if remaining contingency is adequate.',
            'Critical': 'Contingency is nearly exhausted relative to remaining work. Prepare a contingency replenishment request or budget amendment.',
        },
    }

    recs = rec_map.get(metric, {})
    text = recs.get(severity)

    if not text:
        # Generic recommendation
        if severity == 'Yellow':
            text = f'Monitor this metric closely. The {metric} is trending toward concerning territory.'
        elif severity == 'Red':
            text = f'This metric requires attention. Investigate the root cause and develop a corrective action plan.'
        else:
            text = f'Critical finding that requires immediate action. Escalate to project leadership for resolution.'

    return {
        'failure_mode': fm,
        'metric': metric,
        'severity': severity,
        'recommendation': text,
    }

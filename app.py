"""
Vantage Advisory Group — Unified Flask Application
Main website + Construction Project Diagnostic Tool with login protection.
"""

import os
import traceback
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    send_from_directory, session, flash
)
from werkzeug.security import check_password_hash

from engine.ingestion import ingest_file
from engine.budget_analysis import analyze_budget
from engine.schedule_analysis import analyze_schedule
from engine.change_analysis import analyze_changes
from engine.estimate_analysis import analyze_estimate
from engine.operations_analysis import analyze_operations
from engine.agency_analysis import analyze_agency
from engine.scoring import compute_all_scores
from engine.report import generate_report

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB max upload
app.secret_key = os.environ.get(
    'SECRET_KEY',
    'vntg-adv-d3v-k3y-f8a2c1e9b7d4f6a0e3c5b8d1f4a7c0e2'
)

SAMPLE_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sample_data')

# --- Authentication ---
# Pre-hashed credential (username: vantage owner, password: aaron-greg)
VALID_USERNAME = 'vantage owner'
CREDENTIAL_HASH = 'scrypt:32768:8:1$JI55uQeIW0v0aNIH$162ae52c89522dbab14d4dca91c5542fa63b1bffa3519d77e2172bf6b5cf6a0a61942f726ffd5172120d7f7da6f62a8b292f6c055b1347ebc402f80dd9b09720'


def login_required(f):
    """Decorator that redirects to login page if user is not authenticated."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


# --- Main Website Routes ---

@app.route('/')
def index():
    return render_template('site/index.html')


# --- Auth Routes ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('logged_in'):
        return redirect(url_for('diagnostic'))

    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        submitted = request.form.get('password', '')
        if username == VALID_USERNAME and check_password_hash(CREDENTIAL_HASH, submitted):
            session['logged_in'] = True
            session['username'] = VALID_USERNAME
            return redirect(url_for('diagnostic'))
        else:
            error = 'Invalid username or password.'

    return render_template('tool/login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))


# --- Diagnostic Tool Routes ---

@app.route('/diagnostic')
@login_required
def diagnostic():
    return render_template('tool/index.html')


@app.route('/diagnostic/sample')
@login_required
def diagnostic_sample():
    return render_template('tool/sample.html')


@app.route('/diagnostic/sample-data/<filename>')
@login_required
def download_sample(filename):
    return send_from_directory(SAMPLE_DATA_DIR, filename, as_attachment=True)


def run_analysis(file_data_list: list, file_names: list) -> dict:
    """
    Run the full diagnostic analysis pipeline on ingested data.
    Returns a report dict for template rendering.
    """
    cost_df = None
    schedule_df = None
    change_df = None

    for fd in file_data_list:
        if fd['type'] == 'cost':
            cost_df = fd['data']
        elif fd['type'] == 'schedule':
            schedule_df = fd['data']
        elif fd['type'] == 'change':
            change_df = fd['data']

    budget_results = analyze_budget(cost_df) if cost_df is not None else {}
    schedule_results = analyze_schedule(schedule_df) if schedule_df is not None else {}
    change_results = analyze_changes(change_df, cost_df) if change_df is not None else {}
    estimate_results = analyze_estimate(cost_df) if cost_df is not None else {}
    operations_results = analyze_operations(cost_df, schedule_df, budget_results, schedule_results)
    agency_results = analyze_agency(cost_df, change_df, budget_results, change_results)

    scores = compute_all_scores(
        budget_results, schedule_results, change_results,
        estimate_results, operations_results, agency_results
    )

    report = generate_report(
        scores, budget_results, schedule_results, change_results,
        estimate_results, operations_results, agency_results,
        file_names
    )

    return report


@app.route('/diagnostic/analyze', methods=['POST'])
@login_required
def analyze():
    """Handle file upload and analysis."""
    files = request.files.getlist('files')

    if not files or all(f.filename == '' for f in files):
        return render_template('tool/index.html', error='Please select at least one file to upload.')

    try:
        file_data_list = []
        file_names = []
        for f in files:
            if f.filename == '':
                continue
            result = ingest_file(f, f.filename)
            file_data_list.append(result)
            file_names.append(f'{f.filename} ({result["type"]}, {result["row_count"]} rows)')

        if not file_data_list:
            return render_template('tool/index.html', error='No valid data could be extracted from the uploaded files.')

        report = run_analysis(file_data_list, file_names)
        return render_template('tool/report.html', report=report)

    except Exception as e:
        traceback.print_exc()
        return render_template('tool/index.html',
                               error=f'Error processing files: {str(e)}. Please check your file format and try again.')


@app.route('/diagnostic/analyze-sample', methods=['POST'])
@login_required
def analyze_sample():
    """Run analysis on selected sample data files."""
    try:
        selected_files = request.form.getlist('files')

        if not selected_files:
            return render_template('tool/sample.html', error='Please select at least one file to analyze.')

        file_data_list = []
        file_names = []

        for filename in selected_files:
            safe_name = os.path.basename(filename)
            filepath = os.path.join(SAMPLE_DATA_DIR, safe_name)
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    result = ingest_file(f, safe_name)
                    file_data_list.append(result)
                    file_names.append(f'{safe_name} ({result["type"]}, {result["row_count"]} rows)')

        if not file_data_list:
            return render_template('tool/sample.html', error='Selected sample data files not found.')

        report = run_analysis(file_data_list, file_names)
        return render_template('tool/report.html', report=report)

    except Exception as e:
        traceback.print_exc()
        return render_template('tool/sample.html',
                               error=f'Error processing sample data: {str(e)}')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

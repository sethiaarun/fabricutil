#!/usr/bin/env python3
"""
ADO Test Results Comparator
===========================
Compares test failures between two Azure DevOps (ADO) pipeline runs
(e.g., PR success vs Current) and generates reports showing common failures,
new failures, and fixed tests.

Usage:
    python ado_test_results_compare.py <baseline_csv> <current_csv> [-o output_dir]
    python ado_test_results_compare.py --baseline-dir <dir1> --current-dir <dir2> [-o output_dir]

Examples:
    python ado_test_results_compare.py preprsuccess/reports/test_failures.csv current/reports/test_failures.csv
    python ado_test_results_compare.py --baseline-dir preprsuccess --current-dir current -o comparison_reports
"""

import argparse
import csv
import html
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Set, Dict, Tuple


@dataclass
class TestFailure:
    """Represents a single test failure/error."""
    test_name: str
    class_name: str
    source_file: str
    module_name: str
    status: str
    message: str
    duration: float
    zip_file: str
    
    @property
    def key(self) -> str:
        """Unique identifier for comparison (class + test name)."""
        return f"{self.class_name}.{self.test_name}"
    
    def __hash__(self):
        return hash(self.key)
    
    def __eq__(self, other):
        return self.key == other.key


def load_failures_from_csv(csv_path: str) -> Dict[str, TestFailure]:
    """Load test failures from CSV file."""
    failures = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failure = TestFailure(
                test_name=row.get('Test Name', ''),
                class_name=row.get('Class Name', ''),
                source_file=row.get('Source File', ''),
                module_name=row.get('Module', ''),
                status=row.get('Status', ''),
                message=row.get('Message', ''),
                duration=float(row.get('Duration (s)', '0').replace('s', '')),
                zip_file=row.get('Zip File', '')
            )
            failures[failure.key] = failure
    
    return failures


def find_csv_in_dir(directory: str) -> str:
    """Find test_failures.csv in directory or its reports subdirectory."""
    # Check direct path
    direct_path = os.path.join(directory, 'test_failures.csv')
    if os.path.exists(direct_path):
        return direct_path
    
    # Check reports subdirectory
    reports_path = os.path.join(directory, 'reports', 'test_failures.csv')
    if os.path.exists(reports_path):
        return reports_path
    
    raise FileNotFoundError(f"Could not find test_failures.csv in {directory} or {directory}/reports/")


def compare_failures(baseline: Dict[str, TestFailure], 
                     current: Dict[str, TestFailure]) -> Tuple[List[TestFailure], List[TestFailure], List[TestFailure]]:
    """
    Compare two sets of failures.
    
    Returns:
        - common: failures in both baseline and current
        - new_failures: failures only in current (regressions)
        - fixed: failures only in baseline (improvements)
    """
    baseline_keys = set(baseline.keys())
    current_keys = set(current.keys())
    
    common_keys = baseline_keys & current_keys
    new_keys = current_keys - baseline_keys
    fixed_keys = baseline_keys - current_keys
    
    common = [current[k] for k in sorted(common_keys)]
    new_failures = [current[k] for k in sorted(new_keys)]
    fixed = [baseline[k] for k in sorted(fixed_keys)]
    
    return common, new_failures, fixed


def generate_comparison_csv(common: List[TestFailure], 
                           new_failures: List[TestFailure], 
                           fixed: List[TestFailure],
                           output_path: str):
    """Generate comparison CSV report."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Category',
            'Test Name',
            'Class Name',
            'Source File',
            'Module',
            'Status',
            'Message'
        ])
        
        for failure in new_failures:
            writer.writerow([
                'NEW FAILURE',
                failure.test_name,
                failure.class_name,
                failure.source_file,
                failure.module_name,
                failure.status,
                failure.message
            ])
        
        for failure in common:
            writer.writerow([
                'COMMON (Known)',
                failure.test_name,
                failure.class_name,
                failure.source_file,
                failure.module_name,
                failure.status,
                failure.message
            ])
        
        for failure in fixed:
            writer.writerow([
                'FIXED',
                failure.test_name,
                failure.class_name,
                failure.source_file,
                failure.module_name,
                failure.status,
                failure.message
            ])
    
    print(f"Comparison CSV generated: {output_path}")


def generate_comparison_html(common: List[TestFailure], 
                             new_failures: List[TestFailure], 
                             fixed: List[TestFailure],
                             output_path: str,
                             baseline_name: str,
                             current_name: str):
    """Generate comparison HTML report."""
    
    # Group by module
    new_by_module = defaultdict(list)
    common_by_module = defaultdict(list)
    fixed_by_module = defaultdict(list)
    
    for f in new_failures:
        new_by_module[f.module_name].append(f)
    for f in common:
        common_by_module[f.module_name].append(f)
    for f in fixed:
        fixed_by_module[f.module_name].append(f)
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Test Results Comparison Report</title>
    <style>
        :root {{
            --bg-color: #1a1a2e;
            --card-bg: #16213e;
            --text-color: #eee;
            --border-color: #0f3460;
            --new-color: #e94560;
            --common-color: #ffc93c;
            --fixed-color: #4ecca3;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        h1 {{
            text-align: center;
            margin-bottom: 10px;
            color: #fff;
        }}
        
        .subtitle {{
            text-align: center;
            color: #888;
            margin-bottom: 30px;
        }}
        
        .comparison-info {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }}
        
        .comparison-item {{
            background: var(--card-bg);
            padding: 15px 25px;
            border-radius: 10px;
            border: 1px solid var(--border-color);
        }}
        
        .comparison-item strong {{
            color: #4ecca3;
        }}
        
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .summary-card {{
            background: var(--card-bg);
            border-radius: 10px;
            padding: 25px;
            text-align: center;
            border: 2px solid var(--border-color);
            transition: transform 0.2s;
        }}
        
        .summary-card:hover {{
            transform: translateY(-3px);
        }}
        
        .summary-card h3 {{
            font-size: 3em;
            margin-bottom: 10px;
        }}
        
        .summary-card p {{
            font-size: 1.1em;
            margin-bottom: 5px;
        }}
        
        .summary-card .description {{
            font-size: 0.85em;
            color: #888;
        }}
        
        .summary-card.new {{ border-color: var(--new-color); }}
        .summary-card.new h3 {{ color: var(--new-color); }}
        
        .summary-card.common {{ border-color: var(--common-color); }}
        .summary-card.common h3 {{ color: var(--common-color); }}
        
        .summary-card.fixed {{ border-color: var(--fixed-color); }}
        .summary-card.fixed h3 {{ color: var(--fixed-color); }}
        
        .section {{
            background: var(--card-bg);
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid var(--border-color);
        }}
        
        .section h2 {{
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .section.new-section h2 {{ color: var(--new-color); }}
        .section.common-section h2 {{ color: var(--common-color); }}
        .section.fixed-section h2 {{ color: var(--fixed-color); }}
        
        .badge {{
            font-size: 0.7em;
            padding: 3px 10px;
            border-radius: 15px;
            margin-left: auto;
        }}
        
        .badge-new {{ background: var(--new-color); }}
        .badge-common {{ background: var(--common-color); color: black; }}
        .badge-fixed {{ background: var(--fixed-color); color: black; }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        
        th {{
            background-color: rgba(0,0,0,0.2);
            font-weight: 600;
            position: sticky;
            top: 0;
        }}
        
        tr:hover {{
            background-color: rgba(255,255,255,0.05);
        }}
        
        .test-name {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.85em;
            word-break: break-word;
        }}
        
        .class-name {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.8em;
            color: #888;
        }}
        
        .module-badge {{
            background-color: var(--border-color);
            padding: 3px 10px;
            border-radius: 15px;
            font-size: 0.8em;
            white-space: nowrap;
        }}
        
        .status {{
            padding: 3px 10px;
            border-radius: 15px;
            font-size: 0.8em;
        }}
        
        .status-failure {{ background: #e94560; }}
        .status-error {{ background: #ff6b35; }}
        
        .message {{
            font-size: 0.85em;
            color: #ccc;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .filter-input {{
            padding: 10px 15px;
            width: 100%;
            max-width: 400px;
            border: 1px solid var(--border-color);
            border-radius: 5px;
            background: rgba(0,0,0,0.3);
            color: var(--text-color);
            font-size: 1em;
            margin-bottom: 15px;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: var(--fixed-color);
        }}
        
        .collapsible {{
            cursor: pointer;
            user-select: none;
        }}
        
        .collapsible::after {{
            content: ' ▼';
            font-size: 0.7em;
        }}
        
        .collapsible.collapsed::after {{
            content: ' ►';
        }}
        
        .module-group {{
            margin-bottom: 15px;
            background: rgba(0,0,0,0.2);
            border-radius: 8px;
            overflow: hidden;
        }}
        
        .module-header {{
            padding: 12px 15px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .module-header:hover {{
            background: rgba(255,255,255,0.05);
        }}
        
        .module-content {{
            padding: 0 15px 15px;
        }}
        
        .module-content.hidden {{
            display: none;
        }}
        
        .priority-banner {{
            background: linear-gradient(135deg, #e94560, #ff6b35);
            padding: 15px 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}
        
        .priority-banner h3 {{
            margin-bottom: 5px;
        }}
        
        .no-issues {{
            padding: 30px;
            text-align: center;
            color: #888;
        }}
        
        @media (max-width: 768px) {{
            .summary {{
                grid-template-columns: 1fr;
            }}
            
            .comparison-info {{
                flex-direction: column;
                align-items: center;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Test Results Comparison Report</h1>
        <p class="subtitle">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <div class="comparison-info">
            <div class="comparison-item">
                <strong>Baseline (PR Success):</strong> {html.escape(baseline_name)}
            </div>
            <div class="comparison-item">
                <strong>Current Run:</strong> {html.escape(current_name)}
            </div>
        </div>
'''
    
    # Priority banner if there are new failures
    if new_failures:
        html_content += f'''
        <div class="priority-banner">
            <h3>⚠️ Action Required: {len(new_failures)} New Failure(s) Detected</h3>
            <p>These tests are failing in the current run but were passing in the baseline.</p>
        </div>
'''
    
    html_content += f'''
        <div class="summary">
            <div class="summary-card new">
                <h3>{len(new_failures)}</h3>
                <p>New Failures</p>
                <span class="description">Regressions - need attention</span>
            </div>
            <div class="summary-card common">
                <h3>{len(common)}</h3>
                <p>Common (Known)</p>
                <span class="description">Failing in both runs</span>
            </div>
            <div class="summary-card fixed">
                <h3>{len(fixed)}</h3>
                <p>Fixed</p>
                <span class="description">No longer failing</span>
            </div>
        </div>
'''
    
    # New Failures Section
    html_content += '''
        <div class="section new-section">
            <h2>🚨 New Failures (Regressions) <span class="badge badge-new">''' + str(len(new_failures)) + '''</span></h2>
'''
    
    if new_failures:
        html_content += '''
            <input type="text" class="filter-input" placeholder="Filter new failures..." onkeyup="filterSection(this, 'new-table')">
            <table id="new-table">
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Class</th>
                        <th>Module</th>
                        <th>Status</th>
                        <th>Message</th>
                    </tr>
                </thead>
                <tbody>
'''
        for f in sorted(new_failures, key=lambda x: (x.module_name, x.class_name)):
            status_class = f"status-{f.status}"
            html_content += f'''
                    <tr>
                        <td class="test-name">{html.escape(f.test_name)}</td>
                        <td class="class-name">{html.escape(f.class_name)}</td>
                        <td><span class="module-badge">{html.escape(f.module_name)}</span></td>
                        <td><span class="status {status_class}">{f.status}</span></td>
                        <td class="message" title="{html.escape(f.message)}">{html.escape(f.message[:80])}</td>
                    </tr>
'''
        html_content += '''
                </tbody>
            </table>
'''
    else:
        html_content += '''
            <div class="no-issues">✅ No new failures detected! All regressions have been addressed.</div>
'''
    
    html_content += '''
        </div>
'''
    
    # Common Failures Section
    html_content += '''
        <div class="section common-section">
            <h2>⚡ Common Failures (Known Issues) <span class="badge badge-common">''' + str(len(common)) + '''</span></h2>
'''
    
    if common:
        html_content += '''
            <input type="text" class="filter-input" placeholder="Filter common failures..." onkeyup="filterSection(this, 'common-table')">
            <table id="common-table">
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Class</th>
                        <th>Module</th>
                        <th>Status</th>
                        <th>Message</th>
                    </tr>
                </thead>
                <tbody>
'''
        for f in sorted(common, key=lambda x: (x.module_name, x.class_name)):
            status_class = f"status-{f.status}"
            html_content += f'''
                    <tr>
                        <td class="test-name">{html.escape(f.test_name)}</td>
                        <td class="class-name">{html.escape(f.class_name)}</td>
                        <td><span class="module-badge">{html.escape(f.module_name)}</span></td>
                        <td><span class="status {status_class}">{f.status}</span></td>
                        <td class="message" title="{html.escape(f.message)}">{html.escape(f.message[:80])}</td>
                    </tr>
'''
        html_content += '''
                </tbody>
            </table>
'''
    else:
        html_content += '''
            <div class="no-issues">No common failures between baseline and current run.</div>
'''
    
    html_content += '''
        </div>
'''
    
    # Fixed Section
    html_content += '''
        <div class="section fixed-section">
            <h2>✅ Fixed Tests <span class="badge badge-fixed">''' + str(len(fixed)) + '''</span></h2>
'''
    
    if fixed:
        html_content += '''
            <input type="text" class="filter-input" placeholder="Filter fixed tests..." onkeyup="filterSection(this, 'fixed-table')">
            <table id="fixed-table">
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Class</th>
                        <th>Module</th>
                        <th>Previous Status</th>
                    </tr>
                </thead>
                <tbody>
'''
        for f in sorted(fixed, key=lambda x: (x.module_name, x.class_name)):
            html_content += f'''
                    <tr>
                        <td class="test-name">{html.escape(f.test_name)}</td>
                        <td class="class-name">{html.escape(f.class_name)}</td>
                        <td><span class="module-badge">{html.escape(f.module_name)}</span></td>
                        <td>{f.status}</td>
                    </tr>
'''
        html_content += '''
                </tbody>
            </table>
'''
    else:
        html_content += '''
            <div class="no-issues">No tests were fixed in this run.</div>
'''
    
    html_content += '''
        </div>
        
        <div class="section">
            <h2>📊 Summary by Module</h2>
            <table>
                <thead>
                    <tr>
                        <th>Module</th>
                        <th style="color: var(--new-color)">New Failures</th>
                        <th style="color: var(--common-color)">Common</th>
                        <th style="color: var(--fixed-color)">Fixed</th>
                    </tr>
                </thead>
                <tbody>
'''
    
    all_modules = set(new_by_module.keys()) | set(common_by_module.keys()) | set(fixed_by_module.keys())
    for module in sorted(all_modules):
        new_count = len(new_by_module.get(module, []))
        common_count = len(common_by_module.get(module, []))
        fixed_count = len(fixed_by_module.get(module, []))
        html_content += f'''
                    <tr>
                        <td><span class="module-badge">{html.escape(module)}</span></td>
                        <td style="color: var(--new-color)">{new_count if new_count else '-'}</td>
                        <td style="color: var(--common-color)">{common_count if common_count else '-'}</td>
                        <td style="color: var(--fixed-color)">{fixed_count if fixed_count else '-'}</td>
                    </tr>
'''
    
    html_content += '''
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        function filterSection(input, tableId) {
            const filter = input.value.toLowerCase();
            const table = document.getElementById(tableId);
            const rows = table.getElementsByTagName('tr');
            
            for (let i = 1; i < rows.length; i++) {
                const row = rows[i];
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            }
        }
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Comparison HTML generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Compare test failures between two runs and generate reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s baseline.csv current.csv -o ./comparison
  %(prog)s --baseline-dir /path/to/preprsuccess --current-dir /path/to/current
        '''
    )
    
    parser.add_argument('baseline_csv', nargs='?', help='Baseline CSV file (PR success)')
    parser.add_argument('current_csv', nargs='?', help='Current CSV file')
    parser.add_argument('--baseline-dir', help='Directory containing baseline results')
    parser.add_argument('--current-dir', help='Directory containing current results')
    parser.add_argument('-o', '--output-dir', default='.', help='Output directory for comparison reports')
    parser.add_argument('--baseline-name', help='Display name for baseline (default: derived from path)')
    parser.add_argument('--current-name', help='Display name for current (default: derived from path)')
    
    args = parser.parse_args()
    
    # Determine CSV paths
    if args.baseline_dir and args.current_dir:
        baseline_csv = find_csv_in_dir(args.baseline_dir)
        current_csv = find_csv_in_dir(args.current_dir)
    elif args.baseline_csv and args.current_csv:
        baseline_csv = args.baseline_csv
        current_csv = args.current_csv
    else:
        parser.error("Provide either CSV files or directories with --baseline-dir and --current-dir")
    
    # Validate files exist
    if not os.path.exists(baseline_csv):
        print(f"Error: Baseline CSV not found: {baseline_csv}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(current_csv):
        print(f"Error: Current CSV not found: {current_csv}", file=sys.stderr)
        sys.exit(1)
    
    # Derive names if not provided
    baseline_name = args.baseline_name or os.path.dirname(baseline_csv).split('/')[-2] or 'Baseline'
    current_name = args.current_name or os.path.dirname(current_csv).split('/')[-2] or 'Current'
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load failures
    print(f"Loading baseline: {baseline_csv}")
    baseline = load_failures_from_csv(baseline_csv)
    print(f"  Found {len(baseline)} failures")
    
    print(f"Loading current: {current_csv}")
    current = load_failures_from_csv(current_csv)
    print(f"  Found {len(current)} failures")
    
    # Compare
    common, new_failures, fixed = compare_failures(baseline, current)
    
    print(f"\nComparison Results:")
    print(f"  Common (Known):  {len(common)}")
    print(f"  New Failures:    {len(new_failures)}")
    print(f"  Fixed:           {len(fixed)}")
    
    # Generate reports
    csv_path = os.path.join(args.output_dir, 'comparison.csv')
    html_path = os.path.join(args.output_dir, 'comparison.html')
    
    generate_comparison_csv(common, new_failures, fixed, csv_path)
    generate_comparison_html(common, new_failures, fixed, html_path, baseline_name, current_name)
    
    # Print summary of new failures
    if new_failures:
        print("\n" + "="*60)
        print("⚠️  NEW FAILURES (REGRESSIONS)")
        print("="*60)
        for f in sorted(new_failures, key=lambda x: (x.module_name, x.class_name)):
            print(f"  [{f.module_name}] {f.class_name}.{f.test_name}")
    else:
        print("\n✅ No new regressions detected!")


if __name__ == '__main__':
    main()

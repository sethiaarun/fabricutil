#!/usr/bin/env python3
"""
Test Results Analyzer
=====================
Analyzes JUnit XML test result zip files and generates CSV and HTML reports
for failed, errored, and aborted test cases.

Usage:
    python test_results_analyzer.py <zip_file1> [zip_file2] ... [-o output_dir]
    python test_results_analyzer.py /path/to/*.zip -o /path/to/output

Examples:
    python test_results_analyzer.py TestResults_756313378.zip
    python test_results_analyzer.py *.zip -o ./reports
    python test_results_analyzer.py test1.zip test2.zip --output-dir ./results
"""

import argparse
import csv
import html
import os
import re
import sys
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional


@dataclass
class TestFailure:
    """Represents a single test failure/error."""
    test_name: str
    class_name: str
    source_file: str
    module_name: str
    status: str  # 'failure', 'error', 'aborted'
    message: str
    stack_trace: str
    zip_file: str
    duration: float


def extract_module_from_classname(classname: str) -> str:
    """Extract module name from Java/Scala class name."""
    # Common patterns for module detection
    parts = classname.split('.')
    
    # Look for common module indicators
    module_indicators = {
        'sql': 'sql/core',
        'catalyst': 'sql/catalyst',
        'hive': 'sql/hive',
        'streaming': 'streaming',
        'mllib': 'mllib',
        'ml': 'mllib',
        'graphx': 'graphx',
        'core': 'core',
        'yarn': 'resource-managers/yarn',
        'kubernetes': 'resource-managers/kubernetes',
        'mesos': 'resource-managers/mesos',
        'avro': 'connector/avro',
        'kafka': 'connector/kafka',
        'connect': 'connector/connect',
        'protobuf': 'connector/protobuf',
        'pipelines': 'sql/pipelines',
        'scripting': 'sql/core',
        'artifact': 'sql/core',
        'execution': 'sql/core',
        'onesecurity': 'onesecurity',
    }
    
    for part in parts:
        part_lower = part.lower()
        if part_lower in module_indicators:
            return module_indicators[part_lower]
    
    # Default: try to infer from package structure
    if len(parts) >= 4:
        # org.apache.spark.sql.xxx -> sql
        if 'spark' in parts:
            spark_idx = parts.index('spark')
            if spark_idx + 1 < len(parts):
                return parts[spark_idx + 1]
    
    return 'unknown'


def extract_source_file(classname: str) -> str:
    """Extract likely source file name from class name."""
    # Get the simple class name (last part)
    simple_name = classname.split('.')[-1]
    
    # Remove common test suffixes to get base name
    base_name = simple_name
    for suffix in ['Suite', 'Test', 'Tests', 'Spec']:
        if base_name.endswith(suffix):
            base_name = base_name[:-len(suffix)]
            break
    
    # Convert to file path
    package_path = '/'.join(classname.split('.')[:-1])
    
    # Return full path with .scala or .java extension
    return f"{package_path}/{simple_name}.scala"


def parse_junit_xml(xml_content: str, zip_filename: str) -> List[TestFailure]:
    """Parse JUnit XML content and extract test failures/errors."""
    failures = []
    
    # Find all testcases
    testcase_pattern = r'<testcase\s+([^>]*)>([\s\S]*?)</testcase>'
    
    for match in re.finditer(testcase_pattern, xml_content):
        attrs_str = match.group(1)
        tc_content = match.group(2)
        
        # Check if this test has a failure, error, or was aborted/skipped
        has_failure = '<failure' in tc_content
        has_error = '<error' in tc_content
        has_skipped = '<skipped' in tc_content and 'aborted' in tc_content.lower()
        
        if not (has_failure or has_error or has_skipped):
            continue
        
        # Extract attributes
        name_match = re.search(r'name="([^"]*)"', attrs_str)
        classname_match = re.search(r'classname="([^"]*)"', attrs_str)
        time_match = re.search(r'time="([^"]*)"', attrs_str)
        
        test_name = name_match.group(1) if name_match else 'unknown'
        class_name = classname_match.group(1) if classname_match else 'unknown'
        duration = float(time_match.group(1)) if time_match else 0.0
        
        # Determine status
        if has_error:
            status = 'error'
            detail_pattern = r'<error[^>]*(?:message="([^"]*)")?[^>]*>([\s\S]*?)</error>'
        elif has_failure:
            status = 'failure'
            detail_pattern = r'<failure[^>]*(?:message="([^"]*)")?[^>]*>([\s\S]*?)</failure>'
        else:
            status = 'aborted'
            detail_pattern = r'<skipped[^>]*(?:message="([^"]*)")?[^>]*/?>(?:([\s\S]*?)</skipped>)?'
        
        # Extract message and stack trace
        detail_match = re.search(detail_pattern, tc_content)
        message = ''
        stack_trace = ''
        
        if detail_match:
            message = detail_match.group(1) or ''
            stack_trace = detail_match.group(2) or '' if len(detail_match.groups()) > 1 else ''
            # Clean up CDATA and HTML entities
            stack_trace = re.sub(r'<!\[CDATA\[([\s\S]*?)\]\]>', r'\1', stack_trace)
            stack_trace = stack_trace.strip()
        
        # Also try to get message from attribute if not found
        if not message:
            msg_match = re.search(r'message="([^"]*)"', tc_content)
            if msg_match:
                message = msg_match.group(1)
        
        failure = TestFailure(
            test_name=test_name,
            class_name=class_name,
            source_file=extract_source_file(class_name),
            module_name=extract_module_from_classname(class_name),
            status=status,
            message=html.unescape(message)[:500] if message else '',  # Truncate long messages
            stack_trace=stack_trace[:2000] if stack_trace else '',  # Truncate long traces
            zip_file=zip_filename,
            duration=duration
        )
        failures.append(failure)
    
    return failures


def process_zip_file(zip_path: str) -> List[TestFailure]:
    """Process a single zip file and extract all test failures."""
    all_failures = []
    zip_filename = os.path.basename(zip_path)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if name.endswith('.xml'):
                    try:
                        content = zf.read(name).decode('utf-8', errors='ignore')
                        failures = parse_junit_xml(content, zip_filename)
                        all_failures.extend(failures)
                    except Exception as e:
                        print(f"Warning: Error processing {name} in {zip_path}: {e}", file=sys.stderr)
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file", file=sys.stderr)
    except Exception as e:
        print(f"Error processing {zip_path}: {e}", file=sys.stderr)
    
    return all_failures


def generate_csv(failures: List[TestFailure], output_path: str):
    """Generate CSV report."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Test Name',
            'Class Name', 
            'Source File',
            'Module',
            'Status',
            'Message',
            'Duration (s)',
            'Zip File'
        ])
        
        for failure in failures:
            writer.writerow([
                failure.test_name,
                failure.class_name,
                failure.source_file,
                failure.module_name,
                failure.status,
                failure.message,
                f"{failure.duration:.3f}",
                failure.zip_file
            ])
    
    print(f"CSV report generated: {output_path}")


def generate_html(failures: List[TestFailure], output_path: str, zip_files: List[str]):
    """Generate HTML report."""
    
    # Group failures by module and status
    by_module = defaultdict(list)
    by_status = defaultdict(list)
    by_zip = defaultdict(list)
    
    for f in failures:
        by_module[f.module_name].append(f)
        by_status[f.status].append(f)
        by_zip[f.zip_file].append(f)
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Test Results Analysis Report</title>
    <style>
        :root {{
            --bg-color: #1a1a2e;
            --card-bg: #16213e;
            --text-color: #eee;
            --border-color: #0f3460;
            --failure-color: #e94560;
            --error-color: #ff6b35;
            --aborted-color: #ffc93c;
            --success-color: #4ecca3;
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
        
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .summary-card {{
            background: var(--card-bg);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            border: 1px solid var(--border-color);
        }}
        
        .summary-card h3 {{
            font-size: 2.5em;
            margin-bottom: 5px;
        }}
        
        .summary-card.total h3 {{ color: #fff; }}
        .summary-card.failures h3 {{ color: var(--failure-color); }}
        .summary-card.errors h3 {{ color: var(--error-color); }}
        .summary-card.aborted h3 {{ color: var(--aborted-color); }}
        
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
        }}
        
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
        
        .status {{
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 500;
        }}
        
        .status-failure {{ background-color: var(--failure-color); color: white; }}
        .status-error {{ background-color: var(--error-color); color: white; }}
        .status-aborted {{ background-color: var(--aborted-color); color: black; }}
        
        .test-name {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.85em;
            word-break: break-word;
            max-width: 400px;
        }}
        
        .class-name {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.8em;
            color: #888;
            word-break: break-all;
        }}
        
        .module-badge {{
            background-color: var(--border-color);
            padding: 3px 10px;
            border-radius: 15px;
            font-size: 0.8em;
            white-space: nowrap;
        }}
        
        .message {{
            font-size: 0.85em;
            color: #ccc;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .collapsible {{
            cursor: pointer;
            user-select: none;
        }}
        
        .collapsible:after {{
            content: ' ▼';
            font-size: 0.8em;
        }}
        
        .collapsible.active:after {{
            content: ' ▲';
        }}
        
        .content {{
            display: none;
            overflow: hidden;
        }}
        
        .content.show {{
            display: block;
        }}
        
        .by-zip {{
            margin-bottom: 15px;
            padding: 10px;
            background: rgba(0,0,0,0.2);
            border-radius: 5px;
        }}
        
        .by-zip-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
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
            margin-bottom: 20px;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: var(--success-color);
        }}
        
        @media (max-width: 768px) {{
            .summary {{
                grid-template-columns: repeat(2, 1fr);
            }}
            
            table {{
                font-size: 0.8em;
            }}
            
            th, td {{
                padding: 8px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🧪 Test Results Analysis Report</h1>
        <p class="subtitle">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Analyzed {len(zip_files)} zip file(s)</p>
        
        <div class="summary">
            <div class="summary-card total">
                <h3>{len(failures)}</h3>
                <p>Total Issues</p>
            </div>
            <div class="summary-card failures">
                <h3>{len(by_status.get('failure', []))}</h3>
                <p>Failures</p>
            </div>
            <div class="summary-card errors">
                <h3>{len(by_status.get('error', []))}</h3>
                <p>Errors</p>
            </div>
            <div class="summary-card aborted">
                <h3>{len(by_status.get('aborted', []))}</h3>
                <p>Aborted</p>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Summary by Zip File</h2>
'''
    
    for zip_name in sorted(by_zip.keys()):
        zip_failures = by_zip[zip_name]
        html_content += f'''
            <div class="by-zip">
                <div class="by-zip-header">
                    <strong>{html.escape(zip_name)}</strong>
                    <span>{len(zip_failures)} issue(s)</span>
                </div>
            </div>
'''
    
    html_content += '''
        </div>
        
        <div class="section">
            <h2>📋 Summary by Module</h2>
            <table>
                <thead>
                    <tr>
                        <th>Module</th>
                        <th>Failures</th>
                        <th>Errors</th>
                        <th>Aborted</th>
                        <th>Total</th>
                    </tr>
                </thead>
                <tbody>
'''
    
    for module in sorted(by_module.keys()):
        module_failures = by_module[module]
        f_count = len([f for f in module_failures if f.status == 'failure'])
        e_count = len([f for f in module_failures if f.status == 'error'])
        a_count = len([f for f in module_failures if f.status == 'aborted'])
        html_content += f'''
                    <tr>
                        <td><span class="module-badge">{html.escape(module)}</span></td>
                        <td>{f_count}</td>
                        <td>{e_count}</td>
                        <td>{a_count}</td>
                        <td><strong>{len(module_failures)}</strong></td>
                    </tr>
'''
    
    html_content += '''
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>🔍 All Test Issues</h2>
            <input type="text" class="filter-input" id="filterInput" placeholder="Filter tests by name, class, module..." onkeyup="filterTable()">
            <div style="overflow-x: auto;">
                <table id="failuresTable">
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Test Name</th>
                            <th>Class / Source</th>
                            <th>Module</th>
                            <th>Message</th>
                            <th>Duration</th>
                            <th>Zip File</th>
                        </tr>
                    </thead>
                    <tbody>
'''
    
    for failure in sorted(failures, key=lambda x: (x.zip_file, x.module_name, x.class_name)):
        status_class = f"status-{failure.status}"
        html_content += f'''
                        <tr>
                            <td><span class="status {status_class}">{failure.status}</span></td>
                            <td class="test-name">{html.escape(failure.test_name)}</td>
                            <td>
                                <div class="class-name">{html.escape(failure.class_name)}</div>
                                <div class="class-name" style="color:#666">{html.escape(failure.source_file)}</div>
                            </td>
                            <td><span class="module-badge">{html.escape(failure.module_name)}</span></td>
                            <td class="message" title="{html.escape(failure.message)}">{html.escape(failure.message[:100])}</td>
                            <td>{failure.duration:.3f}s</td>
                            <td style="font-size:0.8em">{html.escape(failure.zip_file)}</td>
                        </tr>
'''
    
    html_content += '''
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        function filterTable() {
            const input = document.getElementById('filterInput');
            const filter = input.value.toLowerCase();
            const table = document.getElementById('failuresTable');
            const rows = table.getElementsByTagName('tr');
            
            for (let i = 1; i < rows.length; i++) {
                const row = rows[i];
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            }
        }
        
        document.querySelectorAll('.collapsible').forEach(item => {
            item.addEventListener('click', function() {
                this.classList.toggle('active');
                const content = this.nextElementSibling;
                content.classList.toggle('show');
            });
        });
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"HTML report generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze JUnit XML test results from zip files and generate reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s TestResults_756313378.zip
  %(prog)s *.zip -o ./reports
  %(prog)s test1.zip test2.zip --output-dir ./results
        '''
    )
    parser.add_argument('zip_files', nargs='+', help='One or more zip files containing JUnit XML results')
    parser.add_argument('-o', '--output-dir', default='.', help='Output directory for reports (default: current directory)')
    parser.add_argument('--csv-name', default='test_failures.csv', help='CSV output filename (default: test_failures.csv)')
    parser.add_argument('--html-name', default='test_failures.html', help='HTML output filename (default: test_failures.html)')
    
    args = parser.parse_args()
    
    # Validate zip files exist
    valid_zips = []
    for zf in args.zip_files:
        if os.path.isfile(zf):
            valid_zips.append(zf)
        else:
            print(f"Warning: {zf} not found, skipping", file=sys.stderr)
    
    if not valid_zips:
        print("Error: No valid zip files provided", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if needed
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process all zip files
    print(f"Processing {len(valid_zips)} zip file(s)...")
    all_failures = []
    for zf in valid_zips:
        print(f"  Processing: {zf}")
        failures = process_zip_file(zf)
        all_failures.extend(failures)
        print(f"    Found {len(failures)} issue(s)")
    
    print(f"\nTotal issues found: {len(all_failures)}")
    
    if not all_failures:
        print("No test failures/errors found!")
        return
    
    # Generate reports
    csv_path = os.path.join(args.output_dir, args.csv_name)
    html_path = os.path.join(args.output_dir, args.html_name)
    
    generate_csv(all_failures, csv_path)
    generate_html(all_failures, html_path, valid_zips)
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    by_status = defaultdict(int)
    by_module = defaultdict(int)
    
    for f in all_failures:
        by_status[f.status] += 1
        by_module[f.module_name] += 1
    
    print(f"  Failures: {by_status.get('failure', 0)}")
    print(f"  Errors:   {by_status.get('error', 0)}")
    print(f"  Aborted:  {by_status.get('aborted', 0)}")
    print(f"  Total:    {len(all_failures)}")
    print("\nBy Module:")
    for module, count in sorted(by_module.items(), key=lambda x: -x[1]):
        print(f"  {module}: {count}")


if __name__ == '__main__':
    main()

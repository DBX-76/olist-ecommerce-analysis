"""
Organize Reports Script - Moves reports into proper subdirectories
"""

import os
import shutil

# Define paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

# Create subdirectories
subdirs = ['eda', 'data_quality', 'cleaning']
for subdir in subdirs:
    os.makedirs(os.path.join(REPORTS_PATH, subdir), exist_ok=True)

print("="*80)
print("ORGANIZING REPORTS")
print("="*80)

# Move EDA reports (HTML files)
print("\nMoving EDA reports...")
eda_files = [f for f in os.listdir(REPORTS_PATH) if f.endswith('_profile.html')]
for filename in eda_files:
    src = os.path.join(REPORTS_PATH, filename)
    dst = os.path.join(REPORTS_PATH, 'eda', filename)
    if os.path.exists(src):
        shutil.move(src, dst)
        print(f"  Moved: {filename} -> eda/")

# Move data quality reports (CSV files)
print("\nMoving data quality reports...")
data_quality_files = [
    'missing_values_report.csv',
    'data_types_report.csv',
    'duplicates_report.csv',
    'outliers_report.csv',
    'data_quality_summary.csv',
    'cleaning_recommendations.csv',
    'date_consistency_report.csv'
]
for filename in data_quality_files:
    src = os.path.join(REPORTS_PATH, filename)
    dst = os.path.join(REPORTS_PATH, 'data_quality', filename)
    if os.path.exists(src):
        shutil.move(src, dst)
        print(f"  Moved: {filename} -> data_quality/")

# Move cleaning reports
print("\nMoving cleaning reports...")
cleaning_files = ['cleaning_summary.csv']
for filename in cleaning_files:
    src = os.path.join(REPORTS_PATH, filename)
    dst = os.path.join(REPORTS_PATH, 'cleaning', filename)
    if os.path.exists(src):
        shutil.move(src, dst)
        print(f"  Moved: {filename} -> cleaning/")

print("\n" + "="*80)
print("ORGANIZATION COMPLETE")
print("="*80)

print("\nNew structure:")
print("  reports/")
print("    ├── eda/              # Exploratory Data Analysis reports (HTML)")
print("    ├── data_quality/     # Data quality analysis reports (CSV)")
print("    ├── cleaning/          # Data cleaning reports (CSV)")
print("    └── README.md         # This file")

print("\nFiles organized successfully!")
print("\nNext steps:")
print("  1. Review reports in their respective directories")
print("  2. Use reports for data cleaning and analysis")
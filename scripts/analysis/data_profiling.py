"""
Test script for ydata_profiling with Brazilian E-Commerce Dataset
This script tests the profiling functionality on a single dataset.
"""

import pandas as pd
from ydata_profiling import ProfileReport
import os

# Define paths (using absolute paths from project root)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

# Ensure reports directory exists
os.makedirs(REPORTS_PATH, exist_ok=True)

print("="*60)
print("Testing ydata_profiling with Orders Dataset")
print("="*60)

# Load orders dataset (central table)
orders_file = os.path.join(RAW_DATA_PATH, 'olist_orders_dataset.csv')
print(f"\nLoading: {orders_file}")

try:
    df = pd.read_csv(orders_file)
    print(f"[OK] Successfully loaded: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
    # Display basic info
    print(f"\nDataset Info:")
    print(f"  - Columns: {list(df.columns)}")
    print(f"  - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Generate profiling report
    print(f"\nGenerating profiling report...")
    profile = ProfileReport(
        df,
        title="Orders Dataset - Test Profile",
        minimal=False,
        explorative=True
    )
    
    # Save report
    report_path = os.path.join(REPORTS_PATH, 'test_orders_profile.html')
    profile.to_file(report_path)
    print(f"[OK] Report saved: {report_path}")
    
    # Display some key statistics
    print(f"\nKey Statistics:")
    print(f"  - Total orders: {df.shape[0]:,}")
    print(f"  - Order status distribution:")
    print(df['order_status'].value_counts())
    
    print(f"\n{'='*60}")
    print("TEST SUCCESSFUL!")
    print(f"{'='*60}")
    print(f"\nOpen the report in your browser: {report_path}")
    
except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()
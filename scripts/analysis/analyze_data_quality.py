"""
Data Quality Analysis Script for Brazilian E-Commerce Dataset
This script analyzes data quality issues and generates a single comprehensive TXT report.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# Set display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', None)

# Define paths (using absolute paths from project root)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up two levels from scripts/analysis/ to reach project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

# Ensure reports directory exists
os.makedirs(REPORTS_PATH, exist_ok=True)
# Create categorized report directories
DATA_QUALITY_DIR = os.path.join(REPORTS_PATH, 'data_quality')
CLEANING_DIR = os.path.join(REPORTS_PATH, 'cleaning')
os.makedirs(DATA_QUALITY_DIR, exist_ok=True)
os.makedirs(CLEANING_DIR, exist_ok=True)

# Output file
OUTPUT_FILE = os.path.join(DATA_QUALITY_DIR, 'data_quality_analysis_report.txt')

# Clear existing file
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

def print_and_write(text):
    """Print to console and write to output file"""
    print(text)
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        f.write(text + '\n')

# Define dataset files
datasets = {
    'orders': 'olist_orders_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'category_translation': 'product_category_name_translation.csv'
}

print_and_write("="*80)
print_and_write("DATA QUALITY ANALYSIS - BRAZILIAN E-COMMERCE DATASET")
print_and_write("="*80)
print_and_write(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load all datasets
print_and_write("\nLoading datasets...")
data = {}

for name, filename in datasets.items():
    filepath = os.path.join(RAW_DATA_PATH, filename)
    try:
        df = pd.read_csv(filepath)
        data[name] = df
        print_and_write(f"  [OK] {name}: {df.shape[0]:,} rows x {df.shape[1]} columns")
    except Exception as e:
        print_and_write(f"  [ERROR] {name}: {e}")

print_and_write(f"\nTotal datasets loaded: {len(data)}")

# 1. Comprehensive Missing Value Analysis
print_and_write("\n" + "="*80)
print_and_write("1. MISSING VALUE ANALYSIS")
print_and_write("="*80)

missing_report = []

for name, df in data.items():
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            missing_pct = round((missing_count / df.shape[0]) * 100, 2)
            
            # Determine severity
            if missing_pct > 50:
                severity = 'CRITICAL'
            elif missing_pct > 20:
                severity = 'HIGH'
            elif missing_pct > 5:
                severity = 'MEDIUM'
            else:
                severity = 'LOW'
            
            missing_report.append({
                'Dataset': name,
                'Column': col,
                'Missing_Count': missing_count,
                'Missing_Percentage': missing_pct,
                'Severity': severity,
                'Data_Type': str(df[col].dtype),
                'Total_Rows': df.shape[0]
            })

if missing_report:
    missing_df = pd.DataFrame(missing_report)
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    missing_df['Severity_Order'] = missing_df['Severity'].map(severity_order)
    missing_df = missing_df.sort_values(['Severity_Order', 'Missing_Percentage'], ascending=[True, False])
    missing_df = missing_df.drop('Severity_Order', axis=1)
    
    print_and_write(f"\nTotal columns with missing values: {len(missing_df)}")
    print_and_write("\nTop 20 missing value issues:")
    print_and_write(missing_df.head(20).to_string(index=False))
else:
    print_and_write("\nNo missing values found in any dataset!")

# 2. Data Type Analysis
print_and_write("\n" + "="*80)
print_and_write("2. DATA TYPE ANALYSIS")
print_and_write("="*80)

dtype_report = []

for name, df in data.items():
    for col in df.columns:
        dtype_report.append({
            'Dataset': name,
            'Column': col,
            'Data_Type': str(df[col].dtype),
            'Non_Null_Count': df[col].count(),
            'Null_Count': df[col].isnull().sum(),
            'Unique_Values': df[col].nunique()
        })

dtype_df = pd.DataFrame(dtype_report)
print_and_write(f"\nTotal columns analyzed: {len(dtype_df)}")
print_and_write("\nData type distribution:")
print_and_write(dtype_df['Data_Type'].value_counts().to_string())

# 3. Duplicate Analysis
print_and_write("\n" + "="*80)
print_and_write("3. DUPLICATE ANALYSIS")
print_and_write("="*80)

duplicate_report = []

for name, df in data.items():
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        duplicate_report.append({
            'Dataset': name,
            'Duplicate_Rows': dup_count,
            'Duplicate_Percentage': round((dup_count / df.shape[0]) * 100, 2),
            'Total_Rows': df.shape[0]
        })
        print_and_write(f"\n{name}: {dup_count:,} duplicates ({round((dup_count / df.shape[0]) * 100, 2)}%)")

if not duplicate_report:
    print_and_write("\nNo duplicates found in any dataset!")

# 3.5 Duplicate Columns Detection
print_and_write("\n" + "="*80)
print_and_write("3.5. DUPLICATE COLUMNS DETECTION")
print_and_write("="*80)

duplicate_columns_report = []

for name, df in data.items():
    cols = df.columns.tolist()
    duplicates_found = []
    
    for i, col1 in enumerate(cols):
        for col2 in cols[i+1:]:
            if df[col1].equals(df[col2]):
                duplicates_found.append((col1, col2))
                duplicate_columns_report.append({
                    'Dataset': name,
                    'Column_1': col1,
                    'Column_2': col2,
                    'Status': 'Duplicate (identical values)'
                })
    
    if duplicates_found:
        print_and_write(f"\n{name}: Found {len(duplicates_found)} duplicate column pair(s)")
        for col1, col2 in duplicates_found:
            print_and_write(f"  - {col1} == {col2}")

if not duplicate_columns_report:
    print_and_write("\nNo duplicate columns found in any dataset!")

# 4. Date Consistency Check
print_and_write("\n" + "="*80)
print_and_write("4. DATE CONSISTENCY CHECK")
print_and_write("="*80)

date_consistency_report = []

# Check orders dataset for date consistency
if 'orders' in data:
    df_orders = data['orders']
    
    # Convert date columns to datetime
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    
    for col in date_cols:
        if col in df_orders.columns:
            df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')
    
    # Check date logic: purchase < approved < carrier < delivered
    valid_dates = df_orders[
        (df_orders['order_purchase_timestamp'].notna()) &
        (df_orders['order_approved_at'].notna()) &
        (df_orders['order_delivered_carrier_date'].notna()) &
        (df_orders['order_delivered_customer_date'].notna())
    ].copy()
    
    # Check if dates are in correct order
    date_order_violations = valid_dates[
        (valid_dates['order_purchase_timestamp'] > valid_dates['order_approved_at']) |
        (valid_dates['order_approved_at'] > valid_dates['order_delivered_carrier_date']) |
        (valid_dates['order_delivered_carrier_date'] > valid_dates['order_delivered_customer_date'])
    ]
    
    # Check if delivered orders have delivery date
    delivered_orders = df_orders[df_orders['order_status'] == 'delivered']
    delivered_without_date = delivered_orders[delivered_orders['order_delivered_customer_date'].isna()]
    
    # Check if carrier date is before or equal to delivery date
    carrier_after_delivery = valid_dates[
        valid_dates['order_delivered_carrier_date'] > valid_dates['order_delivered_customer_date']
    ]
    
    # 1. Late Deliveries (delivered after estimated date)
    late_deliveries = delivered_orders[
        (delivered_orders['order_delivered_customer_date'].notna()) &
        (delivered_orders['order_estimated_delivery_date'].notna()) &
        (delivered_orders['order_delivered_customer_date'] > delivered_orders['order_estimated_delivery_date'])
    ]
    
    # 2. Long Transit Times (more than 30 days)
    transit_times = delivered_orders[
        (delivered_orders['order_purchase_timestamp'].notna()) & 
        (delivered_orders['order_delivered_customer_date'].notna())
    ].copy()
    transit_times['transit_days'] = (transit_times['order_delivered_customer_date'] - transit_times['order_purchase_timestamp']).dt.days
    long_transit = transit_times[transit_times['transit_days'] > 30]
    
    # 3. Timestamp Logic Errors (carrier pickup before approval)
    timestamp_errors = delivered_orders[
        (delivered_orders['order_approved_at'].notna()) & 
        (delivered_orders['order_delivered_carrier_date'].notna()) &
        (delivered_orders['order_delivered_carrier_date'] < delivered_orders['order_approved_at'])
    ]
    
    # 4. Delivery Sequence Issues (delivered before carrier pickup)
    delivery_sequence_errors = delivered_orders[
        (delivered_orders['order_delivered_carrier_date'].notna()) & 
        (delivered_orders['order_delivered_customer_date'].notna()) &
        (delivered_orders['order_delivered_customer_date'] < delivered_orders['order_delivered_carrier_date'])
    ]
    
    # 5. Missing Critical Data
    missing_data = delivered_orders[
        (delivered_orders['order_purchase_timestamp'].isna()) |
        (delivered_orders['order_approved_at'].isna()) |
        (delivered_orders['order_delivered_carrier_date'].isna()) |
        (delivered_orders['order_delivered_customer_date'].isna()) |
        (delivered_orders['order_estimated_delivery_date'].isna())
    ]
    
    date_consistency_report.append({
        'Dataset': 'orders',
        'Date_Order_Violations': len(date_order_violations),
        'Delivered_Without_Date': len(delivered_without_date),
        'Carrier_After_Delivery': len(carrier_after_delivery),
        'Late_Deliveries': len(late_deliveries),
        'Long_Transit_Times': len(long_transit),
        'Timestamp_Logic_Errors': len(timestamp_errors),
        'Delivery_Sequence_Issues': len(delivery_sequence_errors),
        'Missing_Critical_Data': len(missing_data),
        'Total_Delivered_Orders': len(delivered_orders),
        'Longest_Transit_Time': transit_times['transit_days'].max() if not transit_times.empty else 0
    })
    
    print_and_write(f"\nOrders Dataset:")
    print_and_write(f"  Total delivered orders: {len(delivered_orders)}")
    print_and_write(f"  Delivered without delivery date: {len(delivered_without_date)}")
    print_and_write(f"  Date order violations: {len(date_order_violations)}")
    print_and_write(f"  Carrier date after delivery: {len(carrier_after_delivery)}")
    print_and_write(f"  Late Deliveries: {len(late_deliveries)} ({round(len(late_deliveries)/len(delivered_orders)*100, 2)}%)")
    print_and_write(f"  Long Transit Times (>30 days): {len(long_transit)} ({round(len(long_transit)/len(delivered_orders)*100, 2)}%)")
    if not transit_times.empty:
        print_and_write(f"  Longest Transit Time: {transit_times['transit_days'].max()} days")
    print_and_write(f"  Timestamp Logic Errors: {len(timestamp_errors)} ({round(len(timestamp_errors)/len(delivered_orders)*100, 2)}%)")
    print_and_write(f"  Delivery Sequence Issues: {len(delivery_sequence_errors)}")
    print_and_write(f"  Missing Critical Data: {len(missing_data)}")
    
    if len(delivered_without_date) > 0:
        print_and_write(f"  [WARNING] {len(delivered_without_date)} delivered orders missing delivery date")
    
    if len(date_order_violations) > 0:
        print_and_write(f"  [WARNING] {len(date_order_violations)} orders with incorrect date sequence")
    
    if len(carrier_after_delivery) > 0:
        print_and_write(f"  [WARNING] {len(carrier_after_delivery)} orders with carrier date after delivery")
    
    if len(late_deliveries) > 0:
        print_and_write(f"  [WARNING] {len(late_deliveries)} orders delivered after estimated date")
    
    if len(long_transit) > 0:
        print_and_write(f"  [WARNING] {len(long_transit)} orders with transit time > 30 days")
    
    if len(timestamp_errors) > 0:
        print_and_write(f"  [WARNING] {len(timestamp_errors)} orders with carrier pickup before approval")
    
    if len(delivery_sequence_errors) > 0:
        print_and_write(f"  [WARNING] {len(delivery_sequence_errors)} orders delivered before carrier pickup")
    
    if len(missing_data) > 0:
        print_and_write(f"  [WARNING] {len(missing_data)} orders with missing critical data")

if not date_consistency_report:
    print_and_write("\nNo date columns found for consistency check.")

# 5. Outlier Detection
print_and_write("\n" + "="*80)
print_and_write("5. OUTLIER DETECTION (IQR Method)")
print_and_write("="*80)

outlier_report = []

for name, df in data.items():
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        if df[col].notna().sum() > 0:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
            outlier_count = len(outliers)
            outlier_pct = round((outlier_count / df[col].notna().sum()) * 100, 2)
            
            if outlier_count > 0:
                outlier_report.append({
                    'Dataset': name,
                    'Column': col,
                    'Outlier_Count': outlier_count,
                    'Outlier_Percentage': outlier_pct,
                    'Lower_Bound': round(lower_bound, 2),
                    'Upper_Bound': round(upper_bound, 2),
                    'Min_Value': round(df[col].min(), 2),
                    'Max_Value': round(df[col].max(), 2)
                })

if outlier_report:
    outlier_df = pd.DataFrame(outlier_report)
    print_and_write(f"\nTotal columns with outliers: {len(outlier_df)}")
    print_and_write("\nTop 10 columns with most outliers:")
    print_and_write(outlier_df.sort_values('Outlier_Count', ascending=False).head(10).to_string(index=False))
else:
    print_and_write("\nNo outliers detected in numerical columns.")

# 6. Data Quality Summary
print_and_write("\n" + "="*80)
print_and_write("6. DATA QUALITY SUMMARY")
print_and_write("="*80)

quality_summary = []

for name, df in data.items():
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    missing_pct = round((missing_cells / total_cells) * 100, 2)
    
    cols_with_missing = sum(df[col].isnull().sum() > 0 for col in df.columns)
    numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
    categorical_cols = len(df.select_dtypes(include=['object']).columns)
    
    quality_summary.append({
        'Dataset': name,
        'Rows': df.shape[0],
        'Columns': df.shape[1],
        'Numeric_Columns': numeric_cols,
        'Categorical_Columns': categorical_cols,
        'Missing_Cells': missing_cells,
        'Missing_Percentage': missing_pct,
        'Columns_With_Missing': cols_with_missing,
        'Duplicate_Rows': df.duplicated().sum(),
        'Memory_MB': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
    })

summary_df = pd.DataFrame(quality_summary)
summary_df = summary_df.sort_values('Missing_Percentage', ascending=False)

print_and_write("\nData Quality Dashboard:")
print_and_write(summary_df.to_string(index=False))

# 6. Cleaning Recommendations
print_and_write("\n" + "="*80)
print_and_write("6. DATA CLEANING RECOMMENDATIONS")
print_and_write("="*80)

recommendations = []

for name, df in data.items():
    # Check for missing values
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            missing_pct = round((missing_count / df.shape[0]) * 100, 2)
            
            if missing_pct > 50:
                action = "DROP COLUMN - Too many missing values"
                priority = "HIGH"
            elif missing_pct > 20:
                action = "IMPUTE or DROP ROWS - Consider imputation or removal"
                priority = "HIGH"
            elif missing_pct > 5:
                action = "IMPUTE - Consider mean/median/forward-fill"
                priority = "MEDIUM"
            else:
                action = "IMPUTE - Low impact imputation"
                priority = "LOW"
            
            recommendations.append({
                'Dataset': name,
                'Column': col,
                'Missing_Percentage': missing_pct,
                'Action': action,
                'Priority': priority
            })

    # Check for duplicates
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        recommendations.append({
            'Dataset': name,
            'Column': 'Full Row',
            'Missing_Percentage': 0,
            'Action': f"REMOVE DUPLICATES - {dup_count} rows",
            'Priority': "MEDIUM"
        })

    # Check for outliers in numerical columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].notna().sum() > 0:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
            outlier_count = len(outliers)
            outlier_pct = round((outlier_count / df[col].notna().sum()) * 100, 2)
            
            if outlier_pct > 5:
                recommendations.append({
                    'Dataset': name,
                    'Column': col,
                    'Missing_Percentage': 0,
                    'Action': f"TREAT OUTLIERS - {outlier_pct}% outliers using IQR method",
                    'Priority': "MEDIUM"
                })

# Generate recommendations report
if recommendations:
    rec_df = pd.DataFrame(recommendations)
    priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    rec_df['Priority_Order'] = rec_df['Priority'].map(priority_order)
    rec_df = rec_df.sort_values(['Priority_Order', 'Missing_Percentage'], ascending=[True, False])
    rec_df = rec_df.drop('Priority_Order', axis=1)
    
    print_and_write("\nRecommended Actions:")
    print_and_write(rec_df.to_string(index=False))
else:
    print_and_write("\nNo cleaning recommendations needed - all data is clean!")

# Final summary
print_and_write("\n" + "="*80)
print_and_write("ANALYSIS COMPLETE")
print_and_write("="*80)
print_and_write(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print_and_write(f"\nGenerated report: {OUTPUT_FILE}")
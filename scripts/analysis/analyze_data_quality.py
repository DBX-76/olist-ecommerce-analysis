"""
Data Quality Analysis Script for Brazilian E-Commerce Dataset
This script analyzes data quality issues and generates comprehensive reports.
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
# Project root should be two levels above this script (repo root)
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

# Ensure reports directory exists
os.makedirs(REPORTS_PATH, exist_ok=True)

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

print("="*80)
print("DATA QUALITY ANALYSIS - BRAZILIAN E-COMMERCE DATASET")
print("="*80)
print(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load all datasets
print("\nLoading datasets...")
data = {}

for name, filename in datasets.items():
    filepath = os.path.join(RAW_DATA_PATH, filename)
    try:
        df = pd.read_csv(filepath)
        data[name] = df
        print(f"  [OK] {name}: {df.shape[0]:,} rows x {df.shape[1]} columns")
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")

print(f"\nTotal datasets loaded: {len(data)}")

# 1. Comprehensive Missing Value Analysis
print("\n" + "="*80)
print("1. MISSING VALUE ANALYSIS")
print("="*80)

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
    
    print(f"\nTotal columns with missing values: {len(missing_df)}")
    print("\nTop 20 missing value issues:")
    print(missing_df.head(20).to_string(index=False))
    
    # Save to CSV
    missing_df.to_csv(os.path.join(REPORTS_PATH, 'missing_values_report.csv'), index=False)
    print(f"\n[OK] Missing values report saved: {os.path.join(REPORTS_PATH, 'missing_values_report.csv')}")
else:
    print("\nNo missing values found in any dataset!")

# 2. Data Type Analysis
print("\n" + "="*80)
print("2. DATA TYPE ANALYSIS")
print("="*80)

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
print(f"\nTotal columns analyzed: {len(dtype_df)}")
print("\nData type distribution:")
print(dtype_df['Data_Type'].value_counts().to_string())

dtype_df.to_csv(os.path.join(REPORTS_PATH, 'data_types_report.csv'), index=False)
print(f"\n[OK] Data types report saved: {os.path.join(REPORTS_PATH, 'data_types_report.csv')}")

# 3. Duplicate Analysis
print("\n" + "="*80)
print("3. DUPLICATE ANALYSIS")
print("="*80)

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
        print(f"\n{name}: {dup_count:,} duplicates ({round((dup_count / df.shape[0]) * 100, 2)}%)")

if duplicate_report:
    dup_df = pd.DataFrame(duplicate_report)
    dup_df.to_csv(os.path.join(REPORTS_PATH, 'duplicates_report.csv'), index=False)
    print(f"\n[OK] Duplicates report saved: {os.path.join(REPORTS_PATH, 'duplicates_report.csv')}")
else:
    print("\nNo duplicates found in any dataset!")

# 3.5 Duplicate Columns Detection
print("\n" + "="*80)
print("3.5. DUPLICATE COLUMNS DETECTION")
print("="*80)

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
        print(f"\n{name}: Found {len(duplicates_found)} duplicate column pair(s)")
        for col1, col2 in duplicates_found:
            print(f"  - {col1} == {col2}")

if duplicate_columns_report:
    dup_cols_df = pd.DataFrame(duplicate_columns_report)
    dup_cols_df.to_csv(os.path.join(REPORTS_PATH, 'duplicate_columns_report.csv'), index=False)
    print(f"\n[OK] Duplicate columns report saved: {os.path.join(REPORTS_PATH, 'duplicate_columns_report.csv')}")
else:
    print("\nNo duplicate columns found in any dataset!")

# 4. Date Consistency Check
print("\n" + "="*80)
print("4. DATE CONSISTENCY CHECK")
print("="*80)

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
    
    print(f"\nOrders Dataset:")
    print(f"  Total delivered orders: {len(delivered_orders)}")
    print(f"  Delivered without delivery date: {len(delivered_without_date)}")
    print(f"  Date order violations: {len(date_order_violations)}")
    print(f"  Carrier date after delivery: {len(carrier_after_delivery)}")
    print(f"  Late Deliveries: {len(late_deliveries)} ({round(len(late_deliveries)/len(delivered_orders)*100, 2)}%)")
    print(f"  Long Transit Times (>30 days): {len(long_transit)} ({round(len(long_transit)/len(delivered_orders)*100, 2)}%)")
    if not transit_times.empty:
        print(f"  Longest Transit Time: {transit_times['transit_days'].max()} days")
    print(f"  Timestamp Logic Errors: {len(timestamp_errors)} ({round(len(timestamp_errors)/len(delivered_orders)*100, 2)}%)")
    print(f"  Delivery Sequence Issues: {len(delivery_sequence_errors)}")
    print(f"  Missing Critical Data: {len(missing_data)}")
    
    if len(delivered_without_date) > 0:
        print(f"  [WARNING] {len(delivered_without_date)} delivered orders missing delivery date")
    
    if len(date_order_violations) > 0:
        print(f"  [WARNING] {len(date_order_violations)} orders with incorrect date sequence")
    
    if len(carrier_after_delivery) > 0:
        print(f"  [WARNING] {len(carrier_after_delivery)} orders with carrier date after delivery")
    
    if len(late_deliveries) > 0:
        print(f"  [WARNING] {len(late_deliveries)} orders delivered after estimated date")
    
    if len(long_transit) > 0:
        print(f"  [WARNING] {len(long_transit)} orders with transit time > 30 days")
    
    if len(timestamp_errors) > 0:
        print(f"  [WARNING] {len(timestamp_errors)} orders with carrier pickup before approval")
    
    if len(delivery_sequence_errors) > 0:
        print(f"  [WARNING] {len(delivery_sequence_errors)} orders delivered before carrier pickup")
    
    if len(missing_data) > 0:
        print(f"  [WARNING] {len(missing_data)} orders with missing critical data")

if date_consistency_report:
    date_df = pd.DataFrame(date_consistency_report)
    date_df.to_csv(os.path.join(REPORTS_PATH, 'date_consistency_report.csv'), index=False)
    print(f"\n[OK] Date consistency report saved: {os.path.join(REPORTS_PATH, 'date_consistency_report.csv')}")
else:
    print("\nNo date columns found for consistency check.")

# 5. Outlier Detection
print("\n" + "="*80)
print("5. OUTLIER DETECTION (IQR Method)")
print("="*80)

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
    print(f"\nTotal columns with outliers: {len(outlier_df)}")
    print("\nTop 10 columns with most outliers:")
    print(outlier_df.sort_values('Outlier_Count', ascending=False).head(10).to_string(index=False))
    
    outlier_df.to_csv(os.path.join(REPORTS_PATH, 'outliers_report.csv'), index=False)
    print(f"\n[OK] Outliers report saved: {os.path.join(REPORTS_PATH, 'outliers_report.csv')}")
else:
    print("\nNo outliers detected in numerical columns.")

# 6. Data Quality Summary
print("\n" + "="*80)
print("6. DATA QUALITY SUMMARY")
print("="*80)

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

print("\nData Quality Dashboard:")
print(summary_df.to_string(index=False))

summary_df.to_csv(os.path.join(REPORTS_PATH, 'data_quality_summary.csv'), index=False)
print(f"\n[OK] Data quality summary saved: {os.path.join(REPORTS_PATH, 'data_quality_summary.csv')}")

# 6. Cleaning Recommendations
print("\n" + "="*80)
print("6. DATA CLEANING RECOMMENDATIONS")
print("="*80)

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
                priority = "MEDIUM"
            elif missing_pct < 5:
                action = "IMPUTE - Use mean/median/mode or forward fill"
                priority = "LOW"
            else:
                action = "REVIEW - Investigate cause of missing values"
                priority = "MEDIUM"
            
            recommendations.append({
                'Dataset': name,
                'Column': col,
                'Missing_Percentage': missing_pct,
                'Priority': priority,
                'Recommended_Action': action
            })
    
    # Check for duplicates
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        recommendations.append({
            'Dataset': name,
            'Column': 'ALL_COLUMNS',
            'Missing_Percentage': 0,
            'Priority': 'MEDIUM',
            'Recommended_Action': f'REMOVE {dup_count} DUPLICATE ROWS'
        })

if recommendations:
    rec_df = pd.DataFrame(recommendations)
    priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    rec_df['Priority_Order'] = rec_df['Priority'].map(priority_order)
    rec_df = rec_df.sort_values(['Priority_Order', 'Missing_Percentage'], ascending=[True, False])
    rec_df = rec_df.drop('Priority_Order', axis=1)
    
    print(f"\nTotal recommendations: {len(rec_df)}")
    print("\nHIGH Priority Issues:")
    high_priority = rec_df[rec_df['Priority'] == 'HIGH']
    if not high_priority.empty:
        print(high_priority.to_string(index=False))
    else:
        print("  None")
    
    print("\nMEDIUM Priority Issues:")
    medium_priority = rec_df[rec_df['Priority'] == 'MEDIUM'].head(10)
    if not medium_priority.empty:
        print(medium_priority.to_string(index=False))
    else:
        print("  None")
    
    rec_df.to_csv(os.path.join(REPORTS_PATH, 'cleaning_recommendations.csv'), index=False)
    print(f"\n[OK] Cleaning recommendations saved: {os.path.join(REPORTS_PATH, 'cleaning_recommendations.csv')}")
else:
    print("\nNo data quality issues found. Data is clean!")

# Final Summary
print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

print("\nGenerated Reports:")
print("  1. missing_values_report.csv - Detailed missing value analysis")
print("  2. data_types_report.csv - Data type information")
print("  3. duplicates_report.csv - Duplicate row analysis")
print("  4. duplicate_columns_report.csv - Duplicate column detection")
print("  5. outliers_report.csv - Outlier detection")
print("  6. date_consistency_report.csv - Date consistency and logic checks")
print("  7. data_quality_summary.csv - Overall quality dashboard")
print("  8. cleaning_recommendations.csv - Actionable recommendations")

print(f"\nAnalysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\nKey Findings:")
print(f"  - Total datasets analyzed: {len(data)}")
print(f"  - Total rows across all datasets: {sum(df.shape[0] for df in data.values()):,}")
print(f"  - Datasets with missing values: {sum(1 for df in data.values() if df.isnull().sum().sum() > 0)}")
print(f"  - Datasets with duplicates: {sum(1 for df in data.values() if df.duplicated().sum() > 0)}")

print("\nNext Steps:")
print("  1. Review cleaning recommendations in reports/cleaning_recommendations.csv")
print("  2. Prioritize HIGH priority issues")
print("  3. Create data cleaning scripts")
print("  4. Validate cleaned data")
print("  5. Proceed with advanced analysis")
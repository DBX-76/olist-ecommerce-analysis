import os
import pandas as pd
from datetime import datetime
import yaml

# Load configuration
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

RAW_DATA_PATH = config['paths']['raw_data']
PROCESSED_DATA_PATH = config['paths']['processed_data']
REPORTS_PATH = config['paths']['reports']

# Load raw datasets
print("Loading raw datasets...")
df_reviews = pd.read_csv(os.path.join(RAW_DATA_PATH, 'olist_order_reviews_dataset.csv'))
df_orders = pd.read_csv(os.path.join(RAW_DATA_PATH, 'olist_orders_dataset.csv'))
df_products = pd.read_csv(os.path.join(RAW_DATA_PATH, 'olist_products_dataset.csv'))
df_geolocation = pd.read_csv(os.path.join(RAW_DATA_PATH, 'olist_geolocation_dataset.csv'))

print(f"[OK] order_reviews: {df_reviews.shape[0]:,} rows x {df_reviews.shape[1]} columns")
print(f"[OK] orders: {df_orders.shape[0]:,} rows x {df_orders.shape[1]} columns")
print(f"[OK] products: {df_products.shape[0]:,} rows x {df_products.shape[1]} columns")
print(f"[OK] geolocation: {df_geolocation.shape[0]:,} rows x {df_geolocation.shape[1]} columns")

# Prepare cleaning report collector
report_entries = []

# 1. Clean order_reviews dataset
print("\n" + "="*80)
print("1. CLEANING ORDER_REVIEWS DATASET")
print("="*80)

print("\nBefore cleaning:")
print(f"  Columns: {list(df_reviews.columns)}")
print(f"  Missing values:\n{df_reviews.isnull().sum()}")

# Fill missing values with "non renseigné"
# These columns are related to review_score and should be kept
df_reviews_clean = df_reviews.copy()
df_reviews_clean['review_comment_title'] = df_reviews_clean['review_comment_title'].fillna('non renseigné')
df_reviews_clean['review_comment_message'] = df_reviews_clean['review_comment_message'].fillna('non renseigné')

print(f"\nFilled missing values with 'non renseigné':")
print(f"  - review_comment_title")
print(f"  - review_comment_message")

print(f"\nAfter cleaning:")
print(f"  Columns: {list(df_reviews_clean.columns)}")
print(f"  Missing values:\n{df_reviews_clean.isnull().sum()}")

# Remove exact duplicate rows in reviews (if any)
before_reviews = len(df_reviews_clean)
df_reviews_clean = df_reviews_clean.drop_duplicates()
removed_reviews = before_reviews - len(df_reviews_clean)
if removed_reviews:
    report_entries.append({
        'timestamp': datetime.now().isoformat(),
        'dataset': 'order_reviews',
        'action': 'drop_duplicates',
        'removed': int(removed_reviews),
        'details': ''
    })
    print(f"  Removed {removed_reviews} exact duplicate row(s) from order_reviews")

# Save cleaned dataset
df_reviews_clean.to_csv(os.path.join(PROCESSED_DATA_PATH, 'olist_order_reviews_clean.csv'), index=False)
print(f"[OK] Saved: {os.path.join(PROCESSED_DATA_PATH, 'olist_order_reviews_clean.csv')}")

# 2. Clean products dataset
print("\n" + "="*80)
print("2. CLEANING PRODUCTS DATASET")
print("="*80)

df_products['product_category_name'] = df_products['product_category_name'].fillna('unknown')
df_products.to_csv(os.path.join(PROCESSED_DATA_PATH, 'olist_products_clean.csv'), index=False)
print("\nBefore cleaning:")
print(f"  Missing values:\n{df_products.isnull().sum()}")

#  - remove specific bad product row
remove_product_id = '5eb564652db742ff8f28759cd8d2652a'
if remove_product_id in df_products['product_id'].values:
    df_products = df_products[df_products['product_id'] != remove_product_id].reset_index(drop=True)
    report_entries.append({
        'timestamp': datetime.now().isoformat(),
        'dataset': 'products',
        'action': 'remove_product_id',
        'removed': 1,
        'details': remove_product_id
    })
    print(f"  Removed product row with product_id={remove_product_id}")

# Imputation strategy:
#  - do NOT impute `product_photos_qty` (no longer needed)
#  - impute size/weight columns for category 'bebes' using the category median
#  - keep imputing lightweight textual/length columns globally
global_numeric_cols = ['product_name_lenght', 'product_description_lenght']
bebes_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

# Ensure category column exists and fill unknowns
df_products['product_category_name'] = df_products['product_category_name'].fillna('unknown')
print(f"  Imputed product_category_name with 'unknown' (where missing)")

# Compute medians for 'bebes' and apply to rows in that category
mask_bebes = df_products['product_category_name'].str.lower() == 'bebes'
if mask_bebes.any():
    medians_bebes = df_products.loc[mask_bebes, bebes_cols].median()
    for col in bebes_cols:
        median_value = medians_bebes[col]
        df_products.loc[mask_bebes, col] = df_products.loc[mask_bebes, col].fillna(median_value)
        report_entries.append({
            'timestamp': datetime.now().isoformat(),
            'dataset': 'products',
            'action': 'impute_category_median',
            'removed': 0,
            'details': f"category=bebes;column={col};median={median_value}"
        })
        print(f"  Imputed {col} for category 'bebes' with median: {median_value}")
else:
    print("  Category 'bebes' not found — skipping category-based imputation")

# Impute remaining global numeric columns with dataset median
for col in global_numeric_cols:
    if col in df_products.columns:
        median_value = df_products[col].median()
        df_products[col] = df_products[col].fillna(median_value)
        report_entries.append({
            'timestamp': datetime.now().isoformat(),
            'dataset': 'products',
            'action': 'impute_global_median',
            'removed': 0,
            'details': f"column={col};median={median_value}"
        })
        print(f"  Imputed {col} with median: {median_value}")

print(f"\nAfter cleaning:")
print(f"  Missing values:\n{df_products.isnull().sum()}")

# Remove exact duplicate rows in products (if any)
before_products = len(df_products)
df_products = df_products.drop_duplicates()
removed_products = before_products - len(df_products)
if removed_products:
    report_entries.append({
        'timestamp': datetime.now().isoformat(),
        'dataset': 'products',
        'action': 'drop_duplicates',
        'removed': int(removed_products),
        'details': ''
    })
    print(f"  Removed {removed_products} exact duplicate row(s) from products")

# Save cleaned dataset
df_products.to_csv(os.path.join(PROCESSED_DATA_PATH, 'olist_products_clean.csv'), index=False)
print(f"[OK] Saved: {os.path.join(PROCESSED_DATA_PATH, 'olist_products_clean.csv')}")

# 3. Clean orders dataset
print("\n" + "="*80)
print("3. CLEANING ORDERS DATASET")
print("="*80)

print("\nBefore cleaning:")
print(f"  Missing values:\n{df_orders.isnull().sum()}")

# Analyze missing dates by order status
print("\nAnalyzing missing dates by order status:")
for date_col in ['order_delivered_customer_date', 'order_delivered_carrier_date', 'order_approved_at']:
    missing_by_status = df_orders[df_orders[date_col].isna()]['order_status'].value_counts()
    print(f"\n  {date_col}:")
    print(f"    {missing_by_status.to_dict()}")

# Note: Missing dates are expected for non-delivered orders
# We will NOT impute these values as they represent legitimate business logic

print("\nDecision: No imputation needed - missing dates are expected for non-delivered orders")

# Remove exact duplicate rows in orders (if any)
before_orders = len(df_orders)
df_orders = df_orders.drop_duplicates()
removed_orders = before_orders - len(df_orders)
if removed_orders:
    report_entries.append({
        'timestamp': datetime.now().isoformat(),
        'dataset': 'orders',
        'action': 'drop_duplicates',
        'removed': int(removed_orders),
        'details': ''
    })
    print(f"  Removed {removed_orders} exact duplicate row(s) from orders")

# Save orders dataset (unchanged, but documented)
df_orders.to_csv(os.path.join(PROCESSED_DATA_PATH, 'olist_orders_clean.csv'), index=False)
print(f"[OK] Saved: {os.path.join(PROCESSED_DATA_PATH, 'olist_orders_clean.csv')}")

# 4. Clean geolocation dataset
print("\n" + "="*80)
print("4. CLEANING GEOLOCATION DATASET")
print("="*80)

print("\nBefore cleaning:")
print(f"  Rows: {df_geolocation.shape[0]:,}")
print(f"  Duplicates: {df_geolocation.duplicated().sum():,} ({round((df_geolocation.duplicated().sum() / df_geolocation.shape[0]) * 100, 2)}%)")

df_geolocation_clean = df_geolocation.drop_duplicates()
# Remove duplicates
before_geo = len(df_geolocation)
df_geolocation_clean = df_geolocation.drop_duplicates()
removed_geo = before_geo - len(df_geolocation_clean)
if removed_geo:
    report_entries.append({
        'timestamp': datetime.now().isoformat(),
        'dataset': 'geolocation',
        'action': 'drop_duplicates',
        'removed': int(removed_geo),
        'details': ''
    })

print(f"\nAfter cleaning:")
print(f"  Rows: {df_geolocation_clean.shape[0]:,}")
print(f"  Duplicates removed: {removed_geo}")

# Save cleaned dataset
df_geolocation_clean.to_csv(os.path.join(PROCESSED_DATA_PATH, 'olist_geolocation_clean.csv'), index=False)
print(f"[OK] Saved: {os.path.join(PROCESSED_DATA_PATH, 'olist_geolocation_clean.csv')}")

# 5. Copy other datasets (no cleaning needed)
print("\n" + "="*80)
print("5. COPYING OTHER DATASETS (NO CLEANING NEEDED)")
print("="*80)

other_datasets = {
    'customers': 'olist_customers_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'category_translation': 'product_category_name_translation.csv'
}

for name, filename in other_datasets.items():
    df = pd.read_csv(os.path.join(RAW_DATA_PATH, filename))
    # remove exact duplicate rows before saving
    before_other = len(df)
    df = df.drop_duplicates()
    removed_other = before_other - len(df)
    output_filename = filename.replace('.csv', '_clean.csv')
    df.to_csv(os.path.join(PROCESSED_DATA_PATH, output_filename), index=False)
    print(f"[OK] Copied {name} -> {output_filename} (removed {removed_other} duplicates)")
    if removed_other:
        report_entries.append({
            'timestamp': datetime.now().isoformat(),
            'dataset': name,
            'action': 'drop_duplicates',
            'removed': int(removed_other),
            'details': filename
        })

# 6. Generate cleaning summary
print("\n" + "="*80)
print("6. CLEANING SUMMARY")
print("="*80)

cleaning_summary = [
    {
        'Dataset': 'order_reviews',
        'Original_Rows': df_reviews.shape[0],
        'Original_Columns': df_reviews.shape[1],
        'Cleaned_Rows': df_reviews_clean.shape[0],
        'Cleaned_Columns': df_reviews_clean.shape[1],
        'Columns_Dropped': 0,
        'Action': 'Filled 2 columns with "non renseigné"'
    },
    {
        'Dataset': 'products',
        'Original_Rows': df_products.shape[0],
        'Original_Columns': df_products.shape[1],
        'Cleaned_Rows': df_products.shape[0],
        'Cleaned_Columns': df_products.shape[1],
        'Columns_Dropped': 0,
        'Action': 'Imputed 8 columns with median/unknown'
    },
    {
        'Dataset': 'orders',
        'Original_Rows': df_orders.shape[0],
        'Original_Columns': df_orders.shape[1],
        'Cleaned_Rows': df_orders.shape[0],
        'Cleaned_Columns': df_orders.shape[1],
        'Columns_Dropped': 0,
        'Action': 'No cleaning needed - missing dates are expected'
    },
    {
        'Dataset': 'geolocation',
        'Original_Rows': df_geolocation.shape[0],
        'Original_Columns': df_geolocation.shape[1],
        'Cleaned_Rows': df_geolocation_clean.shape[0],
        'Cleaned_Columns': df_geolocation_clean.shape[1],
        'Columns_Dropped': 0,
        'Action': f'Removed {df_geolocation.duplicated().sum():,} duplicates'
    }
]

summary_df = pd.DataFrame(cleaning_summary)
print("\nCleaning Summary:")
print(summary_df.to_string(index=False))

# Save summary
# Ensure cleaning report directory exists and write summary there
cleaning_report_dir = os.path.join(REPORTS_PATH, 'cleaning')
os.makedirs(cleaning_report_dir, exist_ok=True)
summary_df.to_csv(os.path.join(cleaning_report_dir, 'cleaning_summary.csv'), index=False)
print(f"\n[OK] Cleaning summary saved: {os.path.join(cleaning_report_dir, 'cleaning_summary.csv')}")

# Final summary
print("\n" + "="*80)
print("CLEANING COMPLETE")
print("="*80)

print(f"\nCleaned datasets saved to: {PROCESSED_DATA_PATH}")
print("\nFiles created:")
for filename in os.listdir(PROCESSED_DATA_PATH):
    if filename.endswith('_clean.csv'):
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        df = pd.read_csv(filepath)
        print(f"  - {filename}: {df.shape[0]:,} rows x {df.shape[1]} columns")

print(f"\nCleaning completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\nNext Steps:")
print("  1. Validate cleaned datasets")
print("  2. Proceed with exploratory data analysis")
print("  3. Create visualizations and reports")

# Write detailed cleaning report to reports/cleaning
cleaning_report_dir = os.path.join(REPORTS_PATH, 'cleaning')
os.makedirs(cleaning_report_dir, exist_ok=True)
if report_entries:
    report_df = pd.DataFrame(report_entries)
    report_path = os.path.join(cleaning_report_dir, f"cleaning_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Detailed cleaning report written to: {report_path}")
else:
    print("[OK] No detailed cleaning actions to report.")

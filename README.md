# Brazilian E-Commerce Data Analysis Project

[![Full Pipeline](https://github.com/DBX-76/olist-ecommerce-analysis/actions/workflows/pipeline.yml/badge.svg)](https://github.com/DBX-76/olist-ecommerce-analysis/actions/workflows/pipeline.yml)

## 📊 Project Overview

This project contains the Brazilian E-Commerce Public Dataset by Olist, a comprehensive collection of data about orders, customers, products, and sellers from a Brazilian marketplace. The project includes advanced data quality analysis, anomaly detection, and geographic data standardization.

## 📁 Updated Project Structure

```
Projet/
├── data/
│   ├── raw/                    # Original CSV files (NOT INCLUDED in Git - see data/README.md)
│   ├── processed/              # Cleaned and processed data
│   │   ├── zip_code_reference.csv      # Geographic reference table
│   │   ├── customers_standardized.csv  # Standardized customer data
│   │   ├── customers_with_geolocation.csv  # Enriched customer data
│   │   ├── sellers_standardized.csv    # Standardized seller data
│   │   ├── sellers_with_geolocation.csv    # Enriched seller data
│   │   ├── financial_analysis/         # Financial analysis results
│   │   ├── advanced_cleaning/          # Advanced cleaning results
│   │   └── product_review_analysis/    # Product and review analysis
│   └── external/               # External reference data (NOT INCLUDED in Git)
├── notebooks/                  # Jupyter notebooks for analysis
│   ├── 01_Exploratory_Data_Analysis.ipynb
│   └── 02_Data_Quality_Analysis.ipynb
├── scripts/                    # Python scripts for data processing
│   ├── core/                   # Core processing scripts
│   │   ├── create_zip_code_reference.py    # Geographic reference creation
│   │   ├── standardize_customers.py        # Customer standardization
│   │   ├── enrich_customers_with_geolocation.py    # Customer enrichment
│   │   ├── detect_seller_anomalies.py      # Seller anomaly detection
│   │   ├── standardize_sellers.py          # Seller standardization
│   │   └── enrich_sellers_with_geolocation.py      # Seller enrichment
│   ├── analysis/               # Analysis scripts
│   │   ├── analyze_data_quality.py         # Data quality analysis
│   │   └── clean_data.py                 # Data cleaning
│   └── utils/                  # Utility scripts
│       ├── organize_reports.py
│       └── test_profiling.py
├── reports/                    # Generated reports and visualizations
│   ├── eda/                    # EDA reports (HTML)
│   ├── data_quality/           # Data quality reports (CSV)
│   ├── cleaning/               # Data cleaning reports (CSV)
│   └── anomaly_detection/      # Anomaly detection reports (CSV)
├── docs/                       # Additional documentation
│   └── Data_Quality_Analysis_Guide.md
├── config/                     # Configuration files
│   ├── config.yaml             # Main configuration
│   ├── data_config.yaml        # Data configuration
│   ├── project_config.yaml     # Project settings
│   └── settings.yaml           # Additional settings
├── tmp/                        # Temporary files
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── test_pipeline.py           # Test script (renamed from test_anomalies.py)
```

## 🚀 Getting Started

### Prerequisites
- Python 3.7+
- Jupyter Notebook
- Required libraries: pandas, numpy, matplotlib, seaborn, ydata-profiling

### Installation

```bash
# Install required packages
pip install -r requirements.txt
```

### Quick Start

1. **Create geographic reference table**:
```bash
python scripts/core/create_zip_code_reference.py
```

2. **Standardize and enrich customer data**:
```bash
python scripts/core/standardize_customers.py
python scripts/core/enrich_customers_with_geolocation.py
```

3. **Detect and handle seller anomalies**:
```bash
python scripts/core/detect_seller_anomalies.py
python scripts/core/standardize_sellers.py
python scripts/core/enrich_sellers_with_geolocation.py
```

4. **Run comprehensive data quality analysis**:
```bash
python scripts/analysis/analyze_data_quality.py
```

5. **Clean the data**:
```bash
python scripts/analysis/clean_data.py
```

6. **View the generated reports**:
   - Open HTML files in [`reports/eda/`](reports/eda/) directory
   - Check anomaly detection reports in [`reports/anomaly_detection/`](reports/anomaly_detection/)

---

**Running the full pipeline (recommended)**

To make it easy to reproduce the full run locally, helper runner scripts are provided.

- `run_pipeline.ps1` — PowerShell runner (Windows).
- `run_pipeline.py` — Cross-platform Python runner (Windows/macOS/Linux). Recommended if you don't use PowerShell.

From the repository root (PowerShell):

```powershell
.\run_pipeline.ps1
```

Or using Python (cross-platform):

```bash
python run_pipeline.py
```

Notes:
- The runners create a virtual environment at `.venv` if missing and install packages from `requirements.txt`.
- Generated processed data (`data/processed/`) and large reports SHOULD NOT be committed to git. The repository includes `.gitignore` recommendations — keep these files out of version control to keep the repo lightweight.
- If you prefer to run steps manually, follow the sequence in the Quick Start section above.

## 🎯 Enhanced Features

### 1. Geographic Data Standardization
- **Zip Code Reference Table**: Creates a canonical geographic reference with statistical measures
- **City Name Standardization**: Resolves inconsistencies in city names across datasets
- **Quality Metrics**: Assigns data quality scores based on sample sizes

### 2. Anomaly Detection
- **Customer Location Anomalies**: Identifies inconsistent location data
- **Seller Location Anomalies**: Detects problematic seller location entries
- **Automated Corrections**: Applies rules to fix common anomalies

### 3. Data Enrichment
- **Customer Geolocation**: Adds geographic coordinates to customer records
- **Seller Geolocation**: Adds geographic coordinates to seller records
- **Spread Metrics**: Calculates geographic spread for quality assessment

## 📊 Analysis Pipeline

### Step 1: Geographic Reference Creation
```python
# Creates zip_code_reference.csv with:
# - Canonical city names (most frequent variant)
# - Average coordinates
# - Coordinate spread metrics
# - Data quality indicators
```

### Step 2: Customer Processing
```python
# Standardizes customer city names using reference table
# Enriches with geographic coordinates
# Tracks standardization metrics
```

### Step 3: Seller Processing
```python
# Detects anomalies in seller location data
# Standardizes city names
# Enriches with geographic coordinates
```

### Step 4: Quality Assessment
```python
# Generates comprehensive quality reports
# Identifies remaining issues
# Provides actionable recommendations
```

## 📋 Dataset Description

The dataset contains 9 CSV files with information about 100k orders from 2016 to 2018:

### Core Datasets

1. **olist_orders_dataset.csv** - Orders information
2. **olist_customers_dataset.csv** - Customer information  
3. **olist_order_items_dataset.csv** - Order items
4. **olist_order_payments_dataset.csv** - Payment information
5. **olist_order_reviews_dataset.csv** - Customer reviews

### Supporting Datasets

6. **olist_products_dataset.csv** - Product information
7. **olist_sellers_dataset.csv** - Seller information
8. **olist_geolocation_dataset.csv** - Geolocation data
9. **product_category_name_translation.csv** - Category translations

## 📊 Enhanced Features

### 1. Geographic Data Standardization
- **Zip Code Reference Table**: Creates a canonical geographic reference with statistical measures
- **City Name Standardization**: Resolves inconsistencies in city names across datasets
- **Quality Metrics**: Assigns data quality scores based on sample sizes

### 2. Financial Anomaly Detection
- **Payment/Order Reconciliation**: Identifies discrepancies between order totals and payment amounts
- **Critical Anomaly Detection**: Flags orders marked as delivered without corresponding payments
- **Financial Discrepancy Analysis**: Detailed analysis of monetary inconsistencies

### 3. Advanced Data Cleaning
- **Corruption Handling**: Removes corrupted records (e.g., shipped orders without items)
- **Exception Documentation**: Documents legitimate exceptions (gift orders, marketing campaigns)
- **Technical Corrections**: Fixes technical issues (installments, payment types)
- **Tax Reconciliation Preparation**: Prepares data for fiscal reconciliation

### 4. Product and Review Anomaly Analysis
- **Product Dimension Analysis**: Detects missing dimensions and implausible densities
- **Unit Conversion**: Corrects units (mm/cm, kg/g) for accurate measurements
- **Review Temporal Analysis**: Identifies reviews posted before purchase
- **Silent Review Detection**: Identifies reviews with no comments

### 5. Anomaly Detection
- **Customer Location Anomalies**: Identifies inconsistent location data
- **Seller Location Anomalies**: Detects problematic seller location entries
- **Automated Corrections**: Applies rules to fix common anomalies

### 6. Data Enrichment
- **Customer Geolocation**: Adds geographic coordinates to customer records
- **Seller Geolocation**: Adds geographic coordinates to seller records
- **Spread Metrics**: Calculates geographic spread for quality assessment

## 📊 Key Outputs

### Processed Data Files
- **`zip_code_reference.csv`**: Geographic reference with canonical names and coordinates
- **`customers_with_geolocation.csv`**: Customers enriched with geographic data (99,441 records)
- **`sellers_with_geolocation.csv`**: Sellers enriched with geographic data (3,095 records)
- **`customers_standardized.csv`**: Customers with standardized city names
- **`sellers_standardized.csv`**: Sellers with standardized city names
- **`financial_analysis/order_items_clean.csv`**: Cleaned order items with anomaly flags
- **`financial_analysis/order_payments_clean.csv`**: Cleaned order payments with anomaly flags
- **`financial_analysis/order_financial_reconciliation.csv`**: Financial reconciliation data
- **`advanced_cleaning/orders_advanced_cleaned.csv`**: Orders with advanced cleaning and documentation
- **`advanced_cleaning/order_items_advanced_cleaned.csv`**: Order items with advanced cleaning
- **`advanced_cleaning/order_payments_advanced_cleaned.csv`**: Order payments with advanced cleaning
- **`product_review_analysis/products_quality_analyzed.csv`**: Products with quality analysis and anomaly flags
- **`product_review_analysis/reviews_quality_analyzed.csv`**: Reviews with quality analysis and anomaly flags

## Documentation

- [Schéma de la base de données](docs/sql/schema.md)

### Generated Reports
- **Anomaly Detection Reports**: Detailed analysis of location inconsistencies
- **Financial Anomaly Reports**: Analysis of payment/order discrepancies
- **Product and Review Analysis Reports**: Quality analysis of products and customer reviews
- **Standardization Reports**: Metrics on data improvement
- **Quality Assessment Reports**: Comprehensive data quality overview

## ⚠️ Important Note About Data Files

**Raw data files are not included in this repository** due to size limitations and Git best practices. 
To run the analysis scripts, you need to:

1. Download the Brazilian E-commerce dataset from the official source
2. Place the CSV files in the `data/raw/` directory
3. Follow the file naming convention described in the documentation

See `data/README.md` for detailed instructions on obtaining the raw data.

## 🤝 Contributing

The project follows this workflow:

1. **Data Preparation**: Use scripts in `scripts/core/` to create reference tables
2. **Standardization**: Apply standardization scripts to clean location data
3. **Enrichment**: Add geographic information using reference tables
4. **Analysis**: Perform analysis using notebooks in `notebooks/`
5. **Reporting**: Generate reports in `reports/`

For data files, contributors should:
- Only commit processed data that is reasonably sized (<100MB)
- Never commit raw data files to the repository
- Use the provided scripts to reproduce the analysis pipeline
- Document any new data processing steps in the appropriate README files

## 🔗 Key Relationships

### Geographic Integration
- `customers.customer_zip_code_prefix` → `zip_code_reference.zip_code_prefix`
- `sellers.seller_zip_code_prefix` → `zip_code_reference.zip_code_prefix`
- Enables geographic analysis and visualization

## 📈 Analysis Opportunities

- **Geographic Distribution Analysis**: Using standardized coordinates
- **Location-Based Clustering**: Group customers/sellers by geography
- **Distance Calculations**: Between customers and sellers for logistics analysis
- **Regional Performance**: Compare sales across different regions
- **Delivery Optimization**: Analyze shipping routes using geographic data

## 🤝 Contributing

The project follows this workflow:

1. **Data Preparation**: Use scripts in `scripts/core/` to create reference tables
2. **Standardization**: Apply standardization scripts to clean location data
3. **Enrichment**: Add geographic information using reference tables
4. **Analysis**: Perform analysis using notebooks in `notebooks/`
5. **Reporting**: Generate reports in `reports/`

## 📄 License

This dataset is provided by Olist and is available for research and educational purposes.
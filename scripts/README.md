# Scripts Directory

This directory contains all Python scripts for data processing and analysis organized by functionality:

## Core Scripts (`core/`)
- `create_zip_code_reference.py` - Creates geographic reference table with canonical names and coordinates
- `standardize_customers.py` - Standardizes customer location data using reference table
- `enrich_customers_with_geolocation.py` - Enriches customers with geographic coordinates
- `detect_seller_anomalies.py` - Detects location anomalies in seller data
- `standardize_sellers.py` - Standardizes seller location data
- `enrich_sellers_with_geolocation.py` - Enriches sellers with geographic coordinates
- `detect_clean_financial_anomalies.py` - Detects and cleans financial anomalies in orders/payments
- `analyze_financial_discrepancies.py` - Detailed analysis of financial discrepancies
- `advanced_financial_cleaning.py` - Advanced cleaning based on financial analysis
- `analyze_clean_products_reviews.py` - Analysis of product and review anomalies

## Analysis Scripts (`analysis/`)
- `analyze_data_quality.py` - Comprehensive data quality analysis
- `clean_data.py` - Data cleaning operations

## Utility Scripts (`utils/`)
- `organize_reports.py` - Organizes generated reports
- `test_profiling.py` - Tests data profiling functionality
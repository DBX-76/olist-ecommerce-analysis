# Brazilian E-Commerce Data Analysis Project

## 📊 Project Overview

This project contains the Brazilian E-Commerce Public Dataset by Olist, a comprehensive collection of data about orders, customers, products, and sellers from a Brazilian marketplace.

## 📁 Project Structure

```
Projet/
├── data/
│   ├── raw/                    # Original CSV files (9 datasets)
│   ├── processed/              # Cleaned and processed data
│   └── external/               # External reference data
├── notebooks/                  # Jupyter notebooks for analysis
├── scripts/                    # Python/R scripts for data processing
├── reports/                    # Generated reports and visualizations
├── docs/                       # Additional documentation
├── config/                     # Configuration files
└── README.md                   # This file
```

## 📋 Dataset Description

The dataset contains 9 CSV files with information about 100k orders from 2016 to 2018:

### Core Datasets

1. **olist_orders_dataset.csv** - Orders information
   - Order status, timestamps, customer ID

2. **olist_customers_dataset.csv** - Customer information
   - Customer location, zip code prefix, city, state

3. **olist_order_items_dataset.csv** - Order items
   - Product ID, seller ID, price, freight value

4. **olist_order_payments_dataset.csv** - Payment information
   - Payment type, installments, payment value

5. **olist_order_reviews_dataset.csv** - Customer reviews
   - Review score, comment title, message

### Supporting Datasets

6. **olist_products_dataset.csv** - Product information
   - Category, dimensions, weight, photos

7. **olist_sellers_dataset.csv** - Seller information
   - Seller location, zip code prefix, city, state

8. **olist_geolocation_dataset.csv** - Geolocation data
   - Zip code prefix, latitude, longitude, city, state

9. **product_category_name_translation.csv** - Category translations
   - Portuguese to English category name translations

## 🚀 Getting Started

### Prerequisites
- Python 3.7+ or R
- Jupyter Notebook (optional)
- Required libraries: pandas, numpy, matplotlib, seaborn, ydata-profiling

### Installation

```bash
# Install required packages
pip install -r requirements.txt
```

### Quick Start with ydata_profiling

1. **Test the profiling functionality**:
```bash
python scripts/test_profiling.py
```

2. **Run the full EDA notebook**:
```bash
jupyter notebook notebooks/01_Exploratory_Data_Analysis.ipynb
```

3. **View the generated reports**:
   - Open HTML files in [`reports/`](reports/) directory
   - Example: `reports/test_orders_profile.html`

### Data Loading Example (Python)

```python
import pandas as pd

# Load datasets
orders = pd.read_csv('data/raw/olist_orders_dataset.csv')
customers = pd.read_csv('data/raw/olist_customers_dataset.csv')
order_items = pd.read_csv('data/raw/olist_order_items_dataset.csv')
# ... load other datasets as needed
```

## 📊 Analysis Ideas

- Customer segmentation and behavior analysis
- Sales trend analysis over time
- Product category performance
- Seller performance metrics
- Geographic distribution analysis
- Payment method preferences
- Review sentiment analysis
- Delivery time analysis

## 📝 Data Schema

### Key Relationships
- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `order_items.seller_id` → `sellers.seller_id`
- `order_payments.order_id` → `orders.order_id`
- `order_reviews.order_id` → `orders.order_id`

## 🔧 Next Steps

1. Explore the raw data in `data/raw/`
2. Create data processing scripts in `scripts/`
3. Develop analysis notebooks in `notebooks/`
4. Generate reports in `reports/`
5. Document findings in `docs/`

## 📄 License

This dataset is provided by Olist and is available for research and educational purposes.

## 🤝 Contributing

Feel free to add your analysis, scripts, and documentation to this project structure.
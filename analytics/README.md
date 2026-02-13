# E-commerce Analytics Module

This module provides comprehensive analytics for the Olist e-commerce dataset, focusing on key business metrics and performance indicators.

## 📊 Available Analytics

### 💰 Sales Metrics
- Daily, monthly, and yearly revenue trends
- Year-over-year growth analysis
- Top 10 performing products
- Revenue forecasting

### 👥 Customer Metrics
- New vs returning customer analysis
- Average cart value
- Conversion rate tracking
- RFM (Recency, Frequency, Monetary) analysis

### 📊 Cohort Analysis
- Customer retention by acquisition month
- Lifetime Value (LTV) by cohort
- Cohort-based performance tracking

## 🛠️ Setup

### Prerequisites
- PostgreSQL database with the Olist dataset loaded
- Python 3.7+

### Installation
```bash
pip install -r requirements.txt
```

## 🚀 Usage

### 1. Generate Static Dashboards
```bash
python analytics/generate_dashboards.py
```
This creates interactive HTML dashboards in the `reports/` directory:
- `reports/sales_dashboard.html`
- `reports/customer_dashboard.html`
- `reports/cohort_dashboard.html`

### 2. Run Performance Tests
```bash
python analytics/performance_test.py
```
This analyzes query performance before and after optimization, creating an index strategy for improved performance.

### 3. Interactive Streamlit Dashboard
```bash
streamlit run analytics/streamlit_dashboard.py
```
This launches an interactive web-based dashboard with real-time data from the database.

## 📋 Query Descriptions

### Optimized SQL Queries
The `kpi_queries.sql` file contains optimized queries for all requested KPIs:

1. **Revenue Analysis**: Daily, monthly, and yearly revenue with YoY comparisons
2. **Product Performance**: Top 10 products by revenue and order count
3. **Customer Segmentation**: New vs returning customer analysis
4. **RFM Analysis**: Customer segmentation based on Recency, Frequency, and Monetary value
5. **Cohort Analysis**: Retention rates and LTV by acquisition cohort

### Performance Optimization
- Strategic indexing on key columns
- Efficient JOIN operations
- Proper use of CTEs and window functions
- Query execution time measurement and reporting

## 📊 Dashboard Features

### Sales Dashboard
- Revenue trends over time
- Monthly and yearly performance
- Top-performing product categories
- Growth rate analysis

### Customer Dashboard
- Customer acquisition and retention
- Average cart value tracking
- Conversion rate monitoring
- RFM customer segmentation

### Cohort Dashboard
- Retention matrix visualization
- LTV by customer acquisition cohort
- Cohort performance tracking

## 📈 Key Performance Indicators

### Sales KPIs
- Total revenue (daily/monthly/yearly)
- Revenue growth (MoM/YoY)
- Top performing products
- Average order value

### Customer KPIs
- New vs returning customer ratio
- Customer acquisition cost
- Average cart value
- Conversion rate

### Cohort KPIs
- Customer retention rate
- Customer lifetime value (LTV)
- Cohort performance tracking
- Churn rate analysis

## 🤝 Contributing

1. Create optimized queries in `kpi_queries.sql`
2. Add visualization functions to `generate_dashboards.py`
3. Test performance improvements with `performance_test.py`
4. Update the Streamlit dashboard as needed

## 📄 License

This project is part of the Olist e-commerce analysis project.
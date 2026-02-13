import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

def get_database_url():
    """Return the database connection URL from environment variables"""
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url

    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    if all([db_host, db_port, db_name, db_user, db_password]):
        url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        if db_host != 'localhost':
            url += "?sslmode=require"
        return url

    raise ValueError("Database environment variables are not all defined. Please check your .env file")

def execute_query(query):
    """Execute a SQL query and return the result as a DataFrame"""
    db_url = get_database_url()
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    
    return df

def create_sales_dashboard():
    """Create dashboard for sales KPIs"""
    # Daily Revenue
    daily_revenue_query = """
    SELECT 
        DATE(order_purchase_timestamp) as purchase_date,
        SUM(oi.price) as daily_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    GROUP BY DATE(order_purchase_timestamp)
    ORDER BY purchase_date;
    """
    daily_revenue_df = execute_query(daily_revenue_query)
    
    # Monthly Revenue
    monthly_revenue_query = """
    SELECT 
        DATE_TRUNC('month', order_purchase_timestamp)::date as purchase_month,
        SUM(oi.price) as monthly_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
    ORDER BY purchase_month;
    """
    monthly_revenue_df = execute_query(monthly_revenue_query)
    
    # Yearly Revenue
    yearly_revenue_query = """
    SELECT 
        EXTRACT(YEAR FROM order_purchase_timestamp) as purchase_year,
        SUM(oi.price) as yearly_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    GROUP BY EXTRACT(YEAR FROM order_purchase_timestamp)
    ORDER BY purchase_year;
    """
    yearly_revenue_df = execute_query(yearly_revenue_query)
    
    # Top 10 Products
    top_products_query = """
    SELECT 
        p.product_category_name_english,
        COUNT(*) as total_orders,
        SUM(oi.price) as total_revenue,
        AVG(oi.price) as avg_price
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.product_category_name_english
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
    top_products_df = execute_query(top_products_query)
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Daily Revenue Trend', 'Monthly Revenue', 'Top 10 Product Categories', 'Revenue by Year'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Daily Revenue
    fig.add_trace(
        go.Scatter(x=daily_revenue_df['purchase_date'], y=daily_revenue_df['daily_revenue'],
                   mode='lines', name='Daily Revenue', line=dict(color='#1f77b4')),
        row=1, col=1
    )
    
    # Monthly Revenue
    fig.add_trace(
        go.Bar(x=monthly_revenue_df['purchase_month'], y=monthly_revenue_df['monthly_revenue'],
               name='Monthly Revenue', marker_color='#ff7f0e'),
        row=1, col=2
    )
    
    # Top Products
    fig.add_trace(
        go.Bar(x=top_products_df['product_category_name_english'], y=top_products_df['total_revenue'],
               name='Top Products Revenue', marker_color='#2ca02c'),
        row=2, col=1
    )
    
    # Yearly Revenue
    fig.add_trace(
        go.Scatter(x=yearly_revenue_df['purchase_year'].astype(str), y=yearly_revenue_df['yearly_revenue'],
                   mode='markers+lines', name='Yearly Revenue', marker=dict(size=10, color='#d62728')),
        row=2, col=2
    )
    
    fig.update_layout(height=800, showlegend=False, title_text="Sales Dashboard")
    
    return fig

def create_customer_dashboard():
    """Create dashboard for customer KPIs"""
    # Customer Type Analysis
    customer_type_query = """
    WITH customer_first_order AS (
        SELECT 
            customer_id,
            MIN(order_purchase_timestamp) as first_order_date,
            COUNT(*) as total_orders
        FROM orders
        WHERE order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY customer_id
    )
    SELECT 
        CASE 
            WHEN cfo.total_orders = 1 THEN 'New Customer'
            ELSE 'Returning Customer'
        END as customer_type,
        COUNT(*) as customer_count
    FROM customer_first_order cfo
    GROUP BY customer_type;
    """
    customer_type_df = execute_query(customer_type_query)
    
    # Average Cart Value
    avg_cart_query = """
    SELECT 
        AVG(order_total) as avg_cart_value
    FROM (
        SELECT 
            o.order_id,
            SUM(oi.price) as order_total
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY o.order_id
    ) order_totals;
    """
    avg_cart_df = execute_query(avg_cart_query)
    
    # Conversion Rate
    conversion_rate_query = """
    SELECT 
        COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_percent
    FROM orders;
    """
    conversion_rate_df = execute_query(conversion_rate_query)
    
    # RFM Analysis Sample
    rfm_query = """
    WITH rfm_data AS (
        SELECT 
            o.customer_id,
            MAX(o.order_purchase_timestamp) as last_order_date,
            COUNT(o.order_id) as frequency,
            SUM(oi.price) as monetary
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY o.customer_id
    ),
    rfm_scores AS (
        SELECT 
            customer_id,
            (CURRENT_DATE - last_order_date::date) as recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY (CURRENT_DATE - last_order_date::date) DESC) as r_score,
            NTILE(5) OVER (ORDER BY frequency) as f_score,
            NTILE(5) OVER (ORDER BY monetary) as m_score
        FROM rfm_data
    )
    SELECT 
        r_score,
        f_score,
        COUNT(*) as customer_count
    FROM rfm_scores
    GROUP BY r_score, f_score
    ORDER BY r_score, f_score;
    """
    rfm_df = execute_query(rfm_query)
    
    # Create subplots - using only compatible chart types
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Customer Type Distribution', 'Average Cart Value', 'Conversion Rate', 'RFM Analysis (RxF)'),
        specs=[[{"type": "domain"}, {"type": "xy"}],  # Pie chart for first subplot
               [{"type": "xy"}, {"type": "xy"}]]
    )
    
    # Customer Type Distribution
    fig.add_trace(
        go.Pie(labels=customer_type_df['customer_type'], values=customer_type_df['customer_count'],
               name="Customer Types"),
        row=1, col=1
    )
    
    # Average Cart Value - Simple bar chart
    fig.add_trace(
        go.Bar(x=['Avg Cart Value'], y=[avg_cart_df['avg_cart_value'].iloc[0]],
               name='Avg Cart Value', marker_color='#ff7f0e'),
        row=1, col=2
    )
    
    # Conversion Rate - Simple bar chart
    fig.add_trace(
        go.Bar(x=['Conversion Rate (%)'], y=[conversion_rate_df['conversion_rate_percent'].iloc[0]],
               name='Conversion Rate', marker_color='#2ca02c'),
        row=2, col=1
    )
    
    # RFM Analysis
    fig.add_trace(
        go.Heatmap(
            x=rfm_df['f_score'],
            y=rfm_df['r_score'],
            z=rfm_df['customer_count'],
            colorscale='Viridis',
            name='RFM Heatmap'
        ),
        row=2, col=2
    )
    
    fig.update_layout(height=800, showlegend=False, title_text="Customer Dashboard")
    
    return fig

def create_cohort_dashboard():
    """Create dashboard for cohort analysis"""
    # Cohort Retention
    cohort_query = """
    WITH cohort_analysis AS (
        SELECT 
            o.customer_id,
            DATE_TRUNC('month', (
                SELECT MIN(order_purchase_timestamp) 
                FROM orders o2 
                WHERE o2.customer_id = o.customer_id
            )) as cohort_month,
            DATE_TRUNC('month', o.order_purchase_timestamp) as order_month
        FROM orders o
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    ),
    cohort_table AS (
        SELECT 
            cohort_month,
            order_month,
            (DATE_PART('year', order_month) - DATE_PART('year', cohort_month)) * 12 +
            (DATE_PART('month', order_month) - DATE_PART('month', cohort_month)) as period_number,
            COUNT(DISTINCT customer_id) as customer_count
        FROM cohort_analysis
        GROUP BY cohort_month, order_month
    )
    SELECT 
        cohort_month,
        period_number,
        customer_count,
        FIRST_VALUE(customer_count) OVER (PARTITION BY cohort_month) as cohort_size,
        ROUND(customer_count * 100.0 / FIRST_VALUE(customer_count) OVER (PARTITION BY cohort_month), 2) as retention_rate
    FROM cohort_table
    ORDER BY cohort_month, period_number;
    """
    cohort_df = execute_query(cohort_query)
    
    # LTV by Cohort
    ltv_query = """
    WITH cohort_ltv AS (
        SELECT 
            DATE_TRUNC('month', (
                SELECT MIN(order_purchase_timestamp) 
                FROM orders o2 
                WHERE o2.customer_id = o.customer_id
            )) as cohort_month,
            o.customer_id,
            SUM(oi.price) as customer_ltv
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY DATE_TRUNC('month', (
                SELECT MIN(order_purchase_timestamp) 
                FROM orders o2 
                WHERE o2.customer_id = o.customer_id
            )), o.customer_id
    )
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size,
        AVG(customer_ltv) as avg_ltv,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY customer_ltv) as median_ltv,
        SUM(customer_ltv) as total_cohort_ltv
    FROM cohort_ltv
    GROUP BY cohort_month
    ORDER BY cohort_month;
    """
    ltv_df = execute_query(ltv_query)
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Cohort Retention Matrix', 'LTV by Cohort'),
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )
    
    # Cohort Retention Heatmap
    pivot_df = cohort_df.pivot(index='cohort_month', columns='period_number', values='retention_rate')
    fig.add_trace(
        go.Heatmap(
            x=pivot_df.columns,
            y=pivot_df.index,
            z=pivot_df.values,
            colorscale='RdYlGn',
            text=pivot_df.values,
            texttemplate="%{text:.1f}%",
            textfont={"size": 10},
            name='Retention Rate'
        ),
        row=1, col=1
    )
    
    # LTV by Cohort
    fig.add_trace(
        go.Bar(x=ltv_df['cohort_month'], y=ltv_df['avg_ltv'],
               name='Average LTV', marker_color='#9467bd'),
        row=2, col=1
    )
    
    fig.update_layout(height=800, showlegend=False, title_text="Cohort Analysis Dashboard")
    
    return fig

def main():
    """Main function to generate all dashboards"""
    print("Generating e-commerce analytics dashboards...")
    
    # Create sales dashboard
    sales_fig = create_sales_dashboard()
    sales_fig.write_html("reports/sales_dashboard.html")
    print("Sales dashboard saved to reports/sales_dashboard.html")
    
    # Create customer dashboard
    customer_fig = create_customer_dashboard()
    customer_fig.write_html("reports/customer_dashboard.html")
    print("Customer dashboard saved to reports/customer_dashboard.html")
    
    # Create cohort dashboard
    cohort_fig = create_cohort_dashboard()
    cohort_fig.write_html("reports/cohort_dashboard.html")
    print("Cohort dashboard saved to reports/cohort_dashboard.html")
    
    print("\nAll dashboards generated successfully!")
    print("\nDashboard files created:")
    print("- reports/sales_dashboard.html")
    print("- reports/customer_dashboard.html") 
    print("- reports/cohort_dashboard.html")

if __name__ == "__main__":
    main()
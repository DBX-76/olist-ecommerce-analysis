import streamlit as st
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

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def execute_query(query):
    """Execute a SQL query and return the result as a DataFrame"""
    db_url = get_database_url()
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    
    return df

def main():
    st.set_page_config(page_title="E-commerce Analytics Dashboard", layout="wide")
    
    # Custom CSS for styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem;
    }
    .section-title {
        font-size: 1.5rem;
        color: #444;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<h1 class="main-header">📊 E-commerce Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a section:", 
                                ["Overview", "Sales Analytics", "Customer Analytics", "Cohort Analysis"])
    
    # Key metrics in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Key Metrics")
    
    # Total Revenue
    revenue_query = """
    SELECT SUM(oi.price) as total_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'approved');
    """
    revenue_df = execute_query(revenue_query)
    total_revenue = revenue_df['total_revenue'].iloc[0] if not revenue_df.empty and revenue_df['total_revenue'].iloc[0] else 0
    
    # Total Orders
    orders_query = """
    SELECT COUNT(DISTINCT o.order_id) as total_orders
    FROM orders o
    WHERE o.order_status IN ('delivered', 'shipped', 'approved');
    """
    orders_df = execute_query(orders_query)
    total_orders = orders_df['total_orders'].iloc[0] if not orders_df.empty else 0
    
    # Total Customers
    customers_query = """
    SELECT COUNT(DISTINCT customer_id) as total_customers
    FROM orders
    WHERE order_status IN ('delivered', 'shipped', 'approved');
    """
    customers_df = execute_query(customers_query)
    total_customers = customers_df['total_customers'].iloc[0] if not customers_df.empty else 0
    
    # Conversion Rate
    conversion_query = """
    SELECT 
        COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_percent
    FROM orders;
    """
    conversion_df = execute_query(conversion_query)
    conversion_rate = conversion_df['conversion_rate_percent'].iloc[0] if not conversion_df.empty else 0
    
    st.sidebar.metric("Total Revenue", f"${total_revenue:,.2f}")
    st.sidebar.metric("Total Orders", f"{total_orders:,}")
    st.sidebar.metric("Total Customers", f"{total_customers:,}")
    st.sidebar.metric("Conversion Rate", f"{conversion_rate:.2f}%")
    
    if page == "Overview":
        st.header("📈 Business Overview")
        
        col1, col2 = st.columns(2)
        with col1:
            # Monthly Revenue Trend
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
            
            fig = px.line(monthly_revenue_df, x='purchase_month', y='monthly_revenue',
                         title='Monthly Revenue Trend')
            fig.update_traces(line=dict(width=3, color='#1f77b4'))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Customer Type Distribution
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
            
            fig = px.pie(customer_type_df, values='customer_count', names='customer_type',
                        title='Customer Type Distribution')
            st.plotly_chart(fig, use_container_width=True)
        
        # Top Product Categories
        st.markdown('<div class="section-title">Top Product Categories</div>', unsafe_allow_html=True)
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
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(top_products_df, x='product_category_name_english', y='total_revenue',
                        title='Top 10 Product Categories by Revenue')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(top_products_df, x='product_category_name_english', y='total_orders',
                        title='Top 10 Product Categories by Order Count')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Sales Analytics":
        st.header("💰 Sales Analytics")
        
        # Revenue by Time Period
        st.subheader("Revenue Trends")
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily Revenue
            daily_revenue_query = """
            SELECT 
                DATE(order_purchase_timestamp) as purchase_date,
                SUM(oi.price) as daily_revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_status IN ('delivered', 'shipped', 'approved')
            GROUP BY DATE(order_purchase_timestamp)
            ORDER BY purchase_date
            LIMIT 30;  -- Last 30 days
            """
            daily_revenue_df = execute_query(daily_revenue_query)
            
            fig = px.line(daily_revenue_df, x='purchase_date', y='daily_revenue',
                         title='Daily Revenue (Last 30 Days)')
            fig.update_traces(line=dict(width=2, color='#ff7f0e'))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
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
            
            fig = px.bar(monthly_revenue_df, x='purchase_month', y='monthly_revenue',
                        title='Monthly Revenue')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Year-over-Year Comparison
        st.subheader("Year-over-Year Growth")
        yoy_query = """
        WITH revenue_by_year AS (
            SELECT 
                EXTRACT(YEAR FROM order_purchase_timestamp) as year,
                EXTRACT(MONTH FROM order_purchase_timestamp) as month,
                SUM(oi.price) as monthly_revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_status IN ('delivered', 'shipped', 'approved')
            GROUP BY EXTRACT(YEAR FROM order_purchase_timestamp), EXTRACT(MONTH FROM order_purchase_timestamp)
        )
        SELECT 
            year,
            month,
            monthly_revenue,
            LAG(monthly_revenue, 12) OVER (ORDER BY year, month) as prev_year_revenue,
            CASE 
                WHEN LAG(monthly_revenue, 12) OVER (ORDER BY year, month) IS NOT NULL THEN
                    ROUND(((monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY year, month)) / 
                           LAG(monthly_revenue, 12) OVER (ORDER BY year, month) * 100), 2)
                ELSE NULL 
            END as yoy_growth_percent
        FROM revenue_by_year
        ORDER BY year, month;
        """
        yoy_df = execute_query(yoy_query)
        
        fig = px.line(yoy_df, x=yoy_df['year'].astype(str) + '-' + yoy_df['month'].astype(str), 
                     y='yoy_growth_percent', title='Year-over-Year Growth Rate (%)')
        fig.update_traces(line=dict(width=3, color='#2ca02c'))
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Top Products Section
        st.subheader("Top Performing Products")
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
        LIMIT 15;
        """
        top_products_df = execute_query(top_products_query)
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(top_products_df, x='product_category_name_english', y='total_revenue',
                        title='Top 15 Product Categories by Revenue')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(top_products_df, x='total_orders', y='avg_price', 
                            size='total_revenue', hover_data=['product_category_name_english'],
                            title='Product Performance: Orders vs Avg Price')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Customer Analytics":
        st.header("👥 Customer Analytics")
        
        # Customer Segmentation
        st.subheader("Customer Segmentation")
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
            COUNT(*) as customer_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_first_order), 2) as percentage
        FROM customer_first_order cfo
        GROUP BY customer_type;
        """
        customer_type_df = execute_query(customer_type_query)
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(customer_type_df, values='customer_count', names='customer_type',
                        title='New vs Returning Customers')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
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
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=avg_cart_df['avg_cart_value'].iloc[0],
                title={'text': "Average Cart Value"},
                gauge={
                    'axis': {'range': [None, max(avg_cart_df['avg_cart_value'].iloc[0] * 2, 100)]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, avg_cart_df['avg_cart_value'].iloc[0]], 'color': "lightgray"},
                        {'range': [avg_cart_df['avg_cart_value'].iloc[0], max(avg_cart_df['avg_cart_value'].iloc[0] * 2, 100)], 'color': "gray"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': avg_cart_df['avg_cart_value'].iloc[0]
                    }
                }
            ))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Conversion Rate
        st.subheader("Conversion Metrics")
        col1, col2 = st.columns(2)
        
        with col1:
            conversion_rate_query = """
            SELECT 
                COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_percent
            FROM orders;
            """
            conversion_df = execute_query(conversion_rate_query)
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=conversion_df['conversion_rate_percent'].iloc[0],
                title={'text': "Overall Conversion Rate (%)"},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 20], 'color': "lightcoral"},
                        {'range': [20, 50], 'color': "khaki"},
                        {'range': [50, 100], 'color': "lightgreen"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': conversion_df['conversion_rate_percent'].iloc[0]
                    }
                }
            ))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # RFM Analysis
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
            
            fig = px.density_heatmap(rfm_df, x='f_score', y='r_score', z='customer_count',
                                    title='RFM Analysis: Recency vs Frequency',
                                    color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Cohort Analysis":
        st.header("📊 Cohort Analysis")
        
        # Cohort Retention Matrix
        st.subheader("Cohort Retention Matrix")
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
        
        # Pivot the data for heatmap
        pivot_df = cohort_df.pivot(index='cohort_month', columns='period_number', values='retention_rate')
        
        fig = px.imshow(pivot_df.values,
                        labels=dict(x="Period", y="Cohort", color="Retention Rate (%)"),
                        x=[str(col) for col in pivot_df.columns],
                        y=[str(date) for date in pivot_df.index],
                        color_continuous_scale='RdYlGn',
                        title='Cohort Retention Matrix')
        fig.update_xaxes(side="top")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # LTV by Cohort
        st.subheader("Lifetime Value by Cohort")
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
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(ltv_df, x='cohort_month', y='avg_ltv',
                        title='Average LTV by Cohort')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(ltv_df, x='cohort_month', y='cohort_size',
                         title='Cohort Size Over Time')
            fig.update_traces(line=dict(width=3, color='#d62728'))
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("*E-commerce Analytics Dashboard - Updated in real-time from PostgreSQL database*")

if __name__ == "__main__":
    main()
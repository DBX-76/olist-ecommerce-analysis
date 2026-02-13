import pandas as pd
import time
from sqlalchemy import create_engine, text
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

def execute_and_time_query(query, query_name, engine):
    """Execute a query and return execution time"""
    start_time = time.time()
    
    with engine.connect() as conn:
        result = pd.read_sql_query(query, conn)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"{query_name}: {execution_time:.4f} seconds")
    return execution_time, len(result)

def create_indexes(engine):
    """Create indexes to optimize query performance"""
    print("Creating indexes for optimized queries...")
    
    with engine.connect() as conn:
        # Index on orders table
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_status_timestamp ON orders(order_status, order_purchase_timestamp);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);"))
        
        # Index on order_items table
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_order_items_price ON order_items(price);"))
        
        # Index on products table
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_products_category ON products(product_category_name, product_category_name_english);"))
        
        # Index on customers table
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_customers_id ON customers(customer_id);"))
        
        conn.commit()
    
    print("Indexes created successfully!")

def run_performance_tests():
    """Run performance tests on queries before and after optimization"""
    db_url = get_database_url()
    engine = create_engine(db_url)
    
    print("Testing query performance...\n")
    
    # Define queries to test
    queries = {
        "Daily Revenue Query": """
        SELECT 
            DATE(order_purchase_timestamp) as purchase_date,
            SUM(oi.price) as daily_revenue
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY DATE(order_purchase_timestamp)
        ORDER BY purchase_date;
        """,
        
        "Monthly Revenue Query": """
        SELECT 
            DATE_TRUNC('month', order_purchase_timestamp)::date as purchase_month,
            SUM(oi.price) as monthly_revenue
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
        GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
        ORDER BY purchase_month;
        """,
        
        "Top 10 Products Query": """
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
        """,
        
        "Customer Type Analysis Query": """
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
        """,
        
        "RFM Analysis Query": """
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
            customer_id,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            CONCAT(r_score, f_score, m_score) as rfm_segment
        FROM rfm_scores
        ORDER BY monetary DESC
        LIMIT 20;
        """
    }
    
    print("Performance test before optimization:")
    print("-" * 40)
    
    # Test queries before optimization
    before_times = {}
    for query_name, query in queries.items():
        exec_time, result_rows = execute_and_time_query(query, query_name, engine)
        before_times[query_name] = exec_time
    
    print(f"\nCreating indexes to optimize queries...")
    create_indexes(engine)
    
    print("\nPerformance test after optimization:")
    print("-" * 40)
    
    # Test queries after optimization
    after_times = {}
    for query_name, query in queries.items():
        exec_time, result_rows = execute_and_time_query(query, query_name, engine)
        after_times[query_name] = exec_time
    
    print("\nPerformance Improvement Summary:")
    print("-" * 40)
    print(f"{'Query':<30} {'Before (s)':<12} {'After (s)':<12} {'Improvement %':<15}")
    print("-" * 70)
    
    for query_name in queries.keys():
        before = before_times[query_name]
        after = after_times[query_name]
        improvement = ((before - after) / before) * 100 if before > 0 else 0
        print(f"{query_name:<30} {before:<12.4f} {after:<12.4f} {improvement:<15.2f}%")
    
    # Calculate overall improvement
    total_before = sum(before_times.values())
    total_after = sum(after_times.values())
    overall_improvement = ((total_before - total_after) / total_before) * 100 if total_before > 0 else 0
    
    print("-" * 70)
    print(f"{'Overall':<30} {total_before:<12.4f} {total_after:<12.4f} {overall_improvement:<15.2f}%")
    
    # Save results to a report
    with open("reports/performance_report.txt", "w") as f:
        f.write("Query Performance Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("Performance Before Optimization:\n")
        for query_name, exec_time in before_times.items():
            f.write(f"- {query_name}: {exec_time:.4f}s\n")
        
        f.write(f"\nPerformance After Optimization:\n")
        for query_name, exec_time in after_times.items():
            f.write(f"- {query_name}: {exec_time:.4f}s\n")
        
        f.write(f"\nPerformance Improvement:\n")
        f.write(f"{'Query':<30} {'Before (s)':<12} {'After (s)':<12} {'Improvement %':<15}\n")
        f.write("-" * 70 + "\n")
        for query_name in queries.keys():
            before = before_times[query_name]
            after = after_times[query_name]
            improvement = ((before - after) / before) * 100 if before > 0 else 0
            f.write(f"{query_name:<30} {before:<12.4f} {after:<12.4f} {improvement:<15.2f}%\n")
        
        f.write("-" * 70 + "\n")
        f.write(f"{'Overall':<30} {total_before:<12.4f} {total_after:<12.4f} {overall_improvement:<15.2f}%\n")
    
    print(f"\nDetailed performance report saved to reports/performance_report.txt")

if __name__ == "__main__":
    run_performance_tests()
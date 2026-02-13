-- Optimized SQL Queries for E-commerce Analytics KPIs
-- These queries focus on performance and provide the requested business metrics

-- 💰 VENTES

-- 1. Chiffre d'affaires (jour, mois, année)
-- Daily Revenue
SELECT 
    DATE(order_purchase_timestamp) as purchase_date,
    SUM(oi.price) as daily_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status IN ('delivered', 'shipped', 'approved')
GROUP BY DATE(order_purchase_timestamp)
ORDER BY purchase_date;

-- Monthly Revenue
SELECT 
    DATE_TRUNC('month', order_purchase_timestamp)::date as purchase_month,
    SUM(oi.price) as monthly_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status IN ('delivered', 'shipped', 'approved')
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY purchase_month;

-- Yearly Revenue
SELECT 
    EXTRACT(YEAR FROM order_purchase_timestamp) as purchase_year,
    SUM(oi.price) as yearly_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status IN ('delivered', 'shipped', 'approved')
GROUP BY EXTRACT(YEAR FROM order_purchase_timestamp)
ORDER BY purchase_year;

-- 2. Évolution CA vs N-1 (Year-over-Year comparison)
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

-- 3. Top 10 produits
SELECT 
    p.product_id,
    p.product_category_name,
    p.product_category_name_english,
    COUNT(*) as total_orders,
    SUM(oi.price) as total_revenue,
    AVG(oi.price) as avg_price,
    SUM(oi.price)/COUNT(*) as revenue_per_order
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_category_name, p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;

-- 👥 CLIENTS

-- 4. Nouveaux clients vs récurrents
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

-- 5. Panier moyen
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

-- 6. Taux de conversion
-- Assuming all orders represent attempts and delivered orders represent conversions
SELECT 
    COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_percent
FROM orders;

-- 7. Analyse RFM (Recency, Frequency, Monetary)
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
        -- Recency: Days since last order (lower is better)
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

-- 📊 COHORTES

-- 8. Rétention par mois de première commande
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

-- 9. LTV (Lifetime Value) par cohorte
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
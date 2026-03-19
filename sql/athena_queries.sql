-- ============================================================================
-- Athena SQL Queries for AWS Data Lake Schema Evolution Framework
-- ============================================================================

-- ============================================================================
-- 1. CREATE EXTERNAL TABLES
-- ============================================================================

-- Create external table for clean orders data
CREATE EXTERNAL TABLE IF NOT EXISTS data_lake_db.clean_orders (
    order_id STRING,
    customer_id STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_date DATE,
    status STRING,
    region STRING,
    shipping_method STRING
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT
)
STORED AS PARQUET
LOCATION 's3://data-lake-bucket/clean/orders/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.day.type'='integer',
    'projection.day.range'='1,31',
    'storage.location.template'='s3://data-lake-bucket/clean/orders/year=${year}/month=${month}/day=${day}'
);

-- Create external table for quarantined orders
CREATE EXTERNAL TABLE IF NOT EXISTS data_lake_db.quarantine_orders (
    order_id STRING,
    customer_id STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_date STRING,
    status STRING,
    region STRING,
    quarantine_timestamp TIMESTAMP
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT
)
STORED AS PARQUET
LOCATION 's3://data-lake-bucket/quarantine/orders_invalid/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- ============================================================================
-- 2. PARTITION MANAGEMENT
-- ============================================================================

-- Repair partitions (discover new partitions)
MSCK REPAIR TABLE data_lake_db.clean_orders;
MSCK REPAIR TABLE data_lake_db.quarantine_orders;

-- Add specific partition manually
ALTER TABLE data_lake_db.clean_orders 
ADD IF NOT EXISTS PARTITION (year=2024, month=1, day=15)
LOCATION 's3://data-lake-bucket/clean/orders/year=2024/month=1/day=15/';

-- Drop old partition
ALTER TABLE data_lake_db.clean_orders 
DROP IF EXISTS PARTITION (year=2023, month=1, day=1);

-- ============================================================================
-- 3. DATA QUALITY QUERIES
-- ============================================================================

-- Count total records in clean layer
SELECT COUNT(*) AS total_clean_records
FROM data_lake_db.clean_orders;

-- Count records by date
SELECT 
    year,
    month,
    day,
    COUNT(*) AS record_count
FROM data_lake_db.clean_orders
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC;

-- Count quarantined records
SELECT COUNT(*) AS total_quarantined_records
FROM data_lake_db.quarantine_orders;

-- Data quality summary
SELECT 
    'Clean Records' AS category,
    COUNT(*) AS count
FROM data_lake_db.clean_orders
UNION ALL
SELECT 
    'Quarantined Records' AS category,
    COUNT(*) AS count
FROM data_lake_db.quarantine_orders;

-- Check for NULL values in critical columns
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date
FROM data_lake_db.clean_orders;

-- Check for duplicate order IDs
SELECT 
    order_id,
    COUNT(*) AS occurrence_count
FROM data_lake_db.clean_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- ============================================================================
-- 4. BUSINESS ANALYTICS QUERIES
-- ============================================================================

-- Total revenue by day
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_items,
    ROUND(SUM(price * quantity), 2) AS total_revenue
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date DESC;

-- Revenue by region
SELECT 
    region,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(price * quantity), 2) AS total_revenue,
    ROUND(AVG(price * quantity), 2) AS avg_order_value
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY region
ORDER BY total_revenue DESC;

-- Top 10 products by revenue
SELECT 
    product_name,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_quantity_sold,
    ROUND(SUM(price * quantity), 2) AS total_revenue
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Order status distribution
SELECT 
    status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM data_lake_db.clean_orders
GROUP BY status
ORDER BY order_count DESC;

-- Daily order trends (last 30 days)
SELECT 
    order_date,
    COUNT(*) AS order_count,
    ROUND(SUM(price * quantity), 2) AS daily_revenue,
    ROUND(AVG(price * quantity), 2) AS avg_order_value
FROM data_lake_db.clean_orders
WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY order_date
ORDER BY order_date DESC;

-- Customer order frequency
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    ROUND(SUM(price * quantity), 2) AS total_spent,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM data_lake_db.clean_orders
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY order_count DESC
LIMIT 20;

-- Revenue by shipping method (if column exists)
SELECT 
    COALESCE(shipping_method, 'Not Specified') AS shipping_method,
    COUNT(*) AS order_count,
    ROUND(SUM(price * quantity), 2) AS total_revenue
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY shipping_method
ORDER BY total_revenue DESC;

-- ============================================================================
-- 5. SCHEMA EVOLUTION QUERIES
-- ============================================================================

-- Check table schema
DESCRIBE data_lake_db.clean_orders;

-- Show table properties
SHOW TBLPROPERTIES data_lake_db.clean_orders;

-- Get column statistics
SHOW COLUMNS FROM data_lake_db.clean_orders;

-- Check if new columns exist
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN shipping_method IS NOT NULL THEN 1 ELSE 0 END) AS records_with_shipping_method,
    ROUND(SUM(CASE WHEN shipping_method IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS percentage_populated
FROM data_lake_db.clean_orders;

-- ============================================================================
-- 6. TIME-BASED ANALYTICS
-- ============================================================================

-- Monthly revenue trend
SELECT 
    year,
    month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price * quantity), 2) AS monthly_revenue
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- Week-over-week growth
WITH weekly_stats AS (
    SELECT 
        DATE_TRUNC('week', order_date) AS week_start,
        COUNT(*) AS order_count,
        SUM(price * quantity) AS revenue
    FROM data_lake_db.clean_orders
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('week', order_date)
)
SELECT 
    week_start,
    order_count,
    ROUND(revenue, 2) AS revenue,
    LAG(revenue) OVER (ORDER BY week_start) AS prev_week_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY week_start)) / LAG(revenue) OVER (ORDER BY week_start) * 100, 2) AS growth_percentage
FROM weekly_stats
ORDER BY week_start DESC;

-- ============================================================================
-- 7. QUARANTINE ANALYSIS
-- ============================================================================

-- Analyze quarantined records by date
SELECT 
    CAST(quarantine_timestamp AS DATE) AS quarantine_date,
    COUNT(*) AS quarantined_count
FROM data_lake_db.quarantine_orders
GROUP BY CAST(quarantine_timestamp AS DATE)
ORDER BY quarantine_date DESC;

-- Identify common issues in quarantined data
SELECT 
    CASE 
        WHEN order_id IS NULL THEN 'Missing Order ID'
        WHEN customer_id IS NULL THEN 'Missing Customer ID'
        WHEN TRY_CAST(price AS DECIMAL) < 0 THEN 'Negative Price'
        WHEN TRY_CAST(quantity AS INT) <= 0 THEN 'Invalid Quantity'
        ELSE 'Other Issue'
    END AS issue_type,
    COUNT(*) AS occurrence_count
FROM data_lake_db.quarantine_orders
GROUP BY 
    CASE 
        WHEN order_id IS NULL THEN 'Missing Order ID'
        WHEN customer_id IS NULL THEN 'Missing Customer ID'
        WHEN TRY_CAST(price AS DECIMAL) < 0 THEN 'Negative Price'
        WHEN TRY_CAST(quantity AS INT) <= 0 THEN 'Invalid Quantity'
        ELSE 'Other Issue'
    END
ORDER BY occurrence_count DESC;

-- ============================================================================
-- 8. PERFORMANCE OPTIMIZATION QUERIES
-- ============================================================================

-- Analyze table size and partition distribution
SELECT 
    year,
    month,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM data_lake_db.clean_orders
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- Identify partitions to archive or delete
SELECT 
    year,
    month,
    day,
    COUNT(*) AS record_count,
    MAX(order_date) AS latest_order_date,
    DATEDIFF(day, MAX(order_date), CURRENT_DATE) AS days_old
FROM data_lake_db.clean_orders
GROUP BY year, month, day
HAVING DATEDIFF(day, MAX(order_date), CURRENT_DATE) > 365
ORDER BY days_old DESC;

-- ============================================================================
-- 9. ADVANCED ANALYTICS
-- ============================================================================

-- Customer segmentation by purchase behavior
WITH customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(*) AS order_count,
        SUM(price * quantity) AS total_spent,
        AVG(price * quantity) AS avg_order_value,
        MAX(order_date) AS last_order_date,
        DATEDIFF(day, MAX(order_date), CURRENT_DATE) AS days_since_last_order
    FROM data_lake_db.clean_orders
    WHERE status = 'completed'
    GROUP BY customer_id
)
SELECT 
    CASE 
        WHEN order_count >= 10 AND total_spent >= 1000 THEN 'VIP'
        WHEN order_count >= 5 AND total_spent >= 500 THEN 'Loyal'
        WHEN order_count >= 2 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_lifetime_value,
    ROUND(AVG(order_count), 2) AS avg_orders_per_customer
FROM customer_metrics
GROUP BY 
    CASE 
        WHEN order_count >= 10 AND total_spent >= 1000 THEN 'VIP'
        WHEN order_count >= 5 AND total_spent >= 500 THEN 'Loyal'
        WHEN order_count >= 2 THEN 'Regular'
        ELSE 'New'
    END
ORDER BY avg_lifetime_value DESC;

-- Product affinity analysis
SELECT 
    a.product_name AS product_a,
    b.product_name AS product_b,
    COUNT(*) AS co_occurrence_count
FROM data_lake_db.clean_orders a
JOIN data_lake_db.clean_orders b 
    ON a.customer_id = b.customer_id 
    AND a.product_name < b.product_name
GROUP BY a.product_name, b.product_name
HAVING COUNT(*) >= 2
ORDER BY co_occurrence_count DESC
LIMIT 20;

-- ============================================================================
-- 10. EXPORT QUERIES FOR REPORTING
-- ============================================================================

-- Create a view for dashboard reporting
CREATE OR REPLACE VIEW data_lake_db.daily_sales_summary AS
SELECT 
    order_date,
    region,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_items,
    ROUND(SUM(price * quantity), 2) AS total_revenue,
    ROUND(AVG(price * quantity), 2) AS avg_order_value
FROM data_lake_db.clean_orders
WHERE status = 'completed'
GROUP BY order_date, region;

-- Query the view
SELECT * FROM data_lake_db.daily_sales_summary
ORDER BY order_date DESC, total_revenue DESC
LIMIT 100;

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. Replace 'data-lake-bucket' with your actual S3 bucket name
-- 2. Ensure partitions are discovered using MSCK REPAIR TABLE after data loads
-- 3. Use partition projection for better query performance
-- 4. Consider using CTAS (CREATE TABLE AS SELECT) for materialized views
-- 5. Monitor query costs using AWS Cost Explorer
-- ============================================================================

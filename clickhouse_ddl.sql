CREATE DATABASE IF NOT EXISTS operational_db;

USE operational_db;

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id String,
    seller_zip_code_prefix Int32,
    seller_city String,
    seller_state String
) ENGINE = ReplacingMergeTree()
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix Int32,
    customer_city String,
    customer_state String
) ENGINE = ReplacingMergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dim_reviews (
    review_id String,
    order_id String,
    review_score Int32,
    review_creation_date DateTime,
    review_answer_timestamp DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY (order_id, review_id);

CREATE TABLE IF NOT EXISTS fact_deliveries (
    order_id String,
    customer_id String,
    seller_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date DateTime,
    
    -- Calculated Operational Metrics
    lead_time_days Float32,
    processing_time_days Float32,
    is_delayed UInt8, -- 1 for True, 0 for False
    
    -- Machine Learning Prediction
    predicted_delay_probability Float32
) ENGINE = MergeTree()
ORDER BY (order_purchase_timestamp, order_id);

CREATE DATABASE IF NOT EXISTS operational_db;

USE operational_db;

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id String,
    seller_zip_code_prefix Nullable(Int32),
    seller_city Nullable(String),
    seller_state Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id String,
    customer_unique_id Nullable(String),
    customer_zip_code_prefix Nullable(Int32),
    customer_city Nullable(String),
    customer_state Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dim_reviews (
    review_id String,
    order_id String,
    review_score Nullable(Int32),
    review_creation_date Nullable(DateTime),
    review_answer_timestamp Nullable(DateTime)
) ENGINE = ReplacingMergeTree()
ORDER BY (order_id, review_id);

CREATE TABLE IF NOT EXISTS fact_deliveries (
    order_id String,
    customer_id Nullable(String),
    seller_id Nullable(String),
    order_status Nullable(String),
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    
    -- Calculated Operational Metrics
    lead_time_days Nullable(Float32),
    processing_time_days Nullable(Float32),
    is_delayed Nullable(UInt8), -- 1 for True, 0 for False
    
    -- Machine Learning Prediction
    predicted_delay_probability Nullable(Float32)
) ENGINE = MergeTree()
ORDER BY (order_purchase_timestamp, order_id);

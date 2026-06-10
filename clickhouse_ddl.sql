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
    customer_id String,
    seller_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    lead_time_days Nullable(Float32),
    seller_processing_days Nullable(Float32),
    carrier_transit_days Nullable(Float32),
    sla_breach_days Nullable(Float32),
    is_delayed Nullable(UInt8), 
    freight_value Nullable(Float32),
    purchase_hour Nullable(UInt8),
    route String,
    customer_state String,
    seller_state String,
    primary_category Nullable(String),
    predicted_delay_probability Nullable(Float32)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (customer_state, seller_state, order_purchase_timestamp, order_id, seller_id);

# Metabase Queries & Visualization Strategy

The following SQL queries should be used in Metabase to create the operational dashboard panels.

## 1. Overall Delivery Delay Rate
**Description**: Shows the percentage of late deliveries compared to the total delivered orders.
**Visualization Type**: **Gauge** or **Number** component.

```sql
SELECT 
    sum(is_delayed) / count(order_id) * 100 AS delay_percentage
FROM operational_db.fact_deliveries
WHERE order_status = 'delivered';
```

## 2. Average Delivery Time vs Estimated by Region (Customer State)
**Description**: Compares the actual delivery time against what was estimated, broken down by the customer's state to identify geographical bottlenecks.
**Visualization Type**: **Bar Chart (Grouped)** or **Choropleth Map** (if mapping states to a map of Indonesia/Brazil). 

```sql
SELECT 
    c.customer_state,
    AVG(f.lead_time_days) AS avg_actual_lead_time_days,
    AVG(dateDiff('day', f.order_purchase_timestamp, f.order_estimated_delivery_date)) AS avg_estimated_lead_time_days
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY avg_actual_lead_time_days DESC;
```

## 3. Top 10 Sellers with Highest Operational Bottlenecks
**Description**: Identifies the sellers taking the longest time to hand over products to the carrier (processing time), filtering for sellers with a meaningful order volume (e.g., > 50 orders).
**Visualization Type**: **Horizontal Bar Chart**.

```sql
SELECT 
    f.seller_id,
    s.seller_state,
    COUNT(f.order_id) AS total_orders,
    AVG(f.processing_time_days) AS avg_processing_time_days
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_sellers s ON f.seller_id = s.seller_id
WHERE f.processing_time_days IS NOT NULL
GROUP BY f.seller_id, s.seller_state
HAVING total_orders > 50
ORDER BY avg_processing_time_days DESC
LIMIT 10;
```

## 4. Correlation between Delivery Delays and Negative Customer Reviews
**Description**: Shows how delivery delays impact customer satisfaction by comparing the average review score of delayed orders vs on-time orders.
**Visualization Type**: **Bar Chart** or **Pie Chart/Donut** showing score distribution for delayed vs non-delayed.

```sql
SELECT 
    CASE WHEN f.is_delayed = 1 THEN 'Delayed' ELSE 'On Time' END AS delivery_status,
    AVG(r.review_score) AS avg_review_score,
    COUNT(r.review_id) AS review_count
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_reviews r ON f.order_id = r.order_id
WHERE f.order_status = 'delivered'
GROUP BY delivery_status;
```

## 5. Machine Learning Insights: High Delay Probability Zones (The "Wow" Factor Panel)
**Description**: Utilizes our Random Forest model predictions to show the average probability of an order being delayed based on where the seller is located. This allows the business to proactively manage expectations for sellers in high-risk areas.
**Visualization Type**: **Table** or **Map**.

```sql
SELECT 
    s.seller_state,
    AVG(f.predicted_delay_probability) * 100 AS avg_delay_probability_percent,
    COUNT(f.order_id) AS total_orders
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_sellers s ON f.seller_id = s.seller_id
GROUP BY s.seller_state
ORDER BY avg_delay_probability_percent DESC;
```

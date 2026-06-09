# Metabase Queries & Visualization Strategy

The following SQL queries should be used in Metabase to create the operational dashboard panels. The flow follows our 3-act executive story: The Hook, The Investigation, and The Resolution.

---
## Act 1: The Hook (The Cost of Inefficiency)

### 1. Overall Delivery Delay Rate (SLA Breach)
**Description**: Shows the percentage of late deliveries (Actual Delivery > Estimated Delivery) compared to total delivered orders.
**Visualization Type**: **Gauge** or **Number** component.

```sql
SELECT 
    count(DISTINCT CASE WHEN is_delayed = 1 THEN order_id ELSE NULL END) / count(DISTINCT order_id) * 100 AS delay_percentage
FROM operational_db.fact_deliveries
WHERE order_status = 'delivered';
```

### 2. SLA Breach Trend Over Time
**Description**: Visualizes whether our logistical performance is improving or deteriorating month over month.
**Visualization Type**: **Line Chart**.

```sql
SELECT 
    toStartOfMonth(order_purchase_timestamp) AS purchase_month,
    sum(is_delayed) / count(order_id) * 100 AS delay_percentage
FROM operational_db.fact_deliveries
WHERE order_status = 'delivered'
GROUP BY purchase_month
ORDER BY purchase_month ASC;
```

### 3. The Ripple Effect: Delays vs Customer Reviews
**Description**: Shows how delivery delays directly impact customer satisfaction by comparing average review scores of delayed vs. on-time orders.
**Visualization Type**: **Bar Chart** or **Pie Chart/Donut**.

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

---
## Act 2: The Investigation (Deconstructing the Delay)

### 4. Seller Processing vs Carrier Transit Breakdown
**Description**: Stacks the two major components of lead time over time to see who is actually causing the delays (Sellers packing slow vs Carriers driving slow).
**Visualization Type**: **Stacked Area Chart** or **Stacked Bar Chart**.

```sql
SELECT 
    toStartOfMonth(order_purchase_timestamp) AS purchase_month,
    AVG(seller_processing_days) AS avg_seller_processing,
    AVG(carrier_transit_days) AS avg_carrier_transit
FROM operational_db.fact_deliveries
WHERE order_status = 'delivered' AND seller_processing_days IS NOT NULL AND carrier_transit_days IS NOT NULL
GROUP BY purchase_month
ORDER BY purchase_month ASC;
```

### 5. Delay Rate by Customer State
**Description**: Identifies geographical bottlenecks by comparing the actual delivery time against what was estimated, broken down by customer state.
**Visualization Type**: **Choropleth Map** (if mapping states) or **Bar Chart (Grouped)**.

```sql
SELECT 
    c.customer_state,
    AVG(f.lead_time_days) AS avg_actual_lead_time_days,
    AVG(f.sla_breach_days) AS avg_sla_breach_days,
    count(DISTINCT CASE WHEN f.is_delayed = 1 THEN f.order_id ELSE NULL END) / count(DISTINCT f.order_id) * 100 AS delay_percentage
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
HAVING count(DISTINCT f.order_id) > 100
ORDER BY delay_percentage DESC;
```

### 6. Worst Performing Logistical Routes
**Description**: Identifies specific supply chain routes (Seller State -> Customer State) experiencing the highest delay rates.
**Visualization Type**: **Horizontal Bar Chart**.

```sql
SELECT 
    route,
    count(DISTINCT CASE WHEN is_delayed = 1 THEN order_id ELSE NULL END) / count(DISTINCT order_id) * 100 AS delay_rate_percent,
    AVG(freight_value) AS avg_freight,
    AVG(lead_time_days) AS avg_delivery_days,
    COUNT(DISTINCT order_id) AS order_count
FROM operational_db.fact_deliveries
WHERE route IS NOT NULL AND order_status = 'delivered'
GROUP BY route
HAVING order_count > 50
ORDER BY delay_rate_percent DESC
LIMIT 15;
```

### 7. Purchase Hour vs Delay Rate
**Description**: Analyzes how the time-of-day when an order is placed correlates with operational delays (e.g., late-night orders missing carrier cutoff times).
**Visualization Type**: **Line Chart**.

```sql
SELECT 
    purchase_hour,
    count(DISTINCT CASE WHEN is_delayed = 1 THEN order_id ELSE NULL END) / count(DISTINCT order_id) * 100 AS delay_rate_percent,
    COUNT(DISTINCT order_id) AS total_orders
FROM operational_db.fact_deliveries
WHERE purchase_hour IS NOT NULL AND order_status = 'delivered'
GROUP BY purchase_hour
ORDER BY purchase_hour;
```

---
## Act 3: The Resolution (Actionable Insights)

### 8. Top 10 Bottleneck Sellers
**Description**: Directly lists the worst sellers who are taking the longest time to process orders, enabling operations to enforce SLAs.
**Visualization Type**: **Horizontal Bar Chart** or **Table**.

```sql
SELECT 
    f.seller_id,
    s.seller_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    AVG(f.seller_processing_days) AS avg_processing_time_days
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_sellers s ON f.seller_id = s.seller_id
WHERE f.seller_processing_days IS NOT NULL AND f.order_status = 'delivered'
GROUP BY f.seller_id, s.seller_state
HAVING total_orders > 50
ORDER BY avg_processing_time_days DESC
LIMIT 10;
```

### 9. Freight Efficiency (Finance Perspective)
**Description**: Analyzes whether higher freight values actually result in faster delivery. If routes are slow AND expensive, Finance needs to renegotiate contracts.
**Visualization Type**: **Scatter Plot**.

```sql
SELECT 
    freight_value,
    lead_time_days,
    route
FROM operational_db.fact_deliveries
WHERE order_status = 'delivered' AND freight_value < 500 AND lead_time_days < 60
SAMPLE 10000;
```

### 10. ML Insights: High-Risk Seller Zones
**Description**: Utilizes our ML model predictions to show the average probability of an order being delayed based on seller location. Customer Service can use this proactively.
**Visualization Type**: **Map** or **Heatmap Table**.

```sql
SELECT 
    s.seller_state,
    AVG(f.predicted_delay_probability) * 100 AS avg_delay_probability_percent,
    COUNT(f.order_id) AS total_orders
FROM operational_db.fact_deliveries f
JOIN operational_db.dim_sellers s ON f.seller_id = s.seller_id
WHERE f.predicted_delay_probability IS NOT NULL
GROUP BY s.seller_state
HAVING total_orders > 100
ORDER BY avg_delay_probability_percent DESC;
```

### 11. Delivery Failure Rate (Cancellations/Unavailable)
**Description**: Not all orders make it to the carrier. This tracks the percentage of orders that failed before delivery.
**Visualization Type**: **Bar Chart**.

```sql
SELECT 
    order_status,
    COUNT(order_id) AS status_count
FROM operational_db.fact_deliveries
WHERE order_status IN ('canceled', 'unavailable')
GROUP BY order_status
ORDER BY status_count DESC;
```

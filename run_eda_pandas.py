import pandas as pd
import numpy as np
import os

data_path = "./DustiniaDelixia_Groceria"
orders = pd.read_csv(f"{data_path}/orders.csv")
sellers = pd.read_csv(f"{data_path}/sellers.csv")
customers = pd.read_csv(f"{data_path}/customers.csv")

timestamp_cols = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]
for c in timestamp_cols:
    orders[c] = pd.to_datetime(orders[c])

orders['delivery_time_days'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.days

print("=== Delivery Times Description ===")
print(orders['delivery_time_days'].describe())

print("\n=== Top 5 Customer States ===")
print(customers['customer_state'].value_counts().head(5))

print("\n=== Top 5 Seller States ===")
print(sellers['seller_state'].value_counts().head(5))

print("\n=== Anomalies ===")
anomalies_time = orders[orders['order_delivered_customer_date'] < orders['order_purchase_timestamp']]
print(f"Number of orders delivered before purchase: {len(anomalies_time)}")

anomalies_carrier = orders[orders['order_delivered_customer_date'] < orders['order_delivered_carrier_date']]
print(f"Number of orders delivered before given to carrier: {len(anomalies_carrier)}")

print("\n=== Missing Values ===")
print(orders[timestamp_cols].isnull().sum())

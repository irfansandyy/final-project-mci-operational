import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, when, count, isnan, isnull

spark = SparkSession.builder \
    .appName("DustiniaDelixia_Operational_EDA") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

data_path = "./DustiniaDelixia_Groceria"
orders = spark.read.csv(f"{data_path}/orders.csv", header=True, inferSchema=True)
sellers = spark.read.csv(f"{data_path}/sellers.csv", header=True, inferSchema=True)
customers = spark.read.csv(f"{data_path}/customers.csv", header=True, inferSchema=True)

timestamp_cols = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]
for c in timestamp_cols:
    orders = orders.withColumn(c, to_timestamp(col(c)))

orders = orders.withColumn("delivery_time_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))

print("=== Delivery Times Description ===")
orders.select("delivery_time_days").summary().show()

print("\n=== Top 5 Customer States ===")
customers.groupBy("customer_state").count().orderBy(col("count").desc()).show(5)

print("\n=== Top 5 Seller States ===")
sellers.groupBy("seller_state").count().orderBy(col("count").desc()).show(5)

print("\n=== Anomalies ===")
anomalies_time = orders.filter(col("order_delivered_customer_date") < col("order_purchase_timestamp"))
print(f"Number of orders delivered before purchase: {anomalies_time.count()}")

anomalies_carrier = orders.filter(col("order_delivered_customer_date") < col("order_delivered_carrier_date"))
print(f"Number of orders delivered before given to carrier: {anomalies_carrier.count()}")

print("\n=== Missing Values ===")
missing_exprs = [count(when(isnull(c) | isnan(c), c)).alias(c) for c in timestamp_cols]
orders.select(missing_exprs).show()

spark.stop()

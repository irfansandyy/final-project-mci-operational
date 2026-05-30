import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, when, count, isnan, isnull, avg, hour, concat_ws

def run_deep_eda():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("DustiniaDelixia_Operational_EDA") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    data_path = "./DustiniaDelixia_Groceria"
    
    print("Loading Data (PySpark DataFrames)...")
    orders = spark.read.csv(f"{data_path}/orders.csv", header=True, inferSchema=True)
    order_items = spark.read.csv(f"{data_path}/order_items.csv", header=True, inferSchema=True)
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
    orders = orders.withColumn("is_delayed", (col("order_delivered_customer_date") > col("order_estimated_delivery_date")).cast("int"))
    orders = orders.withColumn("purchase_hour", hour(col("order_purchase_timestamp")))

    print("=== Time-of-Day Delays ===")
    hourly_delays = orders.filter(col("is_delayed").isNotNull()).groupBy("purchase_hour").agg(
        avg("is_delayed").alias("delay_rate"),
        count("order_id").alias("total_orders")
    ).orderBy("purchase_hour")
    hourly_delays.show(24)

    print("=== Route Bottlenecks & Freight Values ===")
    fact = orders.join(order_items, on="order_id", how="left") \
        .join(sellers, on="seller_id", how="left") \
        .join(customers, on="customer_id", how="left")

    fact = fact.withColumn("route", concat_ws(" -> ", col("seller_state"), col("customer_state")))

    route_stats = fact.filter(col("is_delayed").isNotNull()).groupBy("route").agg(
        avg("is_delayed").alias("delay_rate"),
        avg("freight_value").alias("avg_freight"),
        avg("delivery_time_days").alias("avg_delivery_days"),
        count("order_id").alias("order_count")
    ).filter(col("order_count") > 50).orderBy(col("delay_rate").desc())
    
    print("Top 10 Worst Routes by Delay Rate:")
    route_stats.show(10, truncate=False)

    print("EDA Complete. Generating plots is handled in the Jupyter Notebook.")
    spark.stop()

if __name__ == "__main__":
    run_deep_eda()

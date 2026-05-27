import nbformat as nbf

nb = nbf.v4.new_notebook()

text_intro = """# DustiniaDelixia Groceria - Operational Analysis EDA
This notebook performs Exploratory Data Analysis (EDA) on the DustiniaDelixia Groceria dataset to identify delivery bottlenecks, geographic distributions, and data anomalies."""

code_setup = """import os
import zipfile
import gdown
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, when, count, isnan, isnull

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("DustiniaDelixia_Operational_EDA") \\
    .config("spark.driver.memory", "4g") \\
    .getOrCreate()

# Ensure dataset is downloaded and extracted
# The dataset is already extracted in our workspace under DustiniaDelixia_Groceria
data_path = "./DustiniaDelixia_Groceria"
"""

text_load = """## 1. Load Data into Spark DataFrames
We will load the relevant tables for the Operational Analyst persona:
- `orders.csv`: Contains order timestamps (purchase, approved, delivered, etc.)
- `order_items.csv`: Contains item details and seller linkage
- `sellers.csv`: Contains seller geography
- `customers.csv`: Contains customer geography
- `geolocation.csv`: Mapping of zip codes to coordinates (if needed)
"""

code_load = """orders = spark.read.csv(f"{data_path}/orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv(f"{data_path}/order_items.csv", header=True, inferSchema=True)
sellers = spark.read.csv(f"{data_path}/sellers.csv", header=True, inferSchema=True)
customers = spark.read.csv(f"{data_path}/customers.csv", header=True, inferSchema=True)

# Register as temp views for easy SQL if needed
orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")
sellers.createOrReplaceTempView("sellers")
customers.createOrReplaceTempView("customers")
"""

text_timestamps = """## 2. Analyze Order Timestamps & Delivery Times
Let's find the distribution of delivery times.
"""

code_timestamps = """# Convert timestamp columns to proper timestamp type
timestamp_cols = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]
for c in timestamp_cols:
    orders = orders.withColumn(c, to_timestamp(col(c)))

# Calculate delivery time in days
orders = orders.withColumn("delivery_time_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))

# Filter out nulls for visualization
delivery_pd = orders.select("delivery_time_days").filter(col("delivery_time_days").isNotNull()).toPandas()

plt.figure(figsize=(10, 6))
sns.histplot(delivery_pd['delivery_time_days'], bins=50, kde=True)
plt.title("Distribution of Delivery Times (Days)")
plt.xlabel("Days to Deliver")
plt.ylabel("Frequency")
plt.xlim(0, 100) # limiting x axis to ignore extreme outliers for the main distribution
plt.show()

# Display summary statistics
delivery_pd.describe()
"""

text_geo = """## 3. Seller and Customer Geographical Distributions
Let's look at where our customers and sellers are located based on their states.
"""

code_geo = """# Customer distribution by state
cust_dist = customers.groupBy("customer_state").count().orderBy(col("count").desc()).toPandas()

plt.figure(figsize=(12, 6))
sns.barplot(data=cust_dist, x="customer_state", y="count", palette="viridis")
plt.title("Customer Distribution by State")
plt.xlabel("State")
plt.ylabel("Number of Customers")
plt.show()

# Seller distribution by state
sell_dist = sellers.groupBy("seller_state").count().orderBy(col("count").desc()).toPandas()

plt.figure(figsize=(12, 6))
sns.barplot(data=sell_dist, x="seller_state", y="count", palette="magma")
plt.title("Seller Distribution by State")
plt.xlabel("State")
plt.ylabel("Number of Sellers")
plt.show()
"""

text_anomalies = """## 4. Missing Values, Outliers, and Anomalies
Checking for anomalies such as delivered dates occurring before order dates.
"""

code_anomalies = """# 1. Delivered before purchase
anomalies_time = orders.filter(col("order_delivered_customer_date") < col("order_purchase_timestamp"))
print(f"Number of orders delivered before purchase: {anomalies_time.count()}")

# 2. Delivered before carrier processing
anomalies_carrier = orders.filter(col("order_delivered_customer_date") < col("order_delivered_carrier_date"))
print(f"Number of orders delivered before given to carrier: {anomalies_carrier.count()}")

# 3. Missing values check in orders
missing_exprs = [count(when(isnull(c) | isnan(c), c)).alias(c) for c in timestamp_cols]
missing_counts = orders.select(missing_exprs).toPandas()
print("Missing values in timestamp columns:")
print(missing_counts.transpose())
"""

nb['cells'] = [
    nbf.v4.new_markdown_cell(text_intro),
    nbf.v4.new_code_cell(code_setup),
    nbf.v4.new_markdown_cell(text_load),
    nbf.v4.new_code_cell(code_load),
    nbf.v4.new_markdown_cell(text_timestamps),
    nbf.v4.new_code_cell(code_timestamps),
    nbf.v4.new_markdown_cell(text_geo),
    nbf.v4.new_code_cell(code_geo),
    nbf.v4.new_markdown_cell(text_anomalies),
    nbf.v4.new_code_cell(code_anomalies)
]

with open('EDA.ipynb', 'w') as f:
    nbf.write(nb, f)
print("Notebook generated successfully at EDA.ipynb")

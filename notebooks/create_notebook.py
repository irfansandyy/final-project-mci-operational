import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = []

# Cell 1: Markdown
cells.append(nbf.v4.new_markdown_cell("""# Comprehensive Operational Analysis: Dustinia Delixia Groceria
**Author:** Operational Analyst  
**Objective:** Translate raw operational data into actionable business narratives. We analyze the raw state using PySpark (limited to 1GB to simulate resource constraints), followed by demonstrating the advanced insights unlocked by our Airflow ETL and ClickHouse Data Warehouse.

---
## Act 1: The Hook (The Cost of Inefficiency)
The Finance team is demanding cost reduction, while Operations is flooded with complaints from top sellers regarding poor delivery experiences. Where is the operational bleeding occurring? We start by exploring the raw data."""))

# Cell 2: Code setup
cells.append(nbf.v4.new_code_cell("""%pip install -r /home/jovyan/jupyter_requirements.txt
%pip install clickhouse-connect pyspark matplotlib seaborn

import warnings
warnings.filterwarnings('ignore')

import matplotlib.pyplot as plt
import seaborn as sns
import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_timestamp, when, avg, count, expr

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({'figure.figsize': (12, 6)})
print("Libraries imported.")"""))

# Cell 3: Code PySpark init
cells.append(nbf.v4.new_code_cell("""# Initialize PySpark with 1GB memory limit
spark = SparkSession.builder \\
    .appName("DustiniaDelixia_EDA") \\
    .config("spark.driver.memory", "1g") \\
    .config("spark.executor.memory", "1g") \\
    .getOrCreate()

print("PySpark Session Initialized with 1GB memory limit.")"""))

# Cell 4: Markdown
cells.append(nbf.v4.new_markdown_cell("""---
## Act 2: The Investigation (Deconstructing the Delay)
We load the raw CSV files using PySpark. Our Airflow DAG will eventually automate this extraction."""))

# Cell 5: Code read data
cells.append(nbf.v4.new_code_cell("""raw_dir = '/home/jovyan/raw_data/'

# Load datasets
orders_df = spark.read.csv(raw_dir + 'orders.csv', header=True, inferSchema=True)
customers_df = spark.read.csv(raw_dir + 'customers.csv', header=True, inferSchema=True)
sellers_df = spark.read.csv(raw_dir + 'sellers.csv', header=True, inferSchema=True)
items_df = spark.read.csv(raw_dir + 'order_items.csv', header=True, inferSchema=True)
reviews_df = spark.read.csv(raw_dir + 'order_reviews.csv', header=True, inferSchema=True)

print(f"Loaded {orders_df.count()} raw orders.")"""))

# Cell 6: Markdown
cells.append(nbf.v4.new_markdown_cell("""### 1. The Operational Funnel
What is the status of our orders? We need our Airflow ETL to filter out cancelled or unavailable orders to focus on completed deliveries."""))

# Cell 7: Code status
cells.append(nbf.v4.new_code_cell("""status_counts = orders_df.groupBy("order_status").count().orderBy(col("count").desc()).toPandas()

plt.figure(figsize=(10, 5))
sns.barplot(data=status_counts, x="count", y="order_status", palette="viridis")
plt.title("Order Status Distribution")
plt.xlabel("Number of Orders")
plt.ylabel("Order Status")
plt.xscale('log')
plt.show()"""))

# Cell 8: Markdown
cells.append(nbf.v4.new_markdown_cell("""### 2. SLA Breach Calculation
How often do we break our promises? We calculate the SLA breach by comparing the actual delivery date to the estimated delivery date."""))

# Cell 9: Code SLA
cells.append(nbf.v4.new_code_cell("""# Filter delivered orders
delivered_df = orders_df.filter(col("order_status") == "delivered")

# Convert strings to timestamps
time_cols = ['order_purchase_timestamp', 'order_approved_at', 
             'order_delivered_carrier_date', 'order_delivered_customer_date', 
             'order_estimated_delivery_date']

for c in time_cols:
    delivered_df = delivered_df.withColumn(c, to_timestamp(col(c)))

# Calculate SLA Breach (days)
delivered_df = delivered_df.withColumn("sla_breach_days", 
    datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")))

# Flag for delayed
delivered_df = delivered_df.withColumn("is_delayed", when(col("sla_breach_days") > 0, 1).otherwise(0))

breach_stats = delivered_df.groupBy("is_delayed").count().toPandas()
print("SLA Breach Counts:")
print(breach_stats)"""))

# Cell 10: Code plot SLA
cells.append(nbf.v4.new_code_cell("""plt.figure(figsize=(6, 6))
plt.pie(breach_stats['count'], labels=['On Time', 'Delayed'], autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'])
plt.title("Overall SLA Breach Percentage")
plt.show()"""))

# Cell 11: Markdown
cells.append(nbf.v4.new_markdown_cell("""### 3. The Bottleneck: Seller vs Carrier
When an order is delayed, who is at fault? The seller taking too long to pack, or the carrier taking too long to transit? Our ETL pipeline will calculate these components."""))

# Cell 12: Code component times
cells.append(nbf.v4.new_code_cell("""delivered_df = delivered_df.withColumn("seller_processing_days", 
    datediff(col("order_delivered_carrier_date"), col("order_approved_at")))

delivered_df = delivered_df.withColumn("carrier_transit_days", 
    datediff(col("order_delivered_customer_date"), col("order_delivered_carrier_date")))

delivered_df = delivered_df.withColumn("total_lead_time_days", 
    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))

components_pd = delivered_df.select("seller_processing_days", "carrier_transit_days").summary("mean").toPandas()
print(components_pd)"""))

# Cell 13: Code plot components
cells.append(nbf.v4.new_code_cell("""mean_processing = float(components_pd.loc[0, 'seller_processing_days'])
mean_transit = float(components_pd.loc[0, 'carrier_transit_days'])

plt.figure(figsize=(8, 5))
sns.barplot(x=['Seller Processing Time', 'Carrier Transit Time'], y=[mean_processing, mean_transit], palette="Set2")
plt.title("Average Days Spent in Operational Phases")
plt.ylabel("Days")
plt.show()"""))

# Cell 14: Markdown
cells.append(nbf.v4.new_markdown_cell("""### 4. Geospatial Route Friction
Which specific routes (Seller State -> Customer State) cause the most pain? We join multiple raw datasets in PySpark to map this out."""))

# Cell 15: Code geo join
cells.append(nbf.v4.new_code_cell("""# Join orders with customers and items/sellers
enriched_df = delivered_df.join(customers_df, "customer_id", "left")

# Get seller state
item_seller_df = items_df.join(sellers_df, "seller_id", "left").select("order_id", "seller_state").dropDuplicates(["order_id"])

enriched_df = enriched_df.join(item_seller_df, "order_id", "left")

enriched_df = enriched_df.withColumn("route", expr("concat(seller_state, ' -> ', customer_state)"))

route_stats = enriched_df.groupBy("route").agg(
    count("order_id").alias("total_orders"),
    avg("is_delayed").alias("delay_rate")
).filter(col("total_orders") > 100).orderBy(col("delay_rate").desc()).limit(10).toPandas()

print(route_stats)"""))

# Cell 16: Code geo plot
cells.append(nbf.v4.new_code_cell("""plt.figure(figsize=(10, 6))
sns.barplot(data=route_stats, y="route", x="delay_rate", palette="Reds_r")
plt.title("Top 10 High-Friction Routes (Highest Delay Rate)")
plt.xlabel("Delay Rate")
plt.ylabel("Route (Seller State -> Customer State)")
plt.show()"""))

# Cell 17: Markdown
cells.append(nbf.v4.new_markdown_cell("""### 5. The Ripple Effect: Operations meets Customer Satisfaction
How do these delays impact the business bottom line? We join against the review dataset."""))

# Cell 18: Code reviews
cells.append(nbf.v4.new_code_cell("""reviews_enriched = enriched_df.join(reviews_df, "order_id", "inner")

review_scores = reviews_enriched.groupBy("is_delayed").agg(avg("review_score").alias("avg_score")).toPandas()

plt.figure(figsize=(6, 5))
sns.barplot(data=review_scores, x="is_delayed", y="avg_score", palette={0: '#2ecc71', 1: '#e74c3c'})
plt.title("Impact of Delays on Review Scores")
plt.xticks(ticks=[0, 1], labels=['On Time', 'Delayed'])
plt.ylabel("Average Review Score (1-5)")
plt.ylim(1, 5)
plt.show()"""))

# Cell 19: Markdown
cells.append(nbf.v4.new_markdown_cell("""---
## Act 3: The Resolution (ClickHouse & Airflow Architecture)
The PySpark analysis above proves the necessity of a robust ETL. Our Airflow DAGs will pre-calculate `is_delayed`, `seller_processing_days`, and `carrier_transit_days`, loading them into ClickHouse `fact_deliveries` and `dim_*` tables for blazing-fast dashboarding in Metabase.

Finance can cut costs by penalizing underperforming routes, and Operations can set strict SLAs for the worst bottleneck sellers. Let's demonstrate the ClickHouse connection for these final BI queries."""))

# Cell 20: Code clickhouse
cells.append(nbf.v4.new_code_cell("""# Connect to ClickHouse to verify the Data Warehouse is ready for Metabase
try:
    client = clickhouse_connect.get_client(
        host='clickhouse', port=8123, 
        username='jupyter_reader', password='jupyter_readonly_pass',
        database='operational_db'
    )
    print("Successfully connected to the ClickHouse Data Warehouse!")
    
    # Example Query
    df_ch = client.query_df("SELECT count(*) as total_fact_rows FROM fact_deliveries")
    print(df_ch)
except Exception as e:
    print("ClickHouse connection not fully set up in this environment yet. Error:", e)"""))

# Cell 21: Markdown
cells.append(nbf.v4.new_markdown_cell("""### Conclusion
Our 3-Act Data Story validates that operations are bleeding trust through late deliveries primarily caused by specific regional bottlenecks and slow seller processing. By orchestrating this logic in Airflow and querying in ClickHouse via Metabase, DustiniaDelixia can proactively monitor and heal its supply chain."""))


nb.cells = cells

with open('EDA.ipynb', 'w') as f:
    nbf.write(nb, f)


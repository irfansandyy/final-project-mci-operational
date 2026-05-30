import os
from datetime import datetime, timedelta
import zipfile
import gdown

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = '/opt/airflow/DustiniaDelixia_Groceria'
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse123')
GDRIVE_URL = 'https://drive.google.com/uc?id=1BRrvsIDOk9soBlsKwWqMsS79oVBQgZtf'
ZIP_PATH = '/opt/airflow/DustiniaDelixia_Groceria.zip'

def download_and_extract_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    required_files = [
        'orders.csv', 
        'order_items.csv', 
        'sellers.csv', 
        'customers.csv', 
        'order_reviews.csv'
    ]
    
    files_exist = all([os.path.exists(os.path.join(DATA_DIR, f)) for f in required_files])
    
    if not files_exist:
        print("Downloading dataset from Google Drive...")
        gdown.download(GDRIVE_URL, ZIP_PATH, quiet=False)
        print("Download complete. Extracting...")
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
        print("Extraction complete.")
    else:
        print("Dataset already exists locally. Skipping download.")

def data_quality_checks():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    spark = SparkSession.builder \
        .appName("DQ_Checks") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
        
    orders = spark.read.csv(os.path.join(DATA_DIR, 'orders.csv'), header=True, inferSchema=True)
    
    # Check 1: No negative delivery times
    delivered = orders.filter(col('order_delivered_customer_date').isNotNull())
    invalid_dates = delivered.filter(col('order_delivered_customer_date') < col('order_purchase_timestamp')).count()
    if invalid_dates > 0:
        print(f"Warning: Found {invalid_dates} delivery dates earlier than purchase dates.")
        
    # Check 2: Unique order IDs
    total_orders = orders.count()
    unique_orders = orders.select('order_id').distinct().count()
    assert total_orders == unique_orders, "Data Quality Error: Duplicate order IDs found."
    print("Data quality checks passed successfully!")
    spark.stop()

def transform_and_load():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp, datediff, hour, concat_ws, unix_timestamp, coalesce, lit, when
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml.classification import RandomForestClassifier
    
    spark = SparkSession.builder \
        .appName("Operational_ETL_PySpark") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()
        
    # Load Data
    orders = spark.read.csv(os.path.join(DATA_DIR, 'orders.csv'), header=True, inferSchema=True)
    order_items = spark.read.csv(os.path.join(DATA_DIR, 'order_items.csv'), header=True, inferSchema=True)
    sellers = spark.read.csv(os.path.join(DATA_DIR, 'sellers.csv'), header=True, inferSchema=True)
    customers = spark.read.csv(os.path.join(DATA_DIR, 'customers.csv'), header=True, inferSchema=True)
    reviews = spark.read.csv(os.path.join(DATA_DIR, 'order_reviews.csv'), header=True, inferSchema=True)

    # Clean Timestamp columns
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    for c in date_cols:
        orders = orders.withColumn(c, to_timestamp(col(c)))

    # Compute Metrics
    orders = orders.withColumn("lead_time_days", (unix_timestamp("order_delivered_customer_date") - unix_timestamp("order_purchase_timestamp")) / (24 * 3600))
    orders = orders.withColumn("processing_time_days", (unix_timestamp("order_delivered_carrier_date") - unix_timestamp("order_purchase_timestamp")) / (24 * 3600))
    orders = orders.withColumn("is_delayed", (col("order_delivered_customer_date") > col("order_estimated_delivery_date")).cast("int"))

    # Join dataframes
    order_seller_bridge = order_items.select("order_id", "seller_id", "freight_value").dropDuplicates(["order_id"])
    fact = orders.join(order_seller_bridge, on="order_id", how="left")
    fact = fact.join(customers.select("customer_id", "customer_state"), on="customer_id", how="left")
    fact = fact.join(sellers.select("seller_id", "seller_state"), on="seller_id", how="left")
    
    fact = fact.withColumn("purchase_hour", hour("order_purchase_timestamp"))
    fact = fact.withColumn("seller_state", coalesce(col("seller_state"), lit('UNKNOWN')))
    fact = fact.withColumn("customer_state", coalesce(col("customer_state"), lit('UNKNOWN')))
    fact = fact.withColumn("route", concat_ws(" -> ", col("seller_state"), col("customer_state")))
    
    # ML Prediction (Spark MLlib)
    # Filter for training where is_delayed is not null
    train_data = fact.filter(col("is_delayed").isNotNull())
    
    seller_indexer = StringIndexer(inputCol="seller_state", outputCol="seller_state_encoded", handleInvalid="keep")
    customer_indexer = StringIndexer(inputCol="customer_state", outputCol="customer_state_encoded", handleInvalid="keep")
    
    fact_indexed = seller_indexer.fit(fact).transform(fact)
    fact_indexed = customer_indexer.fit(fact_indexed).transform(fact_indexed)
    
    # Fill nulls in purchase hour for ML
    fact_indexed = fact_indexed.fillna({"purchase_hour": 12})
    
    assembler = VectorAssembler(inputCols=["seller_state_encoded", "customer_state_encoded", "purchase_hour"], outputCol="features")
    fact_assembled = assembler.transform(fact_indexed)
    
    train_assembled = fact_assembled.filter(col("is_delayed").isNotNull())
    
    if train_assembled.count() > 0:
        rf = RandomForestClassifier(labelCol="is_delayed", featuresCol="features", numTrees=20, maxDepth=5, seed=42)
        model = rf.fit(train_assembled)
        predictions = model.transform(fact_assembled)
        
        # Extract probability of class 1 (delayed)
        from pyspark.ml.functions import vector_to_array
        predictions = predictions.withColumn("predicted_delay_probability", vector_to_array(col("probability"))[1])
        fact_final = predictions.drop("features", "probability", "rawPrediction", "prediction", "seller_state_encoded", "customer_state_encoded")
    else:
        fact_final = fact_assembled.withColumn("predicted_delay_probability", lit(0.0)).drop("features", "seller_state_encoded", "customer_state_encoded")
        
    # Prepare JDBC Properties
    jdbc_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/operational_db"
    properties = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    # Load Dimensions
    dim_cust = customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state")
    dim_cust.write.jdbc(url=jdbc_url, table="dim_customers", mode="append", properties=properties)

    dim_sellers = sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    dim_sellers.write.jdbc(url=jdbc_url, table="dim_sellers", mode="append", properties=properties)

    dim_reviews = reviews.select("review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp").filter(col("order_id").isNotNull() & col("review_id").isNotNull())
    dim_reviews = dim_reviews.withColumn("review_creation_date", to_timestamp(col("review_creation_date")))
    dim_reviews = dim_reviews.withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))
    dim_reviews.write.jdbc(url=jdbc_url, table="dim_reviews", mode="append", properties=properties)

    # Fact Table selection
    fact_cols = [
        'order_id', 'customer_id', 'seller_id', 'order_status', 
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date', 
        'lead_time_days', 'processing_time_days', 'is_delayed', 
        'freight_value', 'purchase_hour', 'route',
        'predicted_delay_probability'
    ]
    fact_final_write = fact_final.select(*fact_cols)
    fact_final_write.write.jdbc(url=jdbc_url, table="fact_deliveries", mode="append", properties=properties)
    
    print(f"Successfully loaded data into ClickHouse via PySpark JDBC.")
    spark.stop()

with DAG(
    'operational_etl_dag',
    default_args=default_args,
    description='ETL pipeline for DustiniaDelixia Groceria Operational Analytics using PySpark',
    schedule=timedelta(hours=1),
    catchup=False,
) as dag:

    download_extract = PythonOperator(
        task_id='download_extract_task',
        python_callable=download_and_extract_data,
    )

    data_quality = PythonOperator(
        task_id='data_quality_check_task',
        python_callable=data_quality_checks,
    )

    transform_and_load_db = PythonOperator(
        task_id='transform_and_load_task',
        python_callable=transform_and_load,
    )

    download_extract >> data_quality >> transform_and_load_db

import os
import zipfile
from datetime import datetime, timedelta
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from clickhouse_driver import Client
import numpy as np
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
GDRIVE_URL = 'https://drive.google.com/uc?id=1BRrvsIDOk9soBlsKwWqMsS79oVBQgZtf'
ZIP_PATH = '/opt/airflow/DustiniaDelixia_Groceria.zip'

def download_and_extract_data():
    """Download the dataset from Google Drive and extract it if not present."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    required_files = [
        'orders.csv', 
        'order_items.csv', 
        'sellers.csv', 
        'customers.csv', 
        'order_reviews.csv'
    ]
    
    # Check if all required files already exist to skip download
    files_exist = all([os.path.exists(os.path.join(DATA_DIR, f)) for f in required_files])
    
    if not files_exist:
        print("Downloading dataset from Google Drive...")
        gdown.download(GDRIVE_URL, ZIP_PATH, quiet=False)
        print("Download complete. Extracting...")
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            # The zip likely extracts into a folder, handle accordingly
            zip_ref.extractall('/opt/airflow/')
        print("Extraction complete.")
        
        # Verify files are now present
        files_exist = all([os.path.exists(os.path.join(DATA_DIR, f)) for f in required_files])
        if not files_exist:
            raise Exception("Dataset files are still missing after extraction. Check zip contents structure.")
    else:
        print("Dataset already exists locally. Skipping download.")

def data_quality_checks():
    """Perform basic assert-based data quality checks."""
    orders = pd.read_csv(os.path.join(DATA_DIR, 'orders.csv'), 
                         parse_dates=['order_purchase_timestamp', 'order_delivered_customer_date'])
    
    # Check 1: No negative delivery times
    delivered_orders = orders.dropna(subset=['order_delivered_customer_date'])
    assert (delivered_orders['order_delivered_customer_date'] >= delivered_orders['order_purchase_timestamp']).all(), \
        "Data Quality Error: Found delivery dates earlier than purchase dates."
    
    # Check 2: Unique order IDs
    assert orders['order_id'].nunique() == len(orders), \
        "Data Quality Error: Duplicate order IDs found."
        
    print("Data quality checks passed successfully!")

def transform_and_load():
    """Transform data, calculate metrics, run ML prediction, and load into ClickHouse."""
    orders = pd.read_csv(os.path.join(DATA_DIR, 'orders.csv'))
    order_items = pd.read_csv(os.path.join(DATA_DIR, 'order_items.csv'))
    sellers = pd.read_csv(os.path.join(DATA_DIR, 'sellers.csv'))
    customers = pd.read_csv(os.path.join(DATA_DIR, 'customers.csv'))
    reviews = pd.read_csv(os.path.join(DATA_DIR, 'order_reviews.csv'))

    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    for col in date_cols:
        orders[col] = pd.to_datetime(orders[col])
        
    reviews['review_creation_date'] = pd.to_datetime(reviews['review_creation_date'])
    reviews['review_answer_timestamp'] = pd.to_datetime(reviews['review_answer_timestamp'])

    orders['lead_time_days'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.total_seconds() / (24 * 3600)
    orders['processing_time_days'] = (orders['order_delivered_carrier_date'] - orders['order_purchase_timestamp']).dt.total_seconds() / (24 * 3600)
    orders['is_delayed'] = (orders['order_delivered_customer_date'] > orders['order_estimated_delivery_date']).astype(int)

    order_seller_bridge = order_items[['order_id', 'seller_id']].drop_duplicates(subset=['order_id'])
    
    fact = orders.merge(order_seller_bridge, on='order_id', how='left')
    fact = fact.merge(customers[['customer_id', 'customer_state']], on='customer_id', how='left')
    fact = fact.merge(sellers[['seller_id', 'seller_state']], on='seller_id', how='left')
    
    fact['purchase_hour'] = fact['order_purchase_timestamp'].dt.hour
    fact['seller_state'] = fact['seller_state'].fillna('UNKNOWN')
    fact['customer_state'] = fact['customer_state'].fillna('UNKNOWN')
    
    le_seller = LabelEncoder()
    le_customer = LabelEncoder()
    fact['seller_state_encoded'] = le_seller.fit_transform(fact['seller_state'])
    fact['customer_state_encoded'] = le_customer.fit_transform(fact['customer_state'])
    
    features = ['seller_state_encoded', 'customer_state_encoded', 'purchase_hour']
    
    train_data = fact.dropna(subset=['is_delayed'])
    
    if len(train_data) > 0:
        X = train_data[features]
        y = train_data['is_delayed']
        clf = RandomForestClassifier(n_estimators=20, max_depth=5, random_state=42)
        clf.fit(X, y)
        fact['predicted_delay_probability'] = clf.predict_proba(fact[features])[:, 1]
    else:
        fact['predicted_delay_probability'] = 0.0

    client = Client(host=CLICKHOUSE_HOST)
    
    cust_records = customers[['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']].to_dict('records')
    client.execute('INSERT INTO operational_db.dim_customers VALUES', cust_records)
    
    seller_records = sellers[['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']].to_dict('records')
    client.execute('INSERT INTO operational_db.dim_sellers VALUES', seller_records)
    
    rev_df = reviews[['review_id', 'order_id', 'review_score', 'review_creation_date', 'review_answer_timestamp']].copy()
    rev_df['review_creation_date'] = rev_df['review_creation_date'].dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', None)
    rev_df['review_answer_timestamp'] = rev_df['review_answer_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', None)
    rev_df = rev_df.dropna(subset=['order_id', 'review_id'])
    rev_records = rev_df.to_dict('records')
    client.execute('INSERT INTO operational_db.dim_reviews VALUES', rev_records)
    
    fact_cols = [
        'order_id', 'customer_id', 'seller_id', 'order_status', 
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date', 
        'lead_time_days', 'processing_time_days', 'is_delayed', 
        'predicted_delay_probability'
    ]
    fact_df = fact[fact_cols].copy()
    
    for col in date_cols:
        fact_df[col] = fact_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', None)
        
    fact_df['seller_id'] = fact_df['seller_id'].fillna('')
    fact_df['lead_time_days'] = fact_df['lead_time_days'].replace({np.nan: None})
    fact_df['processing_time_days'] = fact_df['processing_time_days'].replace({np.nan: None})
    fact_df['is_delayed'] = fact_df['is_delayed'].fillna(0).astype(int)
    
    fact_records = fact_df.to_dict('records')
    client.execute('INSERT INTO operational_db.fact_deliveries VALUES', fact_records)
    
    print(f"Successfully loaded {len(fact_records)} records into fact_deliveries.")

with DAG(
    'operational_etl_dag',
    default_args=default_args,
    description='ETL pipeline for DustiniaDelixia Groceria Operational Analytics',
    schedule_interval=timedelta(days=1),
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

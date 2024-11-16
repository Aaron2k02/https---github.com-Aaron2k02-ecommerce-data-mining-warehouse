# Standard library imports
from datetime import datetime  # For handling date and time operations
import logging  # For logging information during task execution
import os  # For interacting with the file system
from airflow.models.baseoperator import chain

# Airflow core imports
from airflow import DAG  # For defining the DAG structure
from airflow.operators.python import PythonOperator  # For running Python functions as tasks
from airflow.decorators import task  # For defining tasks using the TaskFlow API

# Google Cloud providers for Airflow
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # For uploading local files to Google Cloud Storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # For interacting with Google Cloud Storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator  # For BigQuery dataset and SQL job operations
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # For interacting with BigQuery

# Data manipulation and file handling
import pandas as pd  # For data manipulation using DataFrames
from io import StringIO  # For handling in-memory text streams

from datetime import datetime, timedelta
from google.cloud import pubsub_v1, storage
from faker import Faker
import json
import random
import csv
import io
import time

# Load credentials from the Airflow connection
from airflow.hooks.base import BaseHook

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Initialize Faker
fake = Faker()

credentials = GoogleBaseHook(gcp_conn_id='data_warehouse').get_credentials()

# Define constants for GCS and BigQuery configurations
BUCKET_NAME = 'u2102810_ecom_dataset'  # GCS bucket name for storing data
GCS_PATH = 'raw'  # GCS folder path for raw data files
GCS_CONN_ID = 'data_warehouse'  # Airflow connection ID for GCS
BIGQUERY_DATASET = 'ecom_stg'  # BigQuery staging dataset name
BIGQUERY_DATASET_PROD = 'ecom_prod'  # BigQuery production dataset name
PROJECT_ID = 'data-mining-warehouse-ecom'  # GCP project ID
TOPIC_ID = "transactions"
SUBSCRIPTION_ID = "transactions-sub"

# Initialize Pub/Sub publisher and GCS client
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
storage_client = storage.Client(credentials=credentials)

# Helper function to read SQL files for transformations
def read_sql_file(file_path):
    """
    Read SQL query from a file.

    Args:
        file_path (str): Path to the SQL file.
    
    Returns:
        str: The SQL query as a string.
    """
    with open(file_path, 'r') as file:
        return file.read()

# Load SQL queries for data transformations
dim_cities_query = read_sql_file('/usr/local/airflow/include/transform/dim_cities_query.sql')
dim_customer_query = read_sql_file('/usr/local/airflow/include/transform/dim_customer_query.sql')
dim_date_query = read_sql_file('/usr/local/airflow/include/transform/dim_date_query.sql')
dim_gender_query = read_sql_file('/usr/local/airflow/include/transform/dim_gender_query.sql')
dim_merchant_query = read_sql_file('/usr/local/airflow/include/transform/dim_merchant_query.sql')
fact_transactions_query = read_sql_file('/usr/local/airflow/include/transform/fact_transactions_query.sql')

# Data generation function
def generate_transaction_data(customer_ids, branch_ids, transaction_id_start=5001):
    coupon_name = f"{fake.random_uppercase_letter()}{fake.random_uppercase_letter()}{fake.random_uppercase_letter()}-{random.randint(100, 999)}"
    transaction_date = fake.date_this_year()
    burn_date = fake.date_between(start_date=transaction_date) if random.choice([True, False]) else None
    data = {
        "transaction_id": transaction_id_start,
        "customer_id": random.choice(customer_ids),
        "transaction_date": str(transaction_date),
        "transaction_status": "subscribed",
        "coupon_name": coupon_name,
        "burn_date": str(burn_date) if burn_date else None,
        "branch_id": random.choice(branch_ids),
        "redemption_status": "Redeemed" if burn_date else "Not Redeemed"
    }
    return data

# Publish fake data to Pub/Sub
def publish_fake_data():
    customer_ids = [36, 307, 37, 309, 38]
    branch_ids = [7, 3, 5, 1, 9]
    for i in range(30):
        data = generate_transaction_data(customer_ids, branch_ids, 5000 + i)
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        print(f"Published: {data}")
        time.sleep(1)

# Pull messages from Pub/Sub and write to GCS
def pull_messages_to_gcs():
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob("raw/Transactions_2024.csv")

    # Prepare in-memory CSV file
    output = io.StringIO()
    fieldnames = [
        "transaction_id", "customer_id", "transaction_date",
        "transaction_status", "coupon_name", "burn_date",
        "branch_id", "redemption_status"
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    def callback(message):
        data = json.loads(message.data)
        message.ack()
        writer.writerow(data)
        print(f"Message processed: {data}")

    subscriber.subscribe(subscription_path, callback=callback)
    time.sleep(30)  # Allow time for messages to be pulled

    # Upload the CSV to GCS
    blob.upload_from_string(output.getvalue(), content_type='text/csv')
    print("Data saved to GCS successfully.")

def load_transactions_to_bq(**kwargs):
    """
    Merge transaction files (transaction*.csv) from GCS and load into BigQuery table 'Transaction'.
    """
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    bq_hook = BigQueryHook(gcp_conn_id=GCS_CONN_ID, use_legacy_sql=False)

    transaction_prefix = f"{GCS_PATH}/Transactions"
    files = gcs_hook.list(bucket_name=BUCKET_NAME, prefix=transaction_prefix)
    logging.info(f"Files in GCS matching 'transaction*': {files}")

    transaction_df = pd.DataFrame()

    for file_path in files:
        if file_path.endswith('.csv'):
            file_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=file_path)
            decoded_file_data = file_data.decode('utf-8')
            df = pd.read_csv(StringIO(decoded_file_data))
            df.columns = [col.lower().replace(".", "_").replace(" ", "_").replace("(", "_").replace(")", "") for col in df.columns]
            transaction_df = pd.concat([transaction_df, df], ignore_index=True)
    
    if transaction_df.empty:
        logging.warning("No transaction files found or files are empty. Skipping load.")
        return

    temp_csv = StringIO()
    transaction_df.to_csv(temp_csv, index=False)
    temp_csv.seek(0)

    merged_file_path = f"{GCS_PATH}/temp_transactions.csv"
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=merged_file_path, data=temp_csv.getvalue())
    logging.info(f"Uploaded merged transaction file to GCS: {merged_file_path}")

    job_config = {
        "sourceUris": [f"gs://{BUCKET_NAME}/{merged_file_path}"],
        "destinationTable": {
            "projectId": "data-mining-warehouse-ecom",
            "datasetId": BIGQUERY_DATASET,
            "tableId": "transactions",
        },
        "sourceFormat": "CSV",
        "skipLeadingRows": 1,
        "writeDisposition": "WRITE_TRUNCATE",
        "autodetect": True
    }

    try:
        bq_hook.insert_job(project_id="data-mining-warehouse-ecom", configuration={"load": job_config})
        logging.info("Successfully loaded merged transaction data into BigQuery table 'Transaction'")
    except Exception as e:
        logging.error(f"Error loading merged transaction data into BigQuery: {e}")
        raise
    finally:
        gcs_hook.delete(bucket_name=BUCKET_NAME, object_name=merged_file_path)

# Helper function to create transformation SQL task
def create_sql_task(task_id: str, sql_query: str):
    """
    Create a BigQueryInsertJobOperator task for SQL transformations.

    Args:
        task_id (str): Unique ID for the task.
        sql_query (str): SQL query to execute.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location="US",
        project_id=PROJECT_ID,
        gcp_conn_id=GCS_CONN_ID,
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                # "defaultDataset": {"projectId": PROJECT_ID, "datasetId": BIGQUERY_DATASET_PROD},
                # "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
    
# Default args for Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
with DAG(
    'cloud_ecommerce_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    schedule_interval=None,  # None for manual triggering
    catchup=False,
    tags=['ecommerce', 'data_pipeline'],
    description='Cloud ELT pipeline for e-commerce data',
) as dag:

    # Airflow Tasks
    publish_task = PythonOperator(
        task_id='publish_fake_data',
        python_callable=publish_fake_data,
        dag=dag,
    )

    pull_task = PythonOperator(
        task_id='pull_messages_to_gcs',
        python_callable=pull_messages_to_gcs,
        dag=dag,
    )
    
    # Task to load transactions files from GCS to BigQuery
    load_transactions_task = PythonOperator(
        task_id='load_transactions_to_bq',
        python_callable=load_transactions_to_bq,
        provide_context=True,
    )
    
    # # Create tasks for each SQL transformation
    dim_cities_task = create_sql_task('transform_dim_cities', dim_cities_query)
    dim_customer_task = create_sql_task('transform_dim_customer', dim_customer_query)
    dim_date_task = create_sql_task('transform_dim_date', dim_date_query)
    dim_gender_task = create_sql_task('transform_dim_gender', dim_gender_query)
    dim_merchant_task = create_sql_task('transform_dim_merchant', dim_merchant_query)
    fact_transactions_task = create_sql_task('transform_fact_transactions', fact_transactions_query)
    
    # # Chain the loading tasks
    
    chain(publish_task, pull_task, load_transactions_task,dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task, fact_transactions_task)
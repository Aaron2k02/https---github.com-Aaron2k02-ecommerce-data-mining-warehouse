from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import pubsub_v1, storage
from faker import Faker
import json
import random
import csv
import io
import time

from google.oauth2 import service_account
from google.cloud import pubsub_v1
import logging

# Load credentials from the Airflow connection
from airflow.hooks.base import BaseHook

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# Initialize Faker
fake = Faker()

# gcs_connection = BaseHook.get_connection("data_warehouse")
# logging.info(gcs_connection.extra_dejson)
# service_account_info = json.loads(gcs_connection.extra_dejson.get("extra__google_cloud_platform__keyfile_dict"))
# credentials = service_account.Credentials.from_service_account_info(service_account_info)

credentials = GoogleBaseHook(gcp_conn_id='data_warehouse').get_credentials()

# Google Cloud configurations (fetch from Airflow variables or define directly)
PROJECT_ID = "data-mining-warehouse-ecom"
TOPIC_ID = "transactions"
SUBSCRIPTION_ID = "transactions-sub"
BUCKET_NAME = "u2102810_ecom_dataset"

# Default args for Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Airflow DAG setup
dag = DAG(
    'pubsub_to_gcs_ingestion',
    default_args=default_args,
    description='Simulate real-time data ingestion using Google Pub/Sub and store in GCS',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 11, 13),
    catchup=False,
)

# Initialize Pub/Sub publisher and GCS client
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
storage_client = storage.Client(credentials=credentials)

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
    for i in range(10):
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

publish_task >> pull_task

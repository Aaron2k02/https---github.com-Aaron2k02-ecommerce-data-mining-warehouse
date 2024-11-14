# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from google.cloud import bigquery
# from kafka import KafkaProducer, KafkaConsumer
# from faker import Faker
# import json
# import random
# import time

# # Initialize Faker
# fake = Faker()

# # BigQuery client setup
# client = bigquery.Client()

# # Default args for Airflow
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Airflow DAG setup
# dag = DAG(
#     'kafka_to_bigquery_ingestion',
#     default_args=default_args,
#     description='Simulate real-time data ingestion and store in BigQuery',
#     schedule_interval=timedelta(minutes=1),
#     start_date=datetime(2024, 11, 13),
#     catchup=False,
# )

# # Kafka producer configuration
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda x: json.dumps(x).encode('utf-8')
# )

# # Kafka consumer configuration
# consumer = KafkaConsumer(
#     'transactions',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='transactions_group'
# )

# # Updated data generation function based on requirements
# def generate_transaction_data(customer_ids, branch_ids, transaction_id_start=5001):
#     # Generate coupon names with format `XXX-123`
#     coupon_name = f"{fake.random_uppercase_letter()}{fake.random_uppercase_letter()}{fake.random_uppercase_letter()}-{random.randint(100, 999)}"

#     # Generate transaction date
#     transaction_date = fake.date_this_year()
    
#     # Generate burn date - should be on or after transaction date
#     burn_date = fake.date_between(start_date=transaction_date) if random.choice([True, False]) else None

#     # Determine transaction status based on the presence of burn_date
#     # transaction_status = "redeemed" if burn_date else "subscribed"

#     # Generate synthetic data
#     data = {
#         "transaction_id": transaction_id_start,
#         "customer_id": random.choice(customer_ids),
#         "transaction_date": transaction_date,
#         "transaction_status": "subscribed",
#         "coupon_name": coupon_name,
#         "burn_date": burn_date,
#         "branch_id": random.choice(branch_ids),
#         "redemption_status": "Redeemed" if burn_date else "Not Redeemed"
#     }
    
#     return data

# # Task 1: Produce fake transaction data to Kafka
# def produce_fake_data():
#     customer_ids = [36, 307, 37, 309, 38, 410, 39, 432, 40, 515, 41, 600, 42, 645, 43, 663, 44, 666, 45, 771, 46, 842, 47, 922, 48, 940, 49, 12, 50, 46]
#     branch_ids = [7, 3, 5, 1, 9, 10, 4, 6, 2, 8]
    
#     # Starting transaction ID
#     transaction_id_start = 5001
    
#     # Produce data every 2 seconds for simulation
#     try:
#         for i in range(30):  # Limit to 30 messages per run
#             data = generate_transaction_data(customer_ids, branch_ids, transaction_id_start + i)
#             print(f"Producing: {data}")
#             producer.send('transactions', value=data)
#             time.sleep(2)
#     except Exception as e:
#         print(f"Error in producing data: {e}")
#     finally:
#         producer.close()

# # Task 2: Ingest Kafka messages to BigQuery
# def ingest_transaction_to_bq(data):
#     table_id = 'your_project.your_dataset.transactions'  # Replace with your BigQuery table

#     # Schema definition for BigQuery
#     schema = [
#         bigquery.SchemaField("transaction_id", "INTEGER"),
#         bigquery.SchemaField("customer_id", "INTEGER"),
#         bigquery.SchemaField("transaction_date", "DATE"),
#         bigquery.SchemaField("transaction_status", "STRING"),
#         bigquery.SchemaField("coupon_name", "STRING"),
#         bigquery.SchemaField("burn_date", "DATE"),
#         bigquery.SchemaField("branch_id", "INTEGER"),
#         bigquery.SchemaField("redemption_status", "STRING"),
#     ]

#     # Insert the data into BigQuery
#     try:
#         rows_to_insert = [data]
#         errors = client.insert_rows_json(table_id, rows_to_insert, row_ids=[None], schema=schema)
#         if errors:
#             print(f"Encountered errors while inserting rows: {errors}")
#         else:
#             print(f"Data inserted into BigQuery: {data}")
#     except Exception as e:
#         print(f"Error inserting data into BigQuery: {e}")

# # Task 3: Read from Kafka and ingest to BigQuery
# def read_from_kafka():
#     try:
#         for message in consumer:
#             data = json.loads(message.value)
#             ingest_transaction_to_bq(data)
#     except Exception as e:
#         print(f"Error reading from Kafka: {e}")
#     finally:
#         consumer.close()
# produce_fake_data
# # Airflow Tasks
# produce_task = PythonOperator(
#     task_id='produce_fake_data',
#     python_callable=produce_fake_data,
#     dag=dag,
# )

# consume_task = PythonOperator(
#     task_id='consume_and_ingest_data',
#     python_callable=read_from_kafka,
#     dag=dag,
# )

# # Task sequence
# produce_task >> consume_task

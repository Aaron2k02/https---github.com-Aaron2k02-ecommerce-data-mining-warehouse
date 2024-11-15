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

# Define constants for GCS and BigQuery configurations
BUCKET_NAME = 'u2102810_ecom_dataset'  # GCS bucket name for storing data
GCS_PATH = 'raw'  # GCS folder path for raw data files
GCS_CONN_ID = 'data_warehouse'  # Airflow connection ID for GCS
BIGQUERY_DATASET = 'ecom_stg'  # BigQuery staging dataset name
BIGQUERY_DATASET_PROD = 'ecom_prod'  # BigQuery production dataset name
PROJECT_ID = 'data-mining-warehouse-ecom'  # GCP project ID

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

# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path, **kwargs):
    """
    Upload CSV files from a local folder to Google Cloud Storage.

    Args:
        data_folder (str): Local folder containing the CSV files.
        gcs_path (str): GCS folder path to upload files to.
    """
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{csv_file.split(".")[0]}_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=BUCKET_NAME,
            gcp_conn_id=GCS_CONN_ID,
            mime_type='text/csv'
        )
        upload_task.execute(context=kwargs)  # Executes each upload task

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
    
def load_table_to_bq(file_name, table_name, **kwargs):
    """
    Load a single CSV file from GCS to BigQuery table.
    """
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    bq_hook = BigQueryHook(gcp_conn_id=GCS_CONN_ID, use_legacy_sql=False)

    # Construct the full GCS file path
    file_path = f"{GCS_PATH}/{file_name}.csv"
    logging.info(f"Attempting to load file from GCS: {file_path}")
    
    # Download the CSV file data from GCS
    file_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=file_path)
    
    # Check if file data is present
    if not file_data:
        logging.warning(f"{file_path} not found or is empty. Skipping load.")
        return

    # Decode the file data to a string and read it as a DataFrame
    decoded_file_data = file_data.decode('utf-8')
    df = pd.read_csv(StringIO(decoded_file_data))
    
    # Normalize column names
    df.columns = [col.lower().replace(".", "_").replace(" ", "_").replace("(", "_").replace(")", "") 
                  for col in df.columns]
    logging.info(f"Normalized column names: {df.columns}")

    # Save DataFrame to a temporary in-memory CSV
    temp_csv = StringIO()
    df.to_csv(temp_csv, index=False)
    temp_csv.seek(0)

    # Define the path for the temporary GCS file
    temp_file_path = f"{GCS_PATH}/temp_{file_name}.csv"
    
    # Upload the temporary CSV to GCS
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=temp_file_path, data=temp_csv.getvalue())
    logging.info(f"Uploaded temporary file to GCS: {temp_file_path}")

    # Configure BigQuery job
    job_config = {
        "sourceUris": [f"gs://{BUCKET_NAME}/{temp_file_path}"],
        "destinationTable": {
            "projectId": "data-mining-warehouse-ecom",
            "datasetId": BIGQUERY_DATASET,
            "tableId": table_name,
        },
        "sourceFormat": "CSV",
        "skipLeadingRows": 1,
        "writeDisposition": "WRITE_TRUNCATE",  # You can pass this as an argument to change behavior
        "autodetect": True
    }

    # Load the CSV file into BigQuery
    try:
        bq_hook.insert_job(project_id="data-mining-warehouse-ecom", configuration={"load": job_config})
        logging.info(f"Successfully loaded '{file_name}' into BigQuery table '{table_name}'")
    except Exception as e:
        logging.error(f"Error loading '{file_name}' into BigQuery: {e}")
        raise
    finally:
        # Clean up temporary file from GCS
        gcs_hook.delete(bucket_name=BUCKET_NAME, object_name=temp_file_path)
        logging.info(f"Deleted temporary file from GCS: {temp_file_path}")

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

# DAG Definition
with DAG(
    'ecommerce_data_pipeline',
    start_date=datetime(2024, 11, 1),
    schedule_interval=None,  # None for manual triggering
    catchup=False,
    tags=['ecommerce', 'data_pipeline'],
    description='ETL pipeline for e-commerce data',
) as dag:
    
    # Task to upload CSV files to GCS
    upload_csvs_task = PythonOperator(
        task_id='upload_all_csvs_to_gcs',
        python_callable=upload_to_gcs,
        op_args=['/usr/local/airflow/include/dataset', 'raw'],  # Local data folder and GCS path
        provide_context=True,
    )
    
    # Task to load transactions files from GCS to BigQuery
    load_transactions_task = PythonOperator(
        task_id='load_transactions_to_bq',
        python_callable=load_transactions_to_bq,
        provide_context=True,
    )
    
    load_branches_task = PythonOperator(
        task_id='load_branches_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'file_name': 'Branches',  # GCS file name without .csv extension
            'table_name': 'branches'  # BigQuery table name
        },
        provide_context=True,
    )
    
    load_cities_task = PythonOperator(
        task_id='load_cities_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'file_name': 'Cities',  # GCS file name without .csv extension
            'table_name': 'cities'  # BigQuery table name
        },
        provide_context=True,
    )

    load_customers_task = PythonOperator(
        task_id='load_customers_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'file_name': 'Customers',  # GCS file name without .csv extension
            'table_name': 'customers'  # BigQuery table name
        },
        provide_context=True,
    )

    load_genders_task = PythonOperator(
        task_id='load_genders_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'file_name': 'Genders',  # GCS file name without .csv extension
            'table_name': 'genders'  # BigQuery table name
        },
        provide_context=True,
    )
    
    load_merchants_task = PythonOperator(
        task_id='load_merchants_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'file_name': 'Merchants',  # GCS file name without .csv extension
            'table_name': 'merchants'  # BigQuery table name
        }
    )
    
    # # Create tasks for each SQL transformation
    dim_cities_task = create_sql_task('transform_dim_cities', dim_cities_query)
    dim_customer_task = create_sql_task('transform_dim_customer', dim_customer_query)
    dim_date_task = create_sql_task('transform_dim_date', dim_date_query)
    dim_gender_task = create_sql_task('transform_dim_gender', dim_gender_query)
    dim_merchant_task = create_sql_task('transform_dim_merchant', dim_merchant_query)
    fact_transactions_task = create_sql_task('transform_fact_transactions', fact_transactions_query)

    # upload_csvs_task >> load_transactions_task 
    # load_transactions_task >> [load_branches_task, load_cities_task, load_customers_task, load_genders_task, load_merchants_task]
    # load_transactions_task >> [dim_cities_task, dim_customer_task, dim_gender_task, dim_merchant_task] >> dim_date_task
    # # chain(
    #     [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task], 
    #     [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task]
    # )
    # upload_csvs_task >> load_transactions_task
    # load_transactions_task >> [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task] >> fact_transactions_task
    
    # # Chain the loading tasks
    chain(upload_csvs_task, load_transactions_task,
            load_branches_task, load_cities_task, load_customers_task, load_genders_task, load_merchants_task,
            [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task])
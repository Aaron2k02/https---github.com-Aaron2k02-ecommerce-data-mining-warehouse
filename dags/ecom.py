from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

from airflow.decorators import task  # Import task for TaskFlow API

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pandas as pd
from io import StringIO

# Read SQL query from the dim_cities_query.sql file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()
    
 # Read SQL query for data modelling
dim_cities_query = read_sql_file('/usr/local/airflow/include/transform/dim_cities_query.sql')
dim_customer_query = read_sql_file('/usr/local/airflow/include/transform/dim_customer_query.sql')
dim_date_query = read_sql_file('/usr/local/airflow/include/transform/dim_date_query.sql')
dim_gender_query = read_sql_file('/usr/local/airflow/include/transform/dim_gender_query.sql')
dim_merchant_query = read_sql_file('/usr/local/airflow/include/transform/dim_merchant_query.sql')
fact_transactions_query = read_sql_file('/usr/local/airflow/include/transform/fact_transactions_query.sql')

# Define constants
BUCKET_NAME = 'u2102810_ecom_dataset'  # Your GCS bucket name
GCS_PATH = 'raw'  # Folder in GCS bucket
GCS_CONN_ID = 'data_warehouse'  # Airflow connection ID for GCP
BIGQUERY_DATASET = 'ecom_stg'  # BigQuery dataset name
BIGQUERY_DATASET_PROD = 'ecom_prod' 
PROJECT_ID = 'data-mining-warehouse-ecom'

# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path, **kwargs):
    bucket_name = 'u2102810_ecom_dataset'  # Your GCS bucket name
    gcs_conn_id = 'data_warehouse'  # Airflow connection ID for GCP

    # List all CSV files in the data folder
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{csv_file.split(".")[0]}_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
            mime_type='text/csv'
        )
        upload_task.execute(context=kwargs)  # Executes each upload task\
    
def load_data_to_bq(**kwargs):
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        bq_hook = BigQueryHook(gcp_conn_id=GCS_CONN_ID, use_legacy_sql=False)

        # List files in the specified GCS bucket and path
        files = gcs_hook.list(bucket_name=BUCKET_NAME, prefix=GCS_PATH + '/')
        logging.info(f"Files in GCS: {files}")

        for file_path in files:
            file_name = file_path.split('/')[-1].replace('.csv', '').lower()
            
            # Download the file from GCS to transform column names
            file_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=file_path)
            
             # Decode bytes to string
            decoded_file_data = file_data.decode('utf-8')
            
            # Load into DataFrame
            df = pd.read_csv(StringIO(decoded_file_data))

            # Rename columns to a consistent naming convention
            df.columns = [col.lower().replace(".", "_").replace(" ", "_").replace("(", "_").replace(")", "") for col in df.columns]
            
            logging.info(f"Files in GCS column name: {df.columns}")

            # Upload the transformed file back to GCS as a temporary file
            temp_file_path = f"{GCS_PATH}/temp_{file_name}.csv"
            temp_data = df.to_csv(index=False)
            gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=temp_file_path, data=temp_data)

            # Define job configuration for BigQuery
            job_config = {
                "sourceUris": [f"gs://{BUCKET_NAME}/{temp_file_path}"],
                "destinationTable": {
                    "projectId": "data-mining-warehouse-ecom",
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": file_name,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True  
            }

            try:
                # Execute the BigQuery load job
                bq_hook.insert_job(
                    project_id="data-mining-warehouse-ecom",
                    configuration={"load": job_config}
                )
                logging.info(f"Successfully loaded {file_name} into BigQuery")
            except Exception as e:
                logging.error(f"Error loading {file_name} into BigQuery: {e}")
                raise
            finally:
                # Remove temporary file from GCS
                gcs_hook.delete(bucket_name=BUCKET_NAME, object_name=temp_file_path)

# Define an independent Python function to run in an external environment
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
def check_staging_data_soda(scan_name='check_staging_data_soda', checks_subpath='sources'):
    from include.soda.check_function import check

   # Ensure the check function returns a JSON-serializable result
    result = check(scan_name, checks_subpath)
    return result.get("result_code")  # Return only the result code for simplicity

# Define your DAG
with DAG(
    'upload_files_and_create_bq_dataset',
    start_date=datetime(2024, 11, 1),
    schedule_interval=None,  # None for manual triggering
    catchup=False,
    tags=['ecom'],
) as dag:
    
    # Task to upload CSV files to GCS
    upload_csvs_task = PythonOperator(
        task_id='upload_all_csvs_to_gcs',
        python_callable=upload_to_gcs,
        op_args=['/usr/local/airflow/include/dataset', 'raw'],  # Local data folder and GCS path
        provide_context=True,
    )
    
    # Task to create an empty BigQuery dataset no need since the load data to bigquery can auto detect the schema
    # create_ecom_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id='create_ecom_stg_dataset',
    #     dataset_id=BIGQUERY_DATASET,  # Name of the dataset in BigQuery
    #     gcp_conn_id='data_warehouse',
    # )
    
    # Task to load files from GCS to BigQuery
    gcs_to_bq_load = PythonOperator(
        task_id='load_data_to_bq',
        python_callable=load_data_to_bq,
        provide_context=True,
    )
    
    # Task to load files from GCS to BigQuery
    check_staging_data = PythonOperator(
        task_id='check_staging_data_soda',
        python_callable=check_staging_data_soda,
        provide_context=True,
        do_xcom_push=False  # Prevents result from being pushed to XCom
    )
    
    # Helper function to create transformation SQL task
    def create_sql_task(task_id, sql_query):
        return BigQueryInsertJobOperator(
            task_id=task_id,
            location="US",
            project_id='data-mining-warehouse-ecom',
            gcp_conn_id=GCS_CONN_ID,
            configuration={
                "query": {
                    "query": sql_query,
                    "useLegacySql": False,
                    "defaultDataset": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET_PROD,
                        "tableId": task_id.split('_')[1],
                    },
                    # "writeDisposition": "WRITE_TRUNCATE",
                    "autodetect": True 
                }
            },
        )

    # # Create tasks for each SQL transformation
    dim_cities_task = create_sql_task('transform_dim_cities', dim_cities_query)
    dim_customer_task = create_sql_task('transform_dim_customer', dim_customer_query)
    dim_date_task = create_sql_task('transform_dim_date', dim_date_query)
    dim_gender_task = create_sql_task('transform_dim_gender', dim_gender_query)
    dim_merchant_task = create_sql_task('transform_dim_merchant', dim_merchant_query)
    fact_transactions_task = create_sql_task('transform_fact_transactions', fact_transactions_query)

    # Define task dependencies
    upload_csvs_task >> gcs_to_bq_load >> check_staging_data
    gcs_to_bq_load >> [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task] >> fact_transactions_task
    
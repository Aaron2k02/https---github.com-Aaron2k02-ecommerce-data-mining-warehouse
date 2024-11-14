# # Standard library imports
# from datetime import datetime  # For handling date and time operations
# import logging  # For logging information during task execution
# import os  # For interacting with the file system

# # Airflow core imports
# from airflow import DAG  # For defining the DAG structure
# from airflow.operators.python import PythonOperator  # For running Python functions as tasks
# from airflow.decorators import task  # For defining tasks using the TaskFlow API

# # Google Cloud providers for Airflow
# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # For uploading local files to Google Cloud Storage
# from airflow.providers.google.cloud.hooks.gcs import GCSHook  # For interacting with Google Cloud Storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator  # For BigQuery dataset and SQL job operations
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # For interacting with BigQuery

# # Data manipulation and file handling
# import pandas as pd  # For data manipulation using DataFrames
# from io import StringIO  # For handling in-memory text streams

# # Define constants for GCS and BigQuery configurations
# COMPOSER_BUCKET_NAME = 'us-central1-ecommerce-data--9be224af-bucket'  # GCS bucket name for storing data
# BUCKET_NAME = 'u2102810_ecom_dataset'  # GCS bucket name for storing data
# GCS_PATH = 'raw'  # GCS folder path for raw data files
# # COMPOSER_GCS_CONN_ID = 'google_cloud_default'  # Airflow connection ID for GCS
# GCS_CONN_ID = 'data_warehouse'  # Airflow connection ID for GCS
# BIGQUERY_DATASET = 'ecom_stg'  # BigQuery staging dataset name
# BIGQUERY_DATASET_PROD = 'ecom_prod'  # BigQuery production dataset name
# PROJECT_ID = 'data-mining-warehouse-ecom'  # GCP project ID

# # Helper function to read SQL files for transformations
# def read_sql_file_from_gcs(bucket_name, file_path):
#     """
#     Read SQL query from a file in GCS.

#     Args:
#         bucket_name (str): GCS bucket name.
#         file_path (str): GCS path to the SQL file.
    
#     Returns:
#         str: The SQL query as a string.
#     """
#     try:
#         gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
#         logging.info(f"Reading SQL file from GCS: bucket={bucket_name}, path={file_path}")
#         file_data = gcs_hook.download(bucket_name=bucket_name, object_name=file_path)
#         sql_query = file_data.decode('utf-8')
#         logging.info(f"Successfully read SQL file: {file_path}")
#         return sql_query
#     except Exception as e:
#         logging.error(f"Failed to read SQL file from GCS: bucket={bucket_name}, path={file_path}")
#         logging.error(str(e))
#         raise

# # Load SQL queries for data transformations
# # Load SQL queries for data transformations
# dim_cities_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/dim_cities_query.sql')
# dim_customer_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/dim_customer_query.sql')
# dim_date_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/dim_date_query.sql')
# dim_gender_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/dim_gender_query.sql')
# dim_merchant_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/dim_merchant_query.sql')
# fact_transactions_query = read_sql_file_from_gcs(COMPOSER_BUCKET_NAME, 'transform/fact_transactions_query.sql')

# # Define the Python function to upload files to GCS
# def upload_to_gcs(data_folder, gcs_path, **kwargs):
#     """
#     Upload CSV files from a local folder to Google Cloud Storage.

#     Args:
#         data_folder (str): Local folder containing the CSV files.
#         gcs_path (str): GCS folder path to upload files to.
#     """
#     csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

#     # Upload each CSV file to GCS
#     for csv_file in csv_files:
#         file_path = os.path.join(data_folder, csv_file)
#         gcs_file_path = f"{gcs_path}/{csv_file}"

#         upload_task = LocalFilesystemToGCSOperator(
#             task_id=f'upload_{csv_file.split(".")[0]}_to_gcs',
#             src=file_path,
#             dst=gcs_file_path,
#             bucket=BUCKET_NAME,
#             gcp_conn_id=GCS_CONN_ID,
#             mime_type='text/csv'
#         )
#         upload_task.execute(context=kwargs)  # Executes each upload task\
    
# def load_data_to_bq(**kwargs):
#         """
#         Load CSV data from GCS to BigQuery tables.

#         This function downloads files from GCS, transforms them, and loads them into BigQuery.
#         """
#         gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
#         bq_hook = BigQueryHook(gcp_conn_id=GCS_CONN_ID, use_legacy_sql=False)

#         # List files in the specified GCS bucket and path
#         files = gcs_hook.list(bucket_name=BUCKET_NAME, prefix=GCS_PATH + '/')
#         logging.info(f"Files in GCS: {files}")

#         for file_path in files:
#             file_name = file_path.split('/')[-1].replace('.csv', '').lower()
            
#             # Download the file from GCS to transform column names
#             file_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=file_path)
            
#              # Decode bytes to string
#             decoded_file_data = file_data.decode('utf-8')
            
#             # Load into DataFrame
#             df = pd.read_csv(StringIO(decoded_file_data))

#             # Rename columns to a consistent naming convention
#             df.columns = [col.lower().replace(".", "_").replace(" ", "_").replace("(", "_").replace(")", "") for col in df.columns]
            
#             logging.info(f"Files in GCS column name: {df.columns}")

#             # Upload the transformed file back to GCS as a temporary file
#             temp_file_path = f"{GCS_PATH}/temp_{file_name}.csv"
#             temp_data = df.to_csv(index=False)
#             gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=temp_file_path, data=temp_data)

#             # Define job configuration for BigQuery
#             job_config = {
#                 "sourceUris": [f"gs://{BUCKET_NAME}/{temp_file_path}"],
#                 "destinationTable": {
#                     "projectId": "data-mining-warehouse-ecom",
#                     "datasetId": BIGQUERY_DATASET,
#                     "tableId": file_name,
#                 },
#                 "sourceFormat": "CSV",
#                 "skipLeadingRows": 1,
#                 "writeDisposition": "WRITE_TRUNCATE",
#                 "autodetect": True  
#             }

#             try:
#                 # Execute the BigQuery load job
#                 bq_hook.insert_job(
#                     project_id="data-mining-warehouse-ecom",
#                     configuration={"load": job_config}
#                 )
#                 logging.info(f"Successfully loaded {file_name} into BigQuery")
#             except Exception as e:
#                 logging.error(f"Error loading {file_name} into BigQuery: {e}")
#                 raise
#             finally:
#                 # Remove temporary file from GCS
#                 gcs_hook.delete(bucket_name=BUCKET_NAME, object_name=temp_file_path)

# # Define an independent Python function to run in an external environment
# @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
# def check_staging_data_soda(scan_name='check_staging_data_soda', checks_subpath='sources'):
#     from include.soda.check_function import check

#    # Ensure the check function returns a JSON-serializable result
#     result = check(scan_name, checks_subpath)
#     return result.get("result_code")  # Return only the result code for simplicity

# # Helper function to create transformation SQL task
# def create_sql_task(task_id: str, sql_query: str):
#     """
#     Create a BigQueryInsertJobOperator task for SQL transformations.

#     Args:
#         task_id (str): Unique ID for the task.
#         sql_query (str): SQL query to execute.
#     """
#     return BigQueryInsertJobOperator(
#         task_id=task_id,
#         location="US",
#         project_id=PROJECT_ID,
#         gcp_conn_id=GCS_CONN_ID,
#         configuration={
#             "query": {
#                 "query": sql_query,
#                 "useLegacySql": False,
#                 "defaultDataset": {"projectId": PROJECT_ID, "datasetId": BIGQUERY_DATASET_PROD},
#                 # "writeDisposition": "WRITE_TRUNCATE",
#             }
#         },
#     )

# # DAG Definition
# with DAG(
#     'cloud_ecommerce_data_pipeline',
#     start_date=datetime(2024, 11, 1),
#     schedule_interval=None,  # None for manual triggering
#     catchup=False,
#     tags=['ecommerce', 'data_pipeline'],
#     description='ETL pipeline for e-commerce data',
# ) as dag:
    
#     # Task to upload CSV files to GCS
#     # upload_csvs_task = PythonOperator(
#     #     task_id='upload_all_csvs_to_gcs',
#     #     python_callable=upload_to_gcs,
#     #     op_args=['gs://us-central1-ecommerce-data--9be224af-bucket/data', 'raw'],  # Local data folder and GCS path
#     #     provide_context=True,
#     # )
    
#     # Task to create an empty BigQuery dataset no need since the load data to bigquery can auto detect the schema
#     # create_ecom_dataset = BigQueryCreateEmptyDatasetOperator(
#     #     task_id='create_ecom_stg_dataset',
#     #     dataset_id=BIGQUERY_DATASET,  # Name of the dataset in BigQuery
#     #     gcp_conn_id='data_warehouse',
#     # )
    
#     # Task to load files from GCS to BigQuery
#     gcs_to_bq_load_task = PythonOperator(
#         task_id='load_data_to_bq',
#         python_callable=load_data_to_bq,
#         provide_context=True,
#     )
    
#     # Task to load files from GCS to BigQuery
#     # check_staging_data_task = PythonOperator(
#     #     task_id='check_staging_data_soda',
#     #     python_callable=check_staging_data_soda,
#     #     provide_context=True,
#     #     do_xcom_push=False  # Prevents result from being pushed to XCom
#     # )
    
#     # # Create tasks for each SQL transformation
#     dim_cities_task = create_sql_task('transform_dim_cities', dim_cities_query)
#     dim_customer_task = create_sql_task('transform_dim_customer', dim_customer_query)
#     dim_date_task = create_sql_task('transform_dim_date', dim_date_query)
#     dim_gender_task = create_sql_task('transform_dim_gender', dim_gender_query)
#     dim_merchant_task = create_sql_task('transform_dim_merchant', dim_merchant_query)
#     fact_transactions_task = create_sql_task('transform_fact_transactions', fact_transactions_query)

#     # Task Dependencies
#     # upload_csvs_task >> gcs_to_bq_load_task 
#     gcs_to_bq_load_task >> [dim_cities_task, dim_customer_task, dim_date_task, dim_gender_task, dim_merchant_task] >> fact_transactions_task
    
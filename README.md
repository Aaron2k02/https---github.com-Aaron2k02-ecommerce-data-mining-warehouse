Here’s an updated README based on your provided details, incorporating the most important information:

---

# ELT Data Pipeline with Google Cloud Platform

This project demonstrates an **ELT (Extract, Load, Transform)** data pipeline designed for real-time analytics using **Google Cloud Platform (GCP)** services. The pipeline showcases a scalable, serverless architecture for transforming raw data into a **star schema** for analytical purposes, while emphasizing **data governance and security**.

---

## Overview

### Key Features:
![Data Mining - Page 1 (16)](https://github.com/user-attachments/assets/ebd6f23b-637f-49a1-a97f-b56770d80e8e)
- **Data Lakehouse Architecture**: Combines the scalability of a data lake with the structure of a data warehouse.
![image](https://github.com/user-attachments/assets/98db7f23-6b39-476d-b683-ee33ef96217b)
- **Real-Time Data Ingestion**: Simulates transactions using **Python Faker** and **Google Pub/Sub**.
![image](https://github.com/user-attachments/assets/c6d6324c-2dd4-4856-9dda-78422c25dfea)
![image](https://github.com/user-attachments/assets/32d4d7f6-9847-47a6-9cf9-0109fa379837)
- **Automated Orchestration**: Uses **Google Cloud Composer** with **Apache Airflow** to manage workflows.
![image](https://github.com/user-attachments/assets/03e6cf30-e3b2-43cd-b078-6dbbece73f47)
- **Star Schema Modeling**: Efficiently structures data for analytics in **BigQuery**.
![image](https://github.com/user-attachments/assets/7bfea2cc-c89a-44b2-9d1d-600e7d7e7012)
RBAC with GCP IAM
![image](https://github.com/user-attachments/assets/745b9876-bcf6-42da-a7bb-f09507a42639)
RLS Implementation in GCP IAM
![image](https://github.com/user-attachments/assets/6c1e4ee3-d191-4a9f-a3f9-fe79caa88feb)
RLS In Action
- **Governance and Security**: Implements **Role-Based Access Control (RBAC)** using **Google IAM** with compliance to **GDPR** and **PDPA**.

---

## Pipeline Architecture
![image](https://github.com/user-attachments/assets/8e82021c-92ce-4741-84e2-44df52693af7)
### 1. Extract Phase
- **Data Simulation**: Synthetic transaction data is generated using Python Faker.
- **Streaming**: Data is published to **Google Pub/Sub** and stored in **Google Cloud Storage (GCS)** as CSV files.

![image](https://github.com/user-attachments/assets/466fc367-e0cb-4e44-b99e-c316b5f408f4)
### 2. Load Phase
- **Staging Tables**: Data is loaded from GCS into **BigQuery** for preliminary organization and validation.

![image](https://github.com/user-attachments/assets/aac4b5a3-767f-411c-afe9-18cdc117ae61)
### 3. Transform Phase
- **SQL Transformations**: SQL scripts stored in GCS are executed in BigQuery via **Airflow’s BigQuery Operators** to create a **star schema** in the production dataset.
  - **Fact Table**: Transactions.
  - **Dimension Tables**: Cities, Customers, Dates, Genders, and Merchants.

#### Example Python Code for SQL Tasks:
```python
def read_sql_file_from_gcs(bucket_name, file_path):
    """
    Read SQL query from a file in GCS.
    """
    gcs_hook = GCSHook(gcp_conn_id="gcs_conn_id")
    file_data = gcs_hook.download(bucket_name=bucket_name, object_name=file_path)
    return file_data.decode('utf-8')

def create_sql_task(task_id, sql_query):
    """
    Create a BigQuery SQL task in Airflow.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location="US",
        project_id="project_id",
        gcp_conn_id="gcs_conn_id",
        configuration={"query": {"query": sql_query, "useLegacySql": False}},
    )
```

### Result:
The transformation process produces a **star schema** that integrates fact and dimension tables, enabling efficient analytical queries and reporting.

---

## Data Governance and Security

### Privacy Regulation Compliance
The pipeline aligns with GDPR and PDPA principles to ensure:
- **User Consent**: Sensitive data handling requires explicit consent.
- **Right to Access and Erasure**: Enables users to view and delete their data upon request.
- **Role-Based Access Control (RBAC)**: Implements fine-grained permissions using **Google IAM**:
  - **Owner**: Full access to datasets (e.g., `cheron2k02@gmail.com`).
  - **Viewer**: Limited access for querying data (e.g., `u2102810@siswa.um.edu.my`).

### Current Implementations
- **Google IAM**: Configures roles such as Owner, Data Viewer, and BigQuery Admin for secure and restricted access to datasets.
- **BigQuery Logging**: Tracks user activity to ensure transparency and compliance.

---

## Technologies Used

- **Google Cloud Platform**:
  - Pub/Sub
  - Cloud Storage
  - BigQuery
  - Cloud Composer (Airflow)
- **Python**:
  - Faker Library
  - Google Cloud SDK
- **Analytics**:
  - Power BI

---

## Results and Analytics

### Pipeline Execution:
- Seamless workflow management via **Airflow DAGs**.
- SQL transformations orchestrated in **Cloud Composer**.

### Data Modeling:
- **Production Dataset**: Star schema with fact and dimension tables in BigQuery.

### Visualization:
- Interactive dashboards in **Power BI** showcasing actionable insights from the transformed data.
Customer Centric Dashboard
![image](https://github.com/user-attachments/assets/9d5a12e1-77ff-4cf4-afba-ee94e7bda031)

City Operational Efficiency Analysis Dashboard
![image](https://github.com/user-attachments/assets/60c2e846-d79f-49aa-8475-07e68ebf4aa9)

Merchant Product Performance and Sales Dashboard
![image](https://github.com/user-attachments/assets/f99784b3-9678-498f-9b4c-6fc178e6d2f9)

---

## Conclusion

This project demonstrates a modern approach to building scalable and secure ELT pipelines for real-time data processing and analytics. It highlights expertise in:
- **Data Warehousing**: Designing star schemas for optimal querying.
- **Cloud Orchestration**: Automating workflows with Airflow and Composer.
- **Governance**: Ensuring data privacy and security with robust policies.

---

Feel free to explore the code and provide feedback! 😊

### [View Full Documentation](https://docs.google.com/document/d/1D_wBVQw9YrQJYOZM-uuOSCrAjlNqk_sK2wSJHXJ8sD0/edit?usp=sharing)

--- 

Let me know if you need additional refinements!

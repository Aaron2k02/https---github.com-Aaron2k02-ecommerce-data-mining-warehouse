---

# ELT Data Pipeline with Google Cloud Platform  

This project demonstrates a scalable **ELT (Extract, Load, Transform)** data pipeline for real-time analytics using **Google Cloud Platform (GCP)** services. It emphasizes a serverless architecture, **data governance**, and **security** while transforming raw data into a **star schema** for analytical purposes.

---

## Overview  

### Key Features  
- **Data Lakehouse Architecture**: Combines the scalability of data lakes with the structure of data warehouses.  
![Data Lakehouse](https://github.com/user-attachments/assets/5a433a67-0ef6-4c2b-822f-8543e7dd6234)  

- **Real-Time Data Ingestion**: Simulates transactions using **Python Faker** and **Google Pub/Sub**.  
![Real-Time Ingestion](https://github.com/user-attachments/assets/c6d6324c-2dd4-4856-9dda-78422c25dfea)  
![Pub/Sub Pipeline](https://github.com/user-attachments/assets/32d4d7f6-9847-47a6-9cf9-0109fa379837)  

- **Automated Orchestration**: Leverages **Google Cloud Composer** and **Apache Airflow** for workflow management.  
![image](https://github.com/user-attachments/assets/30f1ddba-e662-40a0-bac6-fdf02e296fc3)

- **Star Schema Modeling**: Structures data for analytics in **BigQuery** with fact and dimension tables.  
![Orchestration](https://github.com/user-attachments/assets/03e6cf30-e3b2-43cd-b078-6dbbece73f47)  

- **Governance and Security**: Implements **Role-Based Access Control (RBAC)** and **Row-Level Security (RLS)** via **Google IAM**.
![Star Schema](https://github.com/user-attachments/assets/7bfea2cc-c89a-44b2-9d1d-600e7d7e7012)
![RBAC](https://github.com/user-attachments/assets/745b9876-bcf6-42da-a7bb-f09507a42639)  
![RLS Example](https://github.com/user-attachments/assets/6c1e4ee3-d191-4a9f-a3f9-fe79caa88feb)  

---

## Pipeline Architecture  

### 1. Extract Phase  
- **Data Simulation**: Generates synthetic transaction data with Python Faker.  
- **Streaming**: Publishes data to **Google Pub/Sub** and stores it in **Google Cloud Storage (GCS)** as CSV files.  
![Extract Phase](https://github.com/user-attachments/assets/8e82021c-92ce-4741-84e2-44df52693af7)  

### 2. Load Phase  
- **Staging Tables**: Loads data from GCS to **BigQuery** for validation and initial organization.  
![Load Phase](https://github.com/user-attachments/assets/466fc367-e0cb-4e44-b99e-c316b5f408f4)  

### 3. Transform Phase  
- **SQL Transformations**: Executes scripts stored in GCS using Airflow’s BigQuery Operators, creating a **star schema** with:  
  - **Fact Table**: Transactions  
  - **Dimension Tables**: Cities, Customers, Dates, Genders, and Merchants  

![Transform Phase](https://github.com/user-attachments/assets/aac4b5a3-767f-411c-afe9-18cdc117ae61)  

#### Example Code:  
```python
def read_sql_file_from_gcs(bucket_name, file_path):
    """
    Read SQL query from GCS file.
    """
    gcs_hook = GCSHook(gcp_conn_id="gcs_conn_id")
    file_data = gcs_hook.download(bucket_name=bucket_name, object_name=file_path)
    return file_data.decode('utf-8')

def create_sql_task(task_id, sql_query):
    """
    Create a BigQuery SQL task.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location="US",
        project_id="project_id",
        gcp_conn_id="gcs_conn_id",
        configuration={"query": {"query": sql_query, "useLegacySql": False}},
    )
```

---

## Data Governance and Security  

### Compliance with Privacy Regulations:  
The pipeline adheres to **GDPR** and **PDPA** principles, ensuring:  
- **User Consent**: Handles sensitive data only with explicit permission.  
- **Right to Access/Erasure**: Facilitates user data requests.  

### Access Controls:  
- **Role-Based Access Control (RBAC)**: Implements Google IAM roles for granular data permissions.  
  - **Owner**: Full access (e.g., `cheron2k02@gmail.com`)  
  - **Viewer**: Restricted querying rights (e.g., `u2102810@siswa.um.edu.my`)  
- **Activity Logging**: Tracks user actions via **BigQuery logs**.  

---

## Results and Analytics  

### Pipeline Execution:  
- Workflows are seamlessly managed through Airflow DAGs.  
- SQL transformations are efficiently orchestrated in Composer.  

### Data Modeling:  
- The **star schema** in BigQuery enables optimized analytical queries.  

### Interactive Dashboards:  
Visualized in **Power BI**:  
- **Customer-Centric Dashboard**:  
  ![Dashboard](https://github.com/user-attachments/assets/9d5a12e1-77ff-4cf4-afba-ee94e7bda031)  
- **Operational Efficiency Analysis**:  
  ![Dashboard](https://github.com/user-attachments/assets/60c2e846-d79f-49aa-8475-07e68ebf4aa9)  
- **Merchant Product Performance**:  
  ![Dashboard](https://github.com/user-attachments/assets/f99784b3-9678-498f-9b4c-6fc178e6d2f9)  

---

## Conclusion  

This project highlights a modern, scalable approach to building secure ELT pipelines, demonstrating expertise in:  
- **Data Warehousing**: Star schema modeling for efficient analytics.  
- **Cloud Orchestration**: Automated workflows with Composer and Airflow.  
- **Data Governance**: Robust security and privacy policies.  

---

### [View Full Documentation](https://docs.google.com/document/d/1D_wBVQw9YrQJYOZM-uuOSCrAjlNqk_sK2wSJHXJ8sD0/edit?usp=sharing)

---

Let me know how else you'd like to refine this! 😊

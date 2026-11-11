# Telecom Churn ETL Pipeline

A comprehensive Data Engineering project implementing an ETL (Extract, Transform, Load) pipeline for telecommunications data, utilizing the Medallion Architecture (from the Bronze to the Silver layer). Orchestration is handled via Apache Airflow, extracting data from object storage (MinIO) and loading it into a relational database (Microsoft SQL Server).

## Tech Stack

- **Orchestration:** Apache Airflow (Dockerized)
- **Source Storage (Bronze):** MinIO (S3-compatible Object Storage)
- **Data Warehouse (Silver):** Microsoft SQL Server (SSMS)
- **Processing & Transformations:** Python (pandas, SQLAlchemy, PyMSSQL)

## DAG Workflow Architecture

The pipeline runs on a monthly schedule interval and consists of three main tasks:

1. **`extract_data`:**
   * Establishes a connection to MinIO using the `S3Hook`.
   * Reads the raw `telecom_churn.csv` file (Bronze Layer) directly into memory.
   * Applies initial data quality filtering (stripping whitespace from IDs, casting to integers, and removing duplicates via `drop_duplicates`).
   * Filters the dataset to strictly keep data for the current month (based on Airflow's `logical_date` / `execution_date`).

2. **`clean_data`:**
   * Cleans and standardizes columns (e.g., homogenizing the gender field).
   * Prepares the final DataFrame for database ingestion.

3. **`load_data_ssms`:**
   * **Idempotency Guarantee:** Prior to insertion, it executes a `DELETE` statement in SQL Server (`autocommit=True`) for the specific reference month, preventing duplicate entries in case of DAG reruns.
   * Bulk loads the cleaned data into the `silver.telecom_churn` table using pandas' `to_sql` method and the SQLAlchemy engine.

## Key Engineering Concepts

- **Idempotent Pipeline:** The workflow is designed to run multiple times for the same month without violating data integrity (Primary Key constraints).
- **Data Quality Management:** Automatic detection and removal of duplicate `customer_id`s at the Python level, before causing `IntegrityError` exceptions in the database.
- **In-Memory Processing:** Direct data reading from MinIO via `io.StringIO`, without creating intermediate local files.

## Setup & Execution Instructions

**1. Clone the repository:**
```bash
git clone https://github.com/bremerus/airflow_ETL_telecom.git
cd airflow_ETL_telecom
```

**2. Start Services:**
Ensure Docker is running on your system and execute:
```bash
docker-compose up -d
```

**3. Airflow Configuration (Connections):**
From the Airflow UI (usually `http://localhost:8080`), configure the following connections:
- `minio_conn`: Type `Amazon S3` (add credentials for your MinIO bucket).
- `my_mssql_connection`: Type `Microsoft SQL Server` (Host: `host.docker.internal`, Port: `1433`, along with the respective database credentials).

**4. Execute the DAG:**
Enable the `dag_etl_v...` DAG from the UI and monitor the progress of the tasks through the Graph or Grid view.

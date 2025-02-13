import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
CSV_FILE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
PART_FILE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PART_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv.gz', '.parquet')
TABLE_NAME_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}'
TRANSFORMED_TABLE_NAME_TEMPLATE = 'fhv_tripdata_transformed_{{ execution_date.strftime(\'%Y_%m\') }}'
MASTER_TABLE_NAME = 'fhv_tripdata_master'

# dataset_file = "fhv_tripdata_2020-01.csv.gz"
# dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv.gz', '.parquet')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv.gz'):
        logging.error(f"Invalid file format: {src_file}")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_yl_19_20_gcs_dag",
    schedule_interval="0 6 1 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 5),
    tags=["dtc-de"],
) as dag:
    
    download_dataset_task = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl -L -sS {URL_TEMPLATE} -o {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={"src_file": OUTPUT_FILE_TEMPLATE},
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PART_FILE}",
            "local_file": PART_FILE_TEMPLATE,
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_NAME_TEMPLATE
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{PART_FILE}"],
                "autodetect": True,
            },
        },
    )

    bigquery_transform_task = BigQueryInsertJobOperator(
    task_id="bigquery_transform",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TRANSFORMED_TABLE_NAME_TEMPLATE}` AS
                SELECT 
                    '{CSV_FILE}' AS file_name,
                    *                    
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME_TEMPLATE}`
            """,
            "useLegacySql": False,
        }
    },
    )

    create_master_table_task = BigQueryInsertJobOperator(
        task_id="create_master_table",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{MASTER_TABLE_NAME}` (
                        file_name STRING,
                        VendorID INT64,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count INT64,
                        trip_distance FLOAT64,
                        RatecodeID INT64,
                        store_and_fwd_flag STRING,
                        PULocationID INT64,
                        DOLocationID INT64,
                        payment_type INT64,
                        fare_amount FLOAT64,
                        extra FLOAT64,
                        mta_tax FLOAT64,
                        tip_amount FLOAT64,
                        tolls_amount FLOAT64,
                        improvement_surcharge FLOAT64,
                        total_amount FLOAT64,
                        congestion_surcharge FLOAT64
                    )
                """,
                "useLegacySql": False,
            }
        },
    )
    # merge is not used here since no unique key is identified in the fhv taxi data.
    # bigquery_merge_task = BigQueryInsertJobOperator(
    # task_id="bigquery_merge",
    # configuration={
    #     "query": {
    #         "query": f"""
    #             MERGE `{PROJECT_ID}.{BIGQUERY_DATASET}.{MASTER_TABLE_NAME}` AS master
    #             USING `{PROJECT_ID}.{BIGQUERY_DATASET}.{TRANSFORMED_TABLE_NAME_TEMPLATE}` AS transformed
    #             ON master.unique_id = transformed.unique_id
    #             WHEN MATCHED THEN
    #             UPDATE SET
    #                 master.file_name = transformed.file_name,
    #                 master.VendorID = transformed.VendorID,
    #                 master.tpep_pickup_datetime = transformed.tpep_pickup_datetime,
    #                 master.tpep_dropoff_datetime = transformed.tpep_dropoff_datetime,
    #                 master.passenger_count = transformed.passenger_count,
    #                 master.trip_distance = transformed.trip_distance,
    #                 master.RatecodeID = transformed.RatecodeID,
    #                 master.store_and_fwd_flag = transformed.store_and_fwd_flag,
    #                 master.PULocationID = transformed.PULocationID,
    #                 master.DOLocationID = transformed.DOLocationID,
    #                 master.payment_type = transformed.payment_type,
    #                 master.fare_amount = transformed.fare_amount,
    #                 master.extra = transformed.extra,
    #                 master.mta_tax = transformed.mta_tax,
    #                 master.tip_amount = transformed.tip_amount,
    #                 master.tolls_amount = transformed.tolls_amount,
    #                 master.improvement_surcharge = transformed.improvement_surcharge,
    #                 master.total_amount = transformed.total_amount,
    #                 master.congestion_surcharge = transformed.congestion_surcharge

    #             WHEN NOT MATCHED THEN
    #             INSERT (
    #                 unique_id,
    #                 file_name,
    #                 VendorID,
    #                 tpep_pickup_datetime,
    #                 tpep_dropoff_datetime,
    #                 passenger_count,
    #                 trip_distance,
    #                 RatecodeID,
    #                 store_and_fwd_flag,
    #                 PULocationID,
    #                 DOLocationID,
    #                 payment_type,
    #                 fare_amount,
    #                 extra,
    #                 mta_tax,
    #                 tip_amount,
    #                 tolls_amount,
    #                 improvement_surcharge,
    #                 total_amount,
    #                 congestion_surcharge
    #             ) 
    #             VALUES (
    #                 transformed.unique_id,
    #                 transformed.file_name,
    #                 transformed.VendorID,
    #                 transformed.tpep_pickup_datetime,
    #                 transformed.tpep_dropoff_datetime,
    #                 transformed.passenger_count,
    #                 transformed.trip_distance,
    #                 transformed.RatecodeID,
    #                 transformed.store_and_fwd_flag,
    #                 transformed.PULocationID,
    #                 transformed.DOLocationID,
    #                 transformed.payment_type,
    #                 transformed.fare_amount,
    #                 transformed.extra,
    #                 transformed.mta_tax,
    #                 transformed.tip_amount,
    #                 transformed.tolls_amount,
    #                 transformed.improvement_surcharge,
    #                 transformed.total_amount,
    #                 transformed.congestion_surcharge
    #                 )
    #         """,
    #         "useLegacySql": False,
    #     }
    # },
    # )
    load_monthly_data_task = BigQueryInsertJobOperator(
        task_id="load_monthly_data_to_master",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{MASTER_TABLE_NAME}`
                    SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{TRANSFORMED_TABLE_NAME_TEMPLATE}`
                """,
                "useLegacySql": False,
            }
        },
    )

    remove_flat_files_task = BashOperator(
        task_id="remove_flat_files",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE} {PART_FILE_TEMPLATE}"
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
        >> bigquery_transform_task
        >> create_master_table_task
        >> load_monthly_data_task
        >> remove_flat_files_task
    )
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
CSV_FILE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
PART_FILE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PART_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv.gz', '.parquet')
TABLE_NAME_TEMPLATE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}'


# dataset_file = "yellow_tripdata_2020-01.csv.gz"
# dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
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
    end_date=datetime(2019, 1, 5),
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

    remove_flat_files_task = BashOperator(
        task_id="remove_flat_files",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE} {PART_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> remove_flat_files_task

import os
from airflow import DAG
from datetime import datetime
from ingest_script import ingest_callable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PG_HOST= os.environ.get("PG_HOST")
PG_PORT= os.environ.get("PG_PORT")
PG_USER= os.environ.get("PG_USER")
PG_PASSWORD= os.environ.get("PG_PASSWORD")
PG_DATABASE= os.environ.get("PG_DATABASE")


local_wofklow_dag = DAG(
    "localIngestionDag"
)

local_wofklow_dag = DAG(
    "localIngestionDag",
    schedule_interval="0 6 1 * *",
    ##default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de"],
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 1, 5)
)


URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_wofklow_dag:

    wget_task = BashOperator(
        task_id="wget",
        bash_command=f'curl -L -sS {URL_TEMPLATE} -o {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    wget_task >> ingest_task
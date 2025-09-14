import os
from datetime import datetime
from airflow.sdk import task, dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from ingest_data_script import ingest_func


AIRFLOW_HOME=os.environ.get("AIRFLOW_HOME", "opt/airflow/")
# url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-06.csv.gz"


URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv.gz"
CSV_NAME = "output_yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv"
TABLE_NAME_TEMPLATE= "yellow_taxi_data_2020_" + "{{ logical_date.strftime(\'%m\') }}"


PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_HOST=os.getenv('PG_HOST')
PG_PORT=os.getenv('PG_PORT')
PG_DB=os.getenv('PG_DB')


@dag(
    dag_id="local_ingestion_dag",
    schedule="0 0 2 * *",
    start_date=datetime(2021, 1, 1)
)
def local_ingestion_dag():

    curl_task = BashOperator(
        task_id="curl",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    unzip_task = BashOperator(
        task_id="unzip",
        bash_command=f"gunzip -kf {OUTPUT_FILE_TEMPLATE}"
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_func,
        op_kwargs={
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST, 
            "port": PG_PORT,
            "db": PG_DB,
            "table_name": TABLE_NAME_TEMPLATE,
            "csv_name": CSV_NAME,
        }
    )

    curl_task >> unzip_task >> ingest_task

local_ingestion_dag()
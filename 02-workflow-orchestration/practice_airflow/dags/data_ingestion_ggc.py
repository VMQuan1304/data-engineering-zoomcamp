import os
from datetime import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryGetDatasetOperator
from ingest_data_ggc_script import convert_csv_to_parquet_callable

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator





AIRFLOW_HOME=os.environ.get("AIRFLOW_HOME", "opt/airflow/")
# url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-06.csv.gz"

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv.gz"
TABLE_NAME_TEMPLATE= "yellow_taxi_data_2020_" + "{{ logical_date.strftime(\'%m\') }}"


CSV_NAME = "output_yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".csv"
PARQUET_NAME = "output_yellow_tripdata_2020-" + "{{ logical_date.strftime(\'%m\') }}" + ".parquet"
    

@dag(
    dag_id="data_ingestion_ggc_dag",
    schedule="0 0 2 * *",
    start_date=datetime(2021, 1, 1)
)
def data_ingestion_ggc():
  
    curl_task = BashOperator(
        task_id="curl",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    unzip_task = BashOperator(
        task_id="unzip",
        bash_command=f"gunzip -kf {OUTPUT_FILE_TEMPLATE}"
    )

    convert_csv_to_parquet_task = PythonOperator(
        task_id="convert",
        python_callable=convert_csv_to_parquet_callable,
        op_kwargs={
            "csv_name": CSV_NAME,
            "parquet_name": PARQUET_NAME,
        }
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=f"{AIRFLOW_HOME}/{PARQUET_NAME}",
        dst=f"data/{PARQUET_NAME}",
        bucket="data-470504-demo-bucket",
        gcp_conn_id="ggc_bigquery",  # your Airflow GCP connection
    )

    create_bigquery_table_task = GCSToBigQueryOperator(
        task_id="create_table",
        bucket="data-470504-demo-bucket",
        source_objects=[f"data/*.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="data-470504.demo_dataset.yellow_taxidata_table",
        gcp_conn_id="ggc_bigquery",
        write_disposition="WRITE_TRUNCATE",
        # force_delete=True,
    ) 

    curl_task >> unzip_task >> convert_csv_to_parquet_task >>  upload_to_gcs_task >> create_bigquery_table_task

data_ingestion_ggc()
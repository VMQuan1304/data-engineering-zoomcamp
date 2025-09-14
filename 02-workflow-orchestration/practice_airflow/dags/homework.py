import os
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


fhv_file_name = "fhv_tripdata_2025-01.parquet"

@dag(
    dag_id="FHV",
    schedule="*/15 1 * * *"
)
def fhv_data_ingestion():

    @task(task_id="curl_task")
    def download_task():
        os.system(f"curl -ssL https://d37ci6vzurychx.cloudfront.net/trip-data/{fhv_file_name} > /opt/airflow/fhv/{fhv_file_name}")

    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=f"fhv/{fhv_file_name}",
        dst=f"fhv/{fhv_file_name}",
        bucket="data-470504-demo-bucket",
        gcp_conn_id="ggc_bigquery",  # your Airflow GCP connection
    )

    download_task() >> upload_task

fhv_data_ingestion()
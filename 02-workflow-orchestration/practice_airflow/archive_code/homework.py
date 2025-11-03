import os
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


AIRFLOW_HOME=os.environ.get("AIRFLOW_HOME", "opt/airflow/")
fhv_file_name = "fhv_tripdata_2025-08.parquet"

@dag(
    dag_id="FHV",
    schedule="*/15 1 * * *"
)
def fhv_data_ingestion():

    @task(task_id="curl_task")
    def download_task():
        os.system(f"mkdir -p {AIRFLOW_HOME}/fhv")
        os.system(f"curl -ssL https://d37ci6vzurychx.cloudfront.net/trip-data/{fhv_file_name} > {AIRFLOW_HOME}/fhv/{fhv_file_name}")
        os.system(f"wc -l fhv/{fhv_file_name}")

    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=f"fhv/{fhv_file_name}",
        dst=f"fhv/{fhv_file_name}",
        bucket="data-470504-demo-bucket",
        gcp_conn_id="ggc_bigquery",  # your Airflow GCP connection
    )

    create_bigquery_table_task = GCSToBigQueryOperator(
        task_id="create_table",
        bucket="data-470504-demo-bucket",
        source_objects=["fhv/*.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="data-470504.demo_dataset.fhv_table",
        gcp_conn_id="ggc_bigquery",
        write_disposition="WRITE_TRUNCATE",
        # force_delete=True,
    ) 

    download_task() >> upload_task >> create_bigquery_table_task

fhv_data_ingestion()
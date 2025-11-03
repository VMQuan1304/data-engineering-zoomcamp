import os
import logging
from datetime import datetime, timedelta
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG

dataset_yellow_trip = "yellow_tripdata_2020-{{ data_interval_start.strftime(\'%m\') }}.csv.gz"
csv_file = dataset_yellow_trip.replace(".gz", "")
parquet_file = csv_file.replace("csv", "parquet")
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_yellow_trip}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")


def convert_csv_to_parquet(csv_path, parquet_path):
    if not csv_path.endswith(".csv"):
        logging.error("System only support csv files")
        return
    table = pv.read_csv(csv_path)
    pq.write_table(table, parquet_path)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


with DAG(
    "gg_cloud_pipeline",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="load_data_to_ggcloud",
    # schedule=timedelta(days=1),
    schedule="0 6 2 * *",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    task1_download_data = BashOperator(
        task_id="download_data",
        bash_command=f"""
        curl -sSL {dataset_url} > {path_to_local_home}/{dataset_yellow_trip}
        gunzip -kf {path_to_local_home}/{dataset_yellow_trip}
        """
    )

    task2_convert_data_to_parquet = PythonOperator(
        task_id="convert_data_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "csv_path":f"{path_to_local_home}/{csv_file}",
            "parquet_path":f"{path_to_local_home}/{parquet_file}"
        }
    )

    task3_upload_data_to_gcs = PythonOperator(
        task_id="upload_data_to_gcs",
        python_callable=upload_blob,
        op_kwargs={
            "bucket_name":BUCKET_NAME,
            "source_file_name":f"{path_to_local_home}/{parquet_file}",
            "destination_blob_name":f"raw/{parquet_file}"
        }
        )

    task4_create_bigquery_table = GCSToBigQueryOperator(
        task_id="create_bigquery_table",
        bucket="data-470504-demo-bucket",
        source_objects=["raw/yellow_tripdata_*.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="data-470504.demo_dataset.trip_data_all",
        # gcp_conn_id="ggc_bigquery",
        write_disposition="WRITE_TRUNCATE",
        # force_delete=True,
    ) 

    task1_download_data >> task2_convert_data_to_parquet >> task3_upload_data_to_gcs >> task4_create_bigquery_table
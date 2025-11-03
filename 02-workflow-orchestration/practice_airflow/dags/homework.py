import os
import logging
from datetime import datetime, timedelta
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG

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


def donwload_parquetize_upload_dag(
    dag,
    data,
    dataset_url,
    gcs_folder,
    bucket_name,
    is_unzip=True,
):
    csv = data.replace(".gz", "")
    parquet = csv.replace("csv", "parquet")

    unzip_cmd = f"gunzip -kf {path_to_local_home}/{data}" if is_unzip else ""

    with dag:
        task1_download_data = BashOperator(
            task_id="download_data",
            bash_command=f"""
            curl -sSLf {dataset_url} > {path_to_local_home}/{data}
            {unzip_cmd}
            """
        )

        task2_convert_data_to_parquet = PythonOperator(
            task_id="convert_data_to_parquet",
            python_callable=convert_csv_to_parquet,
            op_kwargs={
                "csv_path":f"{path_to_local_home}/{csv}",
                "parquet_path":f"{path_to_local_home}/{parquet}"
            }
        )

        task3_upload_data_to_gcs = PythonOperator(
            task_id="upload_data_to_gcs",
            python_callable=upload_blob,
            op_kwargs={
                "bucket_name":bucket_name,
                "source_file_name":f"{path_to_local_home}/{parquet}",
                "destination_blob_name":f"{gcs_folder}/{parquet}"
            }
            )

        task1_download_data >> task2_convert_data_to_parquet >> task3_upload_data_to_gcs


yellow_trip_data = "yellow_tripdata_2020-{{ data_interval_start.strftime(\'%m\') }}.csv.gz"
yellow_trip_csv = yellow_trip_data.replace(".gz", "")
yellow_trip_parquet = yellow_trip_csv.replace("csv", "parquet")
yellow_trip_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{yellow_trip_data}"
yellow_trip_gcs_folder = "raw/yellow_trip_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


yellow_taxi_data = DAG(
    "yellow_taxi_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="load_data_to_ggcloud",
    schedule="0 6 2 * *",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de"],
)

donwload_parquetize_upload_dag(
    dag=yellow_taxi_data,
    data=yellow_trip_data,
    dataset_url=yellow_trip_dataset_url,
    gcs_folder=yellow_trip_gcs_folder,
    bucket_name=BUCKET_NAME,
)

green_trip_data = "green_tripdata_2020-{{ data_interval_start.strftime(\'%m\') }}.csv.gz"
green_trip_csv = green_trip_data.replace(".gz", "")
green_trip_parquet = green_trip_csv.replace("csv", "parquet")
green_trip_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{green_trip_data}"
green_trip_gcs_folder = "raw/green_trip_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


green_taxi_data = DAG(
    "green_taxi_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="load_data_to_ggcloud",
    schedule="0 6 2 * *",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de"],
)

donwload_parquetize_upload_dag(
    dag=green_taxi_data,
    data=green_trip_data,
    dataset_url=green_trip_dataset_url,
    gcs_folder=green_trip_gcs_folder,
    bucket_name=BUCKET_NAME,
)





fhv_trip_data = "fhv_tripdata_2020-{{ data_interval_start.strftime(\'%m\') }}.csv.gz"
fhv_trip_csv = fhv_trip_data.replace(".gz", "")
fhv_trip_parquet = fhv_trip_csv.replace("csv", "parquet")
fhv_trip_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{fhv_trip_data}"
fhv_trip_gcs_folder = "raw/fhv_trip_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


fhv_taxi_data = DAG(
    "fhv_taxi_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="load_data_to_ggcloud",
    schedule="0 6 2 * *",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de"],
)

donwload_parquetize_upload_dag(
    dag=fhv_taxi_data,
    data=fhv_trip_data,
    dataset_url=fhv_trip_dataset_url,
    gcs_folder=fhv_trip_gcs_folder,
    bucket_name=BUCKET_NAME,
)

zone_data = "taxi_zone_lookup.csv"
zone_csv = fhv_trip_data.replace(".gz", "")
zone_parquet = fhv_trip_csv.replace("csv", "parquet")
zone_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/{zone_data}"
zone_gcs_folder = "raw/zone_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


zone_taxi_data = DAG(
    "zone_taxi_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="load_data_to_ggcloud", schedule="@once",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de"],
)

donwload_parquetize_upload_dag(
    dag=zone_taxi_data,
    data=zone_data,
    dataset_url=zone_dataset_url,
    gcs_folder=zone_gcs_folder,
    bucket_name=BUCKET_NAME,
    is_unzip=False,
)
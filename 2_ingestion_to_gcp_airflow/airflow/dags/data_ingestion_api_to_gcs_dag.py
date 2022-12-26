import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
# PROJECT_ID = "fellowship-7"
# BUCKET = "fellowship-7"

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_API = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
API_RESULT = 'population.json'
path_to_local_home = "/opt/airflow/"
CSV_SAVED = URL_API.replace('.json', '.csv')
PARQUET_FILE = CSV_SAVED.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def call_api(url_json: str, local_json: str):
    r = requests.get(url = url_json)

    data = r.json()
    with open(local_json, 'w') as outfile:
        json.dump(data, outfile)

def csv_saver(json_file: str):
    """Pass json file path url and convert to csv filetype"""
    if not json_file.endswith('.json'):
        logging.error('Can only accept source files in JSON format, for the moment')
        return

    table = jsw.read_json(json_file)
    table_arr = table['data'].to_numpy()

    list_tbl = list(i for i in table_arr[0])
    df = pd.DataFrame.from_dict(list_tbl)
    df_tbl = pa.Table.from_pandas(df)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    call_dataset_task = PythonOperator(
        task_id="call_dataset_task",
        python_callable=call_api,
        op_kwargs={ 
            "url_json": URL_API,
            "local_json": f"{LOCAL_HOME_PATH}/{API_RESULT}"
        }
    )

    save_as_csv = PythonOperator(
        task_id="save_as_csv",
        python_callable=csv_saver,
        op_kwargs={ 
            "json_file": f"{LOCAL_HOME_PATH}/{API_RESULT}",
        }
    )


    # download_dataset_task = BashOperator(
    #     task_id="download_dataset_task",
    #     bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    # )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task 
    # >> bigquery_external_table_task

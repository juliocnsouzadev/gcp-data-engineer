import base64
import json
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook


from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# settings
DAG_NAME = "LOAD-DATA-INTO-BIGQUERY"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]

# gpc
PROJECT_ID = "muvirtua"
BIG_QUERY_DATASET = "vehicle_analysis"
LANING_BUCKET = "01-logistics-landing"


# tasks ids
T1_ID = "T1-LOAD_DATA"

default_arguments = {
    "owner": OWNER_NAME,
    "email": EMAILS,
    "email_on_failure": False,
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
}

with DAG(
    DAG_NAME,
    schedule_interval=timedelta(hours=1),
    catchup=False,
    default_args=default_arguments,
) as dag:

    # Tasks

    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id=T1_ID,
        bucket=LANING_BUCKET,
        source_object=["*"],
        skip_rows="",
        fiel_delimiter=",",
        destination_project_dataset_table=PROJECT_ID
        + "."
        + BIG_QUERY_DATASET
        + ".history",
        create_disposition="CREATE_IF_NEEEDED",
        write_disposition="WRITE_APPEND",
        bigquery_conn_id="google_cloud_default",
        google_cloud_storage_conn_id="google_cloud_default",
        project_id=PROJECT_ID,
    )

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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

# settings
DAG_NAME = "LOAD-DATA-INTO-BIGQUERY"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]

# gpc
PROJECT_ID = "muvirtua"
BIG_QUERY_DATASET = "vehicle_analysis"
LANING_BUCKET = "01-logistics-landing"
BQ_HISTORY_TABLE = PROJECT_ID + "." + BIG_QUERY_DATASET + ".history"
BQ_LATEST_TABLE = PROJECT_ID + "." + BIG_QUERY_DATASET + ".latest"

# tasks ids
T_LOAD_DATA = "LOAD_DATA"
T_LIST_DATA = "LIST_DATA"
T_FILTER_LATEST = "FILTER_LATEST"

LATEST_QUERY = """
SELECT * except (rank)
FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY vehicle_id ORDER BY DATETIME(date, TIME(hour, minute, 0)) DESC
    ) as rank
    FROM `{}`) as latest
WHERE rank = 1;
""".format(
    BQ_HISTORY_TABLE
)

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
    "max_active_runs": 1,
}


def list_bucket_objects(bucket=None):
    hook = GoogleCloudStorageHook()
    storage_objects = hook.list(bucket)
    return storage_objects


with DAG(
    DAG_NAME,
    schedule_interval=timedelta(hours=1),
    catchup=False,
    default_args=default_arguments,
) as dag:

    # Tasks

    list_files = PythonOperator(
        task_id=T_LIST_DATA,
        python_callable=list_bucket_objects,
        op_kwargs={"bucket": LANING_BUCKET},
    )

    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id=T_LOAD_DATA,
        bucket=LANING_BUCKET,
        source_objects=["*"],
        skip_leading_rows=1,
        field_delimiter=",",
        destination_project_dataset_table=BQ_HISTORY_TABLE,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        bigquery_conn_id="google_cloud_default",
        google_cloud_storage_conn_id="google_cloud_default",
    )

    filter_latest = BigQueryOperator(
        task_id=T_FILTER_LATEST,
        sql=LATEST_QUERY,
        destination_dataset_table=BQ_LATEST_TABLE,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        use_legacy_sql=False,
        location="US",
        bigquery_conn_id="google_cloud_default",
    )


list_files >> load_data >> filter_latest

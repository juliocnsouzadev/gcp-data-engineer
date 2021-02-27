import base64
import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.dates import days_ago

# settings
DAG_NAME = "LOAD-DATA-INTO-BIGQUERY"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]

# gpc
PROJECT_ID = Variable.get("project")
BIG_QUERY_DATASET = Variable.get("bq_dataset")
LANING_BUCKET = Variable.get("landing_bucket")
BACKUP_BUCKET = Variable.get("backup_bucket")
BQ_HISTORY_TABLE = "{}.{}.history".format(PROJECT_ID, BIG_QUERY_DATASET)
BQ_LATEST_TABLE = "{}.{}.latest".format(PROJECT_ID, BIG_QUERY_DATASET)

# tasks ids
T_LOAD_DATA = "LOAD_DATA"
T_LIST_DATA = "LIST_DATA"
T_FILTER_LATEST = "FILTER_LATEST"
T_MOVE_DATA = "MOVE_DATA"

LATEST_QUERY = """
SELECT * except (rank)
FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY vehicle_id ORDER BY DATETIME(date, TIME(hour, minute, 0)) DESC
    ) as rank
    FROM `{{history_table}}`) as latest
WHERE rank = 1;
"""

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


def move_bucket_objects(
    source_bucket=None, destination_bucket=None, prefix=None, **kwargs
):
    storage_objects = kwargs["ti"].xcom_pull(task_ids=T_LIST_DATA)
    hook = GoogleCloudStorageHook()
    for storage_object in storage_objects:
        destination_object = storage_object
        if prefix:
            destination_object = "{}/{}".format(prefix, destination_object)
        # opy(self, source_bucket, source_object, destination_bucket=None, destination_object=None)
        hook.copy(source_bucket, storage_object, destination_bucket, destination_object)
        hook.delete(source_bucket, storage_object)


with DAG(
    DAG_NAME,
    schedule_interval=timedelta(hours=1),
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project": PROJECT_ID, "history_table": BQ_HISTORY_TABLE},
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

    move_files = PythonOperator(
        task_id=T_MOVE_DATA,
        python_callable=move_bucket_objects,
        op_kwargs={
            "source_bucket": LANING_BUCKET,
            "destination_bucket": BACKUP_BUCKET,
            "prefix": "{{ts_nodash}}",
        },
        provide_context=True,
    )


list_files >> load_data >> filter_latest >> move_files

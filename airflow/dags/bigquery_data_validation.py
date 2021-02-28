from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bigquery_plugin import (
    BigQueryDatasetSensor,
    BigQueryDataValidationOperator,
)
from airflow.utils.dates import days_ago

# settings
DAG_NAME = "BIGQUERY_DATA_VALIDATION"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]
SCHEDULE_INTERVAL = "0 20 * * *"

# gpc
PROJECT_ID = Variable.get("project")
BIG_QUERY_DATASET = Variable.get("bq_dataset")
BQ_HISTORY_TABLE = "{}.{}.history".format(PROJECT_ID, BIG_QUERY_DATASET)

# tasks ids
T2_ASSESS_TABLE = "ASSESS_TABLE"
T1_ASSESS_DATASET = "ASSESS_DATASET"

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

with DAG(
    DAG_NAME,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project": PROJECT_ID},
) as dag:

    t1_assess_dataset = BigQueryDatasetSensor(
        task_id=T1_ASSESS_DATASET, project_id=PROJECT_ID, dataset_id=BIG_QUERY_DATASET
    )

    t2_assess_table = BigQueryDataValidationOperator(
        task_id=T2_ASSESS_TABLE,
        sql="SELECT COUNT(*) FROM `{}`".format(BQ_HISTORY_TABLE),
        location="US",
    )

t1_assess_dataset >> t2_assess_table

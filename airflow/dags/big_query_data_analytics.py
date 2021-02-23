from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.dataproc_operartor import DataprocClustCreateOperator

# settings
DAG_NAME = "RUN_SPARK_JOBS"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]

# gpc
PROJECT_ID = Variable.get("project")
BUCKET_SPARK = Variable.get("logistics-spark")

# task ids
T_CREATE_CLUSTER = "create_cluster"
T_ASSESS_DAY = "assess_day"
T_WEEKDAY_ANALYTICS = "week_day_analytics"
T_WEEKEND_ANALYTICS = "weekend_analytics"

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


def assess_day(execution_date=None):
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    if date.isoweekday() < 6:
        return T_WEEKDAY_ANALYTICS
    return T_WEEKEND_ANALYTICS


with DAG(
    DAG_NAME,
    schedule_interval="0 20 * * *",
    catchup=False,
    default_args=default_arguments,
) as dag:

    t_create_cluster = DataprocClustCreateOperator(
        task_id=T_CREATE_CLUSTER,
        project_id=PROJECT_ID,
        cluster_name="spark-cluster-{{ts_nodash}}",
        num_workers=2,
        storage_bucket=BUCKET_SPARK,
        zone="us-east1",
    )

    t_assess_day = BranchPythonOperator(
        task_id=T_ASSESS_DAY,
        python_callable=assess_day,
        op_kwargs={"execution_date": "{{ds}}"},
    )

t_create_cluster >> t_assess_day
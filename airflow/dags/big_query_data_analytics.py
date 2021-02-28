"""
# Run spark job files on Cloud Dataproc
 - weekday jobs
 - weekend jobs
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.models import Variable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from weekday_pyspark_subdag import weekday_subdag

# settings
DAG_NAME = "RUN_SPARK_JOBS"
OWNER_NAME = "Julio Souza"
EMAILS = ["juliocnsouzadev@gmail.com"]

# gpc
PROJECT_ID = Variable.get("project")
BUCKET_SPARK = Variable.get("logistics-spark")
LATEST_PYSPARK_JAR = ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
WEEKEND_FOLDER_PATH = "pyspark/weekend"
WEEK_DAY_FOLDER_PATH = "pyspark/weekday"
WEEKEND_SPARK_JOB_FILE = "gas_composition_count.py"
WEEKDAY_SPARK_JOB_FILES = [
    "avg_speed.py",
    "avg_temperature.py",
    "avg_tire_pressure.py",
]

# task ids
T_CREATE_CLUSTER = "create_cluster"
T_ASSESS_DAY = "assess_day"
T_WEEKDAY_ANALYTICS = "week_day_analytics"
T_WEEKEND_ANALYTICS = "weekend_analytics"
T_DELETE_CLUSTER = "delete_cluster"

SCHEDULE_INTERVAL = "0 20 * * *"

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
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    default_args=default_arguments,
) as dag:

    dag.doc_md = __doc__

    t_create_cluster = DataprocClusterCreateOperator(
        task_id=T_CREATE_CLUSTER,
        project_id=PROJECT_ID,
        cluster_name="spark-cluster-{{ds_nodash}}",
        num_workers=2,
        storage_bucket=BUCKET_SPARK,
        zone="us-east1",
    )
    t_create_cluster.doc_md = """
    ## Create a Dataproc for processing the spark jobs
    """

    t_assess_day = BranchPythonOperator(
        task_id=T_ASSESS_DAY,
        python_callable=assess_day,
        op_kwargs={"execution_date": "{{ds}}"},
    )
    t_assess_day.doc_md = """
    ## Asseses if the the is a week or weekend day
    """

    t_weekend_analytics = DataProcPySparkOperator(
        task_id=T_WEEKEND_ANALYTICS,
        main="gs://{}/{}/{}".format(
            BUCKET_SPARK, WEEKEND_FOLDER_PATH, WEEKEND_SPARK_JOB_FILE
        ),
        cluster_name="spark-cluster-{{ds_nodash}}",
    )
    t_weekend_analytics.doc_md = """
    ## Process weekend spark jobs
    """

    t_weekday_analytics = SubDagOperator(
        task_id=T_WEEKDAY_ANALYTICS,
        subdag=weekday_subdag(
            parent_dag=DAG_NAME,
            task_id=T_WEEKDAY_ANALYTICS,
            schedule_interval=SCHEDULE_INTERVAL,
            default_args=default_arguments,
            cluster_name="spark-cluster-{{ds_nodash}}",
            job_files_paths=[
                "gs://{}/{}/{}".format(BUCKET_SPARK, WEEKEND_FOLDER_PATH, job_file)
                for job_file in WEEKDAY_SPARK_JOB_FILES
            ],
        ),
    )
    t_weekday_analytics.doc_md = """
    ## Process week day spark jobs
    """

    t_delete_cluster = DataprocClusterDeleteOperator(
        task_id=T_DELETE_CLUSTER,
        project_id=PROJECT_ID,
        cluster_name="spark-cluster-{{ds_nodash}}",
        trigger_rule="all_done",
        region="us-east1",
    )
    t_delete_cluster.doc_md = """
    ## Deletes the Dataproc cluster
    """

t_create_cluster >> t_assess_day >> [
    t_weekend_analytics,
    t_weekday_analytics,
] >> t_delete_cluster

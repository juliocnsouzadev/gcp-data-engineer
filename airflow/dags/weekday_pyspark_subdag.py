from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator


def weekday_subdag(
    parent_dag=None,
    task_id=None,
    schedule_interval=None,
    default_args=None,
    cluster_name=None,
    job_files_paths=[],
):
    subdag = DAG(
        f"{parent_dag}.{task_id}",
        schedule_interval=schedule_interval,
        default_args=default_args,
    )

    for job_file_path in job_files_paths:
        splited_path = job_file_path.split("/")
        this_task_id = splited_path[len(splited_path) - 1].replace(".py", "")
        DataProcPySparkOperator(
            task_id=this_task_id,
            main=job_file_path,
            cluster_name=cluster_name,
            dag=subdag,
        )

    return subdag

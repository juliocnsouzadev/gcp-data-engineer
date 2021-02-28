from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
from googleapiclient.errors import HttpError


class BigQueryDataValidationOperator(BaseOperator):
    template_fields = ["sql"]
    ui_color = "#4b0082"

    @apply_defaults
    def __init__(
        self,
        sql,
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.gcp_conn_id = gcp_conn_id

    def run_query(self, project, credentials):
        client = bigquery.Client(project=project, credentials=credentials)
        query_job = client.query(self.sql)
        results = query_job.result()
        return [list(row.values()) for row in results][0]

    def execute(self, context):
        # make connection to big query
        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
        )
        # run sql query
        records = self.run_query(
            project=hook.get_field("project"), credentials=hook.get_credentials()
        )
        # call bool() for each value
        if not records:
            raise AirflowException("Query returned no results")
        elif not all([bool(record) for record in records]):
            raise AirflowException(
                "Test failed on Query: {}\nRecords: {}".format(self.sql, records)
            )
        # all good
        self.log.info("Test passed on Query: {}\nRecords: {}".format(self.sql, records))


class BigQueryDatasetSensor(BaseSensorOperator):

    template_fields = ["project_id", "dataset_id"]
    ui_color = "#4b0082"

    def __init__(
        self,
        project_id,
        dataset_id,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.gcp_conn_id = gcp_conn_id

    def poke(self, context):
        # initialise bigquery hook
        hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id)
        # get bigquery service object
        service = hook.get_service()
        # check if dataset exists
        try:
            service.datasets().get(
                dataset_id=self.dataset_id, project_id=self.project_id
            ).execute()
            return True
        except HttpError as e:
            if e.resp["status"] == "404":
                return False

            raise AirflowException("Error: {}".format(e))


class BigQueryPlugin(AirflowPlugin):
    name = "bigquery_plugin"
    operators = [BigQueryDataValidationOperator]
    sensors = [BigQueryDatasetSensor]

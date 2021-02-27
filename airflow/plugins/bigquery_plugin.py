from airflow import BaseOperator
from airflow.exceptions import AirflowException
from airflow.puglins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hooks import BigQueryHook
from google.cloud import bigquery


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


class BigQueryPlugin(AirflowPlugin):
    name = "bigquery_plugin"
    operators = [BigQueryDataValidationOperator]

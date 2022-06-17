
# STEP 1: Libraries needed
from datetime import timedelta, date
import time
import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator,DataprocCreateClusterOperator,DataprocDeleteClusterOperator,DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator



# STEP 3: Set default arguments for the DAG
DEFAULT_DAG_ARGS = {
    'owner': 'eSIM Developer',  # The owner of the task.
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # We use this in combination with schedule_interval=None to only trigger the DAG with a
    # POST to the REST API.
    # Alternatively, we could set this to yesterday and the dag will be triggered upon upload to the
    # dag folder.
    'start_date': '2022-04-04',
    # 'email': None,
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries.
    # 'project_id': Variable.get('gcp_project'),  # Cloud Composer project ID.
    'project_id': 'vf-grp-cpsa-pprd-cpsoi-10',  # Cloud Composer project ID.
    # We only want the DAG to run when we POST to the api.
    # Alternatively, this could be set to '@daily' to run the job once a day.
    # more options at https://airflow.apache.org/scheduler.html#dag-runs
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with DAG('createdataproc1', description='DAG for deployment a Dataproc Cluster',
         default_args=DEFAULT_DAG_ARGS,
         schedule_interval=None) as dag:  # Here we are using dag as context.'30 4 * * *'
    # STEP 5: Set Operators
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
   
    create_cluster = ClusterGenerator(
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='esim-thales-op-cluster',
        region='europe-west1',
       
        gcp_conn_id='google_cloud_default',
                ##network_uri='vodafone-cpsoi-dev-vpc',
       )
    delete_cluster = DataprocDeleteClusterOperator(
       task_id='delete_dataproc_cluster',
       # Obviously needs to match the name of cluster created in the prior two Operators.
       cluster_name='esim-thales-op-cluster',
       region='europe-west1',
       project_id='vf-grp-cpsa-pprd-cpsoi-10',
       # This will tear down the cluster even if there are failures in upstream tasks.
       trigger_rule=TriggerRule.ALL_DONE)
create_cluster>> delete_cluster 
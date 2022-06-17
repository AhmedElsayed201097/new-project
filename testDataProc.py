# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 10:46:16 2022

@author: HemadA
"""

# STEP 1: Libraries needed
from datetime import timedelta, date
import time
import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# STEP 2:Define a start date
# In this case yesterday
# YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
# These are stored as a Variables in our Airflow Environment.
BUCKET = Variable.get('gcs_bucket')  # GCS bucket with our data.

##Variables
# 1.Spark file path
spark_job_esim_thales_inventory = 'gs://' + BUCKET + '/spark_code/eSIM_thales_inventory.py'

# transformed_bucket = 'gs://esim-service/temp_out'

# 3.BQ table Name
output_table_esim_thales_inventory = 'vf-grp-cpsa-prd-cpsoi-10.esim.thales_inventory'

start_date = date.today()
end_date = start_date

start_date_para = str(start_date)
end_date_para = str(start_date)

operational_data_source_objects = ''
date_objects = ''
# while start_date <= end_date:
# try:
datenow = start_date.strftime("%Y%m%d")


now = date.today()

# operational_data_source_objects_final = Convert(operational_data_source_objects)

# STEP 3: Set default arguments for the DAG
DEFAULT_DAG_ARGS = {
    'owner': 'eSIM Developer',  # The owner of the task.
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # We use this in combination with schedule_interval=None to only trigger the DAG with a
    # POST to the REST API.
    # Alternatively, we could set this to yesterday and the dag will be triggered upon upload to the
    # dag folder.
    'start_date': '2022-06-14',
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
with DAG('create_delete_cluster_test_ahmed', description='DAG for deployment a Dataproc Cluster',
         default_args=DEFAULT_DAG_ARGS,
         schedule_interval=None
         ) as dag:  # Here we are using dag as context.'30 4 * * *'
    # STEP 5: Set Operators
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='create-dataproc-cluster-test',
        num_workers=6,
        master_machine_type='n1-standard-4',
        master_disk_size=1024,
        worker_machine_type='n1-standard-4',
        master_disk_type="pd-standard",
        worker_disk_type="pd-standard",
        worker_disk_size=1024,
        # jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar', # yasmin try it
        zone='europe-west1-b',
        #network_uri='vodafone-esim-pprd-vpc-2',
        subnetwork_uri='vodafone-esim-pprd-vpc-2-europe-west1',
        region='europe-west1',
        #autoscaling_policy=True,
        enable_component_gateway =True,
        use_if_exists =True,
        delete_on_error =True,
        gcp_conn_id ='google_cloud_default',
        service_account='vf-cpsa-esim-composer-sa-2@vf-grp-cpsa-pprd-cpsoi-10.iam.gserviceaccount.com',
        #tags='allow-ssh',
        project_id='vf-grp-cpsa-pprd-cpsoi-10')

    # Delete the Cloud Dataproc cluster.
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        # Obviously needs to match the name of cluster created in the prior two Operators.
        cluster_name='create-dataproc-cluster-test',
        region='europe-west1',
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        # This will tear down the cluster even if there are failures in upstream tasks.
        trigger_rule=TriggerRule.ALL_DONE)

# STEP 6: Order of execution of task that defined.

create_cluster >> delete_cluster
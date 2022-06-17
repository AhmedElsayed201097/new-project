
# STEP 1: Libraries needed
from datetime import timedelta, date
import time
import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,DataprocDeleteClusterOperator,DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator

# STEP 2:Define a start date
# In this case yesterday
# YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
# These are stored as a Variables in our Airflow Environment.
BUCKET = Variable.get('gcs_bucket')  # GCS bucket with our data.

##Variables
# 1.Spark file path
spark_job_esim_thales_Operational = 'gs://vf-grp-cpsa-pprd-cpsoi-10-esim-service/spark_code/eSIM_thales_operationalpyNEW.py'
#'gs://' + BUCKET + '/spark_code/eSIM_thales_operational.py'

# transformed_bucket = 'gs://esim-service/temp_out'

# 3.BQ table Name
output_table_esim_thales_Operational = 'vf-grp-cpsa-pprd-cpsoi-10.esim_thales.thales_operational'

# 4. Date Import Logic to run automation/daily basis
# start_date = date(2021, 1, 1)
# end_date = date(2021, 6, 20)
# delta = timedelta(days=1)
start_date = date(2022, 1, 7)
end_date = date(2022, 1, 7)

start_date_para = str(start_date)
end_date_para = str(start_date)

operational_data_source_objects = ''
date_objects = ''
# while start_date <= end_date:
# try:
datenow = start_date.strftime("%Y%m%d")

# operational_data_out_path = "esim-thales-parquet/thales/operational/"+datenow+"/*"
# operational_data_source_objects += operational_data_out_path
# date_objects += "'" + str(start_date) + "',"
# start_date += delta
# except:
# start_date += delta
# operational_data_source_objects = operational_data_source_objects.replace("part-*", "part-*,")
# operational_data_source_objects = operational_data_source_objects[:-1]
# date_objects = date_objects[:-1]
# date_objects = "(" + date_objects + ")"

# def Convert(string):
#     li = list(string.split(","))
#     return li


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
with DAG('eSIM-thales-operational-service1', description='DAG for deployment a Dataproc Cluster',
         default_args=DEFAULT_DAG_ARGS,
         schedule_interval=None) as dag:  # Here we are using dag as context.'30 4 * * *'
    # STEP 5: Set Operators
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
    CLUSTER_CONFIG = ClusterGenerator(
    project_id='vf-grp-cpsa-pprd-cpsoi-10',
    region='europe-west1',
    cluster_name='esim-thales-op-cluster',
    #tags=["dataproc"],
    #num_workers=2,
    storage_bucket=None,
    #num_masters=1,
    #master_machine_type="n1-standard-4",
    #master_disk_type="pd-standard",
    #master_disk_size=1024,
    #orker_machine_type="n1-standard-4",
    #worker_disk_type="pd-standard",
    #worker_disk_size=1024,
    properties={},
    #image_version="1.5-ubuntu18",
    #autoscaling_policy=True,
    service_account='vf-cpsa-esim-composer-sa-2@vf-grp-cpsa-pprd-cpsoi-10.iam.gserviceaccount.com'
    #idle_delete_ttl=7200,
    #optional_components=['JUPYTER', 'ANACONDA'],
    ).make()
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='esim-thales-op-cluster',
        region='europe-west1',
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        cluster_config=CLUSTER_CONFIG)

    # Delete the existing partionDATA from BQ table if exists
    bq_delete_existing_partitionData = BigQueryExecuteQueryOperator(
        task_id='Deleting_existing_partitionData',
        sql=""" DELETE FROM `{thales_operational}` WHERE dt = "{max_date}"
            """.format(max_date=now, thales_operational=output_table_esim_thales_Operational),
        use_legacy_sql=False,
        gcp_conn_id ='bigquery_default',
        # destination_dataset_table=False,
        #ocation='europe-west1',
        trigger_rule=TriggerRule.ALL_DONE)

    # Submit the PySpark job.
    # For THALES Operational
    submit_pyspark_thales_operational = DataprocSubmitPySparkJobOperator(
        task_id='thales_operational_run_dataproc_pyspark',
        main=spark_job_esim_thales_Operational,
        # Obviously needs to match the name of cluster created in the prior Operator.
        cluster_name='esim-thales-op-cluster',
        region='europe-west1',
        # arguments=[
        #     'gs://vf-grp-cpsa-prd-cpsoi-10-esim-output/services/esim/thales/operational',
        #     'gs://' + BUCKET + '/operational_temp_out',
        #     start_date_para,
        #     end_date_para
        # ]
    )

    # Delete the Cloud Dataproc cluster.
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        # Obviously needs to match the name of cluster created in the prior two Operators.
        cluster_name='esim-thales-op-cluster',
        region='europe-west1',
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        # This will tear down the cluster even if there are failures in upstream tasks.
        trigger_rule=TriggerRule.ALL_DONE)

    # Delete  gcs files in the timestamped transformed folder.
    delete_transformed_files_thales_operational = GCSDeleteObjectsOperator(
        task_id='delete_transformed_files_thales_operational',
        bucket_name='esim-service-pprd',
        prefix='temp_out/operational_temp',
        objects=None,
        trigger_rule=TriggerRule.ALL_DONE)

# STEP 6: Order of execution of task that defined.
# >> bq_delete_existing_partitionData
# >> bq_load_thales_operational
bq_delete_existing_partitionData>>create_cluster>> submit_pyspark_thales_operational >> delete_cluster >> delete_transformed_files_thales_operational
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
BUCKET = Variable.get('gcs_bucket') 

# STEP 2:Define a start date
# In this case yesterday
#YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
#Today = date.today()
# These are stored as a Variables in our Airflow Environment.
BUCKET = Variable.get('gcs_bucket')  # GCS bucket with our data.

##Variables
# 1.Spark file path
spark_job_esim_gnd_Operational = 'gs://' + BUCKET + '/spark_code/eSIM_gnd_operational.py'

# 2.Temp GCS path where Spark code put output
# delete_esim_gnd_Operational = 'gs://' + BUCKET + '/temp_out/operational/*'


# 3.BQ table Name
output_table_esim_gnd_Operational = 'vf-grp-cpsa-pprd-cpsoi-10.esim.gnd_operational'

# 4. Date Import Logic to run automation/daily basis
# start_date = date(2019, 12, 19)
# end_date = date(2020, 12, 27)
# delta = timedelta(days=1)
start_date = date.today() - timedelta(2)
end_date = start_date
delta = timedelta(days=1)

start_date_para = str(start_date)
end_date_para = str(end_date)

operational_data_source_objects = ''
date_objects = ''
while start_date <= end_date:
    try:
        datenow = start_date.strftime("%Y%m%d")
        year = datenow[:4]
        month = datenow[4:6]
        day = datenow[6:8]
        trans_date = start_date.strftime("%Y-%m-%d")
        operational_data_out_path = "operational_temp_out/operational/dt=" + year + month + day + "/part-*"
        operational_data_source_objects += operational_data_out_path
        date_objects += "'" + str(start_date) + "',"
        start_date += delta
    except:
        start_date += delta
operational_data_source_objects = operational_data_source_objects.replace("part-*", "part-*,")
operational_data_source_objects = operational_data_source_objects[:-1]
date_objects = date_objects[:-1]
date_objects = "(" + date_objects + ")"


def Convert(string):
    li = list(string.split(","))
    return li


operational_data_source_objects_final = Convert(operational_data_source_objects)

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
with DAG('eSIM-gnd-operational-service', description='DAG for deployment a Dataproc Cluster',
         default_args=DEFAULT_DAG_ARGS,
         schedule_interval=None) as dag:  # Here we are using dag as context.
    # STEP 5: Set Operators
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
        create_cluster=DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='esim-spark-cluster-{{ ds_nodash }}',
        num_workers=4,
        master_machine_type='n1-standard-8',
        master_disk_size=1000,
        worker_machine_type='n1-standard-16',
        worker_disk_size=1000,
        zone='europe-west1-b',
        ##network_uri='vodafone-cpsoi-dev-vpc',
        subnetwork_uri='vodafone-vf-grp-cpsa-prd-cpsoi-10-prd-be-net',
        region='europe-west1',
        tags='allow-ssh',
        project_id='vf-grp-cpsa-pprd-cpsoi-10')
        # Submit the PySpark job.
        # For GND Operational
        submit_pyspark_gnd_operational = DataprocSubmitPySparkJobOperator(
            task_id='gnd_operational_run_dataproc_pyspark',
            main=spark_job_esim_gnd_Operational,
            # Obviously needs to match the name of cluster created in the prior Operator.
            cluster_name='esim-spark-cluster-{{ ds_nodash }}',
            region='europe-west1',
            arguments=[
                'gs://vf-grp-cpsa-pprd-cpsoi-10-esim-output/services/esim/gnd/operational',
                'gs://' + BUCKET + '/operational_temp_out',
                start_date_para,
                end_date_para
            ]
        )
        # Delete the Cloud Dataproc cluster.
        delete_cluster = DataprocDeleteClusterOperator(
            task_id='delete_dataproc_cluster',
            # Obviously needs to match the name of cluster created in the prior two Operators.
            cluster_name='esim-spark-cluster-{{ ds_nodash }}',
            region='europe-west1',
            project_id='vf-grp-cpsa-pprd-cpsoi-10',
            # This will tear down the cluster even if there are failures in upstream tasks.
            trigger_rule=TriggerRule.ALL_DONE)
        # Delete the existing partionDATA from BQ table if exists
        bq_delete_existing_partitionData = BigQueryExecuteQueryOperator(
            task_id='Deleting_existing_partitionData',
            sql=""" DELETE FROM `{gnd_operational}` WHERE dt in {max_date}
                """.format(max_date=date_objects, gnd_operational=output_table_esim_gnd_Operational),
            use_legacy_sql=False,
            bigquery_conn_id='bigquery_default',
            # destination_dataset_table=False,
            trigger_rule=TriggerRule.ALL_DONE)
        # Load the transformed files to a BigQuery table.
        bq_load_gnd_operational = GCSToBigQueryOperator(
            task_id='Loading_GCS_to_BigQuery_gnd_operational',
            bucket=BUCKET,
            # Reads the relative path to the objects transformed by the spark job.
            source_objects=operational_data_source_objects_final,
            destination_project_dataset_table=output_table_esim_gnd_Operational,
            schema_fields=None,
            schema_object=None,
            autodetect=True,
            source_format='Parquet',
            time_partitioning={'field': 'dt', 'type': 'DAY'},
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=0,
            write_disposition='WRITE_APPEND',
            max_bad_records=0,
            #project_id='vf-grp-cpsa-pprd-cpsoi-10',
            trigger_rule=TriggerRule.ALL_DONE)
        # Delete  gcs files in the timestamped transformed folder.
        delete_transformed_files_gnd_operational = GCSDeleteObjectsOperator(
            task_id='delete_transformed_files_gnd_operational',
            bucket_name=BUCKET,
            prefix='operational_temp',
            objects=None,
            
            trigger_rule=TriggerRule.ALL_DONE)
# STEP 6: Order of execution of task that defined.
create_cluster >> submit_pyspark_gnd_operational >> delete_cluster >> bq_delete_existing_partitionData >> bq_load_gnd_operational >> delete_transformed_files_gnd_operational

    
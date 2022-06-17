from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, DataprocSubmitPySparkJobOperator,DataprocSubmitSparkJobOperator
from airflow.utils.trigger_rule import TriggerRule

yesterday = datetime(2022, 4, 3)

default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with models.DAG(
        'DQ_gnd_opertional_record_check',
        description='DQ Automation',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    start = BashOperator(
        task_id='Header',
        bash_command='echo "DQ Execution Started"'
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='cluster-dq-gnd-op-{{ ds_nodash }}',
        num_workers=2,
        master_machine_type='n1-standard-4',
        master_disk_size=1024,
        worker_machine_type='n1-standard-4',
        worker_disk_size=1024,
        properties={"dataproc:dataproc.logging.stackdriver.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true"},
        zone='europe-west1-b',
        ##network_uri='vodafone-cpsoi-dev-vpc',
        subnetwork_uri='vodafone-vf-grp-cpsa-prd-cpsoi-10-prd-be-net',
        region='europe-west1',
        tags='allow-ssh',
        project_id='vf-grp-cpsa-pprd-cpsoi-10')

    call_DQF = DataprocSubmitSparkJobOperator(
        task_id='execute_spark_job_cluster_test',
        main_jar='gs://esim-data-governance-app-test/jars/CPNS_updated.jar',
        cluster_name='cluster-dq-gnd-op-{{ ds_nodash }}',
        main_class='vodafone.dataqulaity.app.DataQualityCheckApp',
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        region='europe-west1',
        dataproc_properties={'spark.submit.deployMode': 'cluster'},
        arguments=['--bucket', 'esim-data-governance-app-test', '--jsonFile',
                   'configuration_path/prod_gnd_operational_record_check_config_path.json', '--mode', 'cluster']
    )
    # Delete the Cloud Dataproc cluster.
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='cluster-dq-gnd-op-{{ ds_nodash }}',
        region='europe-west1',
        project_id='vf-grp-cpsa-pprd-cpsoi-10',
        # This will tear down the cluster even if there are failures in upstream tasks.
        trigger_rule=TriggerRule.ALL_DONE)

    end = BashOperator(
        task_id='Footer',
        bash_command='echo "DQ Execution Done"'
    )

start >> create_cluster >> call_DQF >> delete_cluster >> end
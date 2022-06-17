#!/usr/bin/env python
# coding: utf-8

# ## Importing liberaries:

import sys
from datetime import datetime, timedelta,date
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.functions import udf, col
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import DataType
from pyspark.sql.functions import lit, concat, udf, when, col
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from datetime import date, timedelta
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import to_date


# ## Start spark session:

spark = SparkSession.builder   .appName('1.1. BigQuery Storage & Spark DataFrames - Python')  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')   .getOrCreate()


sqlContext=SQLContext(spark)
spark.conf.set("spark.sql.repl.eagerEval.enabled",True)


# In[8]:


def esim_thales_ops_trns (inputpath):
    today = date.today()
    input_part = today.strftime("%Y%m%d")
    operational_path = inputpath + "/" + input_part + "/*.json"
    
    df_main = spark.read.json(operational_path)
    df_main = df_main.withColumnRenamed("EID", "eid") \
                .withColumnRenamed("ENDDATE", "enddate") \
                .withColumnRenamed("ICCID", "iccid") \
                .withColumnRenamed("MNO_ID", "mno_id") \
                .withColumnRenamed("N_ID", "n_id") \
                .withColumnRenamed("Notification_Dev_Status", "notification_dev_status") \
                .withColumnRenamed("Profile_State", "profile_state") \
                .withColumnRenamed("Profile_Type", "profile_type") \
                .withColumnRenamed("Reason_Code", "reason_code") \
                .withColumnRenamed("STARTDATE", "startdate") \
                .withColumnRenamed("Subject_Code", "subject_code") \
                .withColumnRenamed("TAC", "tac") \
                .withColumnRenamed("Transaction", "transaction") \
                .withColumnRenamed("Transaction_Status", "transaction_status") \
                .withColumnRenamed("USECASEID", "usecaseid") \
                .withColumnRenamed("UseCaseName", "usecasename") \
                .withColumnRenamed("duration_time", "duration_time")
    
    df_main = df_main.withColumn("opco", F.when(F.trim(df_main['MNO_ID']) == 7, "Germany")
                           .when(F.trim(df_main['MNO_ID']) == 37, "UK")
                           .when(F.trim(df_main['MNO_ID']) == 21, "Australia")
                           .when(F.trim(df_main['MNO_ID']) == 36, "Italy")
                           .when(F.trim(df_main['MNO_ID']) == 58, "Czech")
                           .when(F.trim(df_main['MNO_ID']) == 95, "Netherlands")
                           .when(F.trim(df_main['MNO_ID']) == 97, "Romania")
                           .when(F.trim(df_main['MNO_ID']) == 103, "Hungary")
                           .when(F.trim(df_main['MNO_ID']) == 108, "Ireland")
                           .otherwise("NA"))
        
    df_main = df_main.dropDuplicates()
    #Change here to date not string to match in bigquery:
    df_main = df_main.withColumn("dt", lit(today))
    df_final = df_main.select('opco',
                                      'eid',
                                      'enddate',
                                      'iccid',
                                      'mno_id',
                                      'n_id',
                                      'notification_dev_status',
                                      'profile_state',
                                      'profile_type',
                                      'reason_code',
                                      'startdate',
                                      'subject_code',
                                      'tac',
                                      'transaction',
                                      'transaction_status',
                                      'usecaseid',
                                      'usecasename',
                                      'duration_time',
                                      'dt'
                                      )
    
    output_path_operational='gs://vf-grp-cpsa-pprd-cpsoi-10-esim-service/temp_out/operational_temp/'+input_part

    df_final.write.format("parquet").mode('overwrite').save(output_path_operational)
    # df_parq = spark.read.parquet(output_path_operational)

    # print("DONE")
    # write to bigQuery table 
    
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "vf-grp-cpsa-pprd-cpsoi-10.esim.thales_operational"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    uri = 'gs://esim-service/temp_out/operational_temp/'+input_part+'/*.parquet'

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
    

inputpath="gs://esim-output/services/esim/thales/operational"
esim_thales_ops_trns(inputpath)


# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Table: Prepare data from different sources for customer churn
# MAGIC * The goal of this pipeline is to prepare the data for ML model for customer churn and marketing dashboards.
# MAGIC 
# MAGIC * This pipeline will be triggered using a scheduled job each hour and it is using two data sources:
# MAGIC 
# MAGIC #### 1- Kafka source: containing user clicks 
# MAGIC We are ingesting directly kafka events from a certain topic. 
# MAGIC 
# MAGIC The kafka events are consumed in incremental mode *(using the 'readstream' method)*: Each hour, we trigger this pipeline using a scheduled job, the pipeline will only get the new added events in the kafka topic and will get them through all the steps in the pipeline.
# MAGIC 
# MAGIC #### 2- Delta table: containing user information
# MAGIC This delta table is updated each night using a scheduled job. The job is ingesting data from mysql table.
# MAGIC 
# MAGIC ==> the delta table is updated each night.
# MAGIC 
# MAGIC The delta table is consumed in full mode *(using the 'read' method)*: Each hour, we trigger this pipeline, the pipeline will get the full data from the delta table.
# MAGIC 
# MAGIC 
# MAGIC ### Conclusion
# MAGIC The pipeline is triggered each hour. 
# MAGIC 
# MAGIC It will consume **only the newly added events** in the kafka topic from the last update (an hour ago), it will ingest **the full user information** from the delta table (which is updated using another job from the Mysql database).
# MAGIC 
# MAGIC Major steps highlighted in the pipeline
# MAGIC 1. We ingest both data sources in bronze layers.
# MAGIC 2. We clean the data and set expectations for data quality in the silver layers.
# MAGIC 3. We join the user events with the user infos and we seperate into male/female in the gold layers
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC * Check the notebook *03-Data-quality-on-DLT*: We compute statistics on the pipeline to monitor data quality using DB sql
# MAGIC * Check the notebook *04-ML_customer_segmentation*: We prepare an ML model using MLflow for customer segmentation/churn. 
# MAGIC * Check the DLT pipeline <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#joblist/pipelines/839d4d47-af97-43e7-a1ff-7f8141a82008" target="_top">Link</a>
# MAGIC 
# MAGIC 
# MAGIC ![alt text](https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/END%20to%20END%20usecase%20overview.png?raw=true)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Incremental / 01_bronze_table_binary_events: Ingest events from Kafka topic
import dlt
@dlt.table(  
  name="01_bronze_table_binary_user_events",
  comment="This is an incremental streaming source from kafka topic:marwa_krouma_oetrta_kafka_test containing raw binary kafka events ",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def get_serialized_kafka_events():
  return spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094") \
    .option("kafka.security.protocol", "SSL") \
    .option("subscribe", "marwa_krouma_oetrta_kafka_test") \
    .option("startingOffsets", "earliest") \
    .load()

# COMMAND ----------

# DBTITLE 1,Incremental / 02_bronze_table_decoded_events: Data coming from Kafka topic is binary so we need to convert it to human readable values 
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,unix_timestamp
import datetime
import time
#timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
input_path = "/mnt/marwa-resources/retail/segmentation/csv"
input_schema = spark.read.csv(input_path).schema
input_schema

@dlt.table(  
  name="02_bronze_table_decoded_user_events",
  comment="This is an incremental streaming table containing raw decoded events",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def deserialize_kafka_events():
  return dlt.read_stream("01_bronze_table_binary_user_events").selectExpr("CAST(key AS STRING) as id", "CAST(value AS STRING) as value", "timestamp as timestamp").withColumn("jsonData", F.from_json(F.col("value"), input_schema)).select("timestamp", "jsonData.*")
#.withColumn('timestamp',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))


# COMMAND ----------

# DBTITLE 1,Incremental / 03_silver_table_cleaned_events: Apply expectations on different events' fields 
#Cleaning data and setting expectations on the fields for data quality 
#Use the expect operator when you want to keep records that violate the expectation. Records that violate the expectation are added to the target dataset along with valid records. The number of records that violate the expectation can be viewed in data quality metrics for the target dataset:
@dlt.expect("timestamp is not valid", "datetime>'2021-07-15T15:27:45.755+0000'")
#When invalid records are unacceptable, use the expect or fail operator to halt execution immediately when a record fails validation. If the operation is a table update, the system atomically rolls back the transaction: This will make your update of the pipeline failing if you have null Ids in your kafka events
@dlt.expect_or_drop("id is not valid ", "id IS NOT NULL")  
@dlt.expect_or_drop("age is not valid", "age>20") 

#Use expect_all_or_drop to specify multiple data quality constraints when records that fail validation should be dropped from the target dataset:
#@dlt.expect_or_drop( "valid spending core kafka", "spending_core > 10")
@dlt.table(  
  name="03_silver_table_cleaned_user_events",
  comment="This is an incremental streaming table containing good quality events data",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def clean_kafka_events():
  return dlt.read_stream("02_bronze_table_decoded_user_events").selectExpr("cast(timestamp as string) as datetime ", "cast(_c0 as bigint) as id" ,"cast(_c1 as string) as gender" ,"cast(_c2 as int) as age" ,"cast(_c3 as float) as annual_income" ,"cast(_c4 as float) as spending_core")


# COMMAND ----------

# DBTITLE 1,Incremental / 04_gold_table_top_young_spenders: Think about your business. Prepare data for your ML models and Dashboards
#Filtering on cleaned data for relevant data only / Leaving only big 10000 spenders under 80 yo for churn analysis
@dlt.table(  
  name="04_gold_table_big_spenders_under_eighty",
  comment="This is an incremental streaming table containing top spenders under 80yo.",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def get_big_spenders_under_eighty():
  return dlt.read_stream("03_silver_table_cleaned_user_events").where("age<80 and spending_core>30").limit(10000)

# COMMAND ----------

# DBTITLE 1,Full / 01_bronze_table_user_info: Ingest data from MysqlDB
#Ingesting client infos raw data from s3 or from a MysqlDB

@dlt.table(  
  name="01_bronze_table_user_info",
  comment="This is a full batch source from mysql database delta table updated once a day at midnight containing raw user data "
)
def get_raw_user_data():
  # Here we read the user_info data that we ingested it from mysql. For this we need the path we used when saving data in the delta table. 
  #See the notebook "01b-create_delta_table_from_mysql - batch update - schedule this to run every x hours"
  path_to_delta_table= "/mnt/delta/mysql/user_db/user_info"
  return spark.read.format("delta").load(path_to_delta_table)

# COMMAND ----------

# DBTITLE 1,Full / 02_silver_table_clean_user_info: Clean data and set expectations for better data quality
#Cleaning data and setting expectations on the fields for data quality 
valid_users = {"name is not": "name IS NOT NULL", "id is not valid": "id IS NOT NULL"}
@dlt.expect_all(valid_users)
#@dlt.expect_or_fail("Valid name", "name IS NOT NULL")
#You can also define constraints as a variable and pass it to one or more queries in your pipeline:
#@dlt.expect_all_or_drop(valid_users)
@dlt.table(  
  name="02_silver_table_clean_user_info",
  comment="This is a full batch table for retail client infos with cleaned-up datatypes / column names and quality expectations."
)
def clean_user_data():
  return dlt.read("01_bronze_table_user_info").selectExpr("cast(_c0 as string) as name" ,"cast(_c1 as string) as address" ,"cast(_c2 as string) as email" ,"cast(_c3 as bigint) as id" ,"cast(_c4 as string) as operation", "cast(_c5 as timestamp) as operation_date ")

# COMMAND ----------

# DBTITLE 1,Incremental / user_profile_and_behavior: Joining kafka user events with Mysql user information to get a complete business view on our customers
#Enriching the kafka user events with the user informations we have from mysqldb/s3
@dlt.table(
  name="user_profile_and_behavior",
  comment="This is an incremental table containing user events with the user information to have complete view over the user profile and their behavior"
)
def enrich_user_events():
  return dlt.read_stream("04_gold_table_big_spenders_under_eighty").join(dlt.read("02_silver_table_clean_user_info"), ["id"], "inner")

# COMMAND ----------

# DBTITLE 1,Incremental / male_profile_and_behaviors: Always thinking business and refining my data
@dlt.table(  
  name="male_profile_and_behavior",
  comment="This is an incremental streaming table containing male enriched data.",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def get_male_enriched_data():
  return dlt.read_stream("user_profile_and_behavior").where(F.col("gender")=="Male")


# COMMAND ----------

# DBTITLE 1,Incremental / female_profile_and_behavior: Always thinking business and refining my data / Incremental
@dlt.table(  
  name="female_profile_and_behavior",
  comment="This is an incremental streaming table containing female enriched data.",
  table_properties={"delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.optimizeWrite" : "true"})
def get_female_enriched_data():
  return dlt.read_stream("user_profile_and_behavior").where(F.col("gender")=="Female")

# COMMAND ----------

# MAGIC %md
# MAGIC # How to create and configure this pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/DLT%20pipeline/step1.png?raw=true" width="90%" length="90%"/><br/>
# MAGIC </div>
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/DLT%20pipeline/step2.png?raw=true" width="90%" length="90%"/><br/>
# MAGIC </div>
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/DLT%20pipeline/step3a.png?raw=true" width="80%" length="80%"/><br/>
# MAGIC </div>
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/DLT%20pipeline/step3b.png?raw=true" width="80%" length="80%"/><br/>
# MAGIC </div>
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://github.com/mkrouma93/demos_marwa/blob/962e4630291af858a6e802729c5f1f7498a9343b/KafkaMysql-customer_churn/ressources/images/DLT%20pipeline/step3c.png?raw=true" width="80%" length="80%"/><br/>
# MAGIC </div>

# COMMAND ----------



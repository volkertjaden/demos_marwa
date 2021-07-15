// Databricks notebook source
// MAGIC %md 
// MAGIC #Querying the Event Log
// MAGIC 
// MAGIC This notebook demonstrates how to find and query the Event Log table for a Delta Live Tables pipeline. The Event Log is itself a Delta Table and contains information about the pipeline such as creation, updates, starting or stopping. It also contains data about the tables and view in the pipeline, such as lineage and data quality scores. The full documentation for the Event Log is in development, and this notebook aims to show some sample queries to pull out interesting metrics.
// MAGIC 
// MAGIC The Event Log for any pipeline will be stored inside the Storage Path at `/system/events`.
// MAGIC 
// MAGIC Please use Databricks Runtime 8.1 or newer to be able to use the JSON SQL operators when querying the Event Log.
// MAGIC 
// MAGIC You don't need to run the queries live, just show the results.
// MAGIC 
// MAGIC written 4/20/21
// MAGIC 
// MAGIC 
// MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
// MAGIC   <img src="https://drive.google.com/a/databricks.com/thumbnail?id=1qEpOIYw7JgZbkiOZLIgZh5oNnyrQ_uIe" width="600" height="600"/><br/>
// MAGIC   *DLT Type of expectations*
// MAGIC </div>

// COMMAND ----------

// DBTITLE 1,Setup paths
val storage_path =   "dbfs:/pipelines/839d4d47-af97-43e7-a1ff-7f8141a82008" //modify this path to your Live Tables pipeline path

val event_log_path = storage_path + "/system/events"

// COMMAND ----------

// DBTITLE 1,Register the Event Log table
 spark.sql("CREATE DATABASE IF NOT EXISTS KafkaMysql_pipeline")
 spark.sql("drop table KafkaMysql_pipeline.event_log_raw ")

 spark.sql(s"""
CREATE TABLE IF NOT EXISTS KafkaMysql_pipeline.event_log_raw
USING delta
LOCATION '$event_log_path'
""") 


// COMMAND ----------

display(dbutils.fs.ls("dbfs:/pipelines/839d4d47-af97-43e7-a1ff-7f8141a82008"))

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/pipelines/839d4d47-af97-43e7-a1ff-7f8141a82008/system/events"))

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM KafkaMysql_pipeline.event_log_raw

// COMMAND ----------

val json_parsed_details = spark.read.json(spark.table("KafkaMysql_pipeline.event_log_raw").select("details").as[String])
val json_schema_details = json_parsed_details.schema


// COMMAND ----------


import org.apache.spark.sql.functions._

val parsed_event_log = spark.table("KafkaMysql_pipeline.event_log_raw").withColumn("details_parsed", from_json($"details", json_schema_details))

parsed_event_log.createOrReplaceTempView("event_log")

// COMMAND ----------

 spark.sql("drop table KafkaMysql_pipeline.event_log ")

parsed_event_log.write.format("delta").option("optimizeWrite", "true").saveAsTable("KafkaMysql_pipeline.event_log")

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select *
// MAGIC from KafkaMysql_pipeline.event_log

// COMMAND ----------

// DBTITLE 0,The "details" column contains metadata about each Event sent to the Event Log
// MAGIC %md
// MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
// MAGIC * `user_action` Events occur when taking actions like creating the pipeline
// MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
// MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
// MAGIC   * `flow_type` - whether this is a complete or append flow
// MAGIC   * `explain_text` - the Spark explain plan
// MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
// MAGIC   * `metrics` - currently contains `num_output_rows`
// MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
// MAGIC     * `dropped_records`
// MAGIC     * `expectations`
// MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
// MAGIC   

// COMMAND ----------

// DBTITLE 1,Lineage
/* 
%sql
SELECT
  details:flow_definition.output_dataset,
  details:flow_definition.input_datasets,
  details:flow_definition.flow_type,
  details:flow_definition.schema,
  details:flow_definition.explain_text,
  details:flow_definition
FROM KafkaMysql_pipeline.event_log
WHERE details:flow_definition IS NOT NULL
ORDER BY timestamp
*/

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC SELECT
// MAGIC   details:flow_definition.output_dataset,
// MAGIC   details:flow_definition.input_datasets,
// MAGIC   details:flow_definition.flow_type
// MAGIC FROM KafkaMysql_pipeline.event_log
// MAGIC WHERE details:flow_definition IS NOT NULL
// MAGIC ORDER BY timestamp

// COMMAND ----------

// DBTITLE 1,Flow Progress & Data Quality Results
// MAGIC 
// MAGIC %sql
// MAGIC SELECT 
// MAGIC   id,
// MAGIC   details:flow_progress.metrics,
// MAGIC   details:flow_progress.status,
// MAGIC   details:flow_progress.data_quality.dropped_records,
// MAGIC   explode(from_json(details:flow_progress:data_quality:expectations
// MAGIC            ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations,
// MAGIC   details:flow_progress
// MAGIC FROM KafkaMysql_pipeline.event_log
// MAGIC WHERE details:flow_progress.metrics IS NOT NULL 
// MAGIC ORDER BY timestamp

// COMMAND ----------

/* 
%sql
SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records,
  timestamp
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM KafkaMysql_pipeline.event_log
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality
*/

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS KafkaMysql_pipeline.kafkamysql_data_quality;
// MAGIC CREATE TABLE KafkaMysql_pipeline.kafkamysql_data_quality (id STRING, dataset STRING, expectation_name STRING,  status_update STRING, passed_records int, failed_records int, dropped_records int, output_records int, datetime timestamp);
// MAGIC DROP TABLE IF EXISTS KafkaMysql_pipeline.kafkamysql_data_quality_mysql;
// MAGIC CREATE TABLE KafkaMysql_pipeline.kafkamysql_data_quality_mysql (id STRING, dataset STRING, expectation_name STRING,  status_update STRING, passed_records int, failed_records int, dropped_records int, output_records int, datetime timestamp);
// MAGIC DROP TABLE IF EXISTS KafkaMysql_pipeline.kafkamysql_data_quality_kafka;
// MAGIC CREATE TABLE KafkaMysql_pipeline.kafkamysql_data_quality_kafka (id STRING, dataset STRING, expectation_name STRING,  status_update STRING, passed_records int, failed_records int, dropped_records int, output_records int, datetime timestamp);

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO KafkaMysql_pipeline.kafkamysql_data_quality
// MAGIC     SELECT
// MAGIC   id,
// MAGIC   expectations.dataset,
// MAGIC   expectations.name,
// MAGIC   data_quality.status_update,
// MAGIC   expectations.passed_records,
// MAGIC   expectations.failed_records,
// MAGIC   data_quality.dropped_records,
// MAGIC   data_quality.output_records,
// MAGIC   timestamp
// MAGIC FROM(
// MAGIC   SELECT 
// MAGIC     id,
// MAGIC     timestamp,
// MAGIC     details:flow_progress.status as status_update,
// MAGIC     details:flow_progress.metrics,
// MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
// MAGIC     details:flow_progress.data_quality.dropped_records as dropped_records,
// MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
// MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
// MAGIC   FROM KafkaMysql_pipeline.event_log
// MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS KafkaMysql_pipeline.kafkamysql_failed_updates;
// MAGIC CREATE TABLE KafkaMysql_pipeline.kafkamysql_failed_updates (id STRING, datetime timestamp, message STRING,  error_detailed STRING);

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into KafkaMysql_pipeline.kafkamysql_failed_updates
// MAGIC select * 
// MAGIC from
// MAGIC (
// MAGIC select id, timestamp as datetime, message, error 
// MAGIC FROM KafkaMysql_pipeline.event_log 
// MAGIC WHERE details:flow_progress.metrics IS NULL and level = "ERROR" and event_type = "flow_progress" 
// MAGIC order by timestamp desc limit 1
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from KafkaMysql_pipeline.kafkamysql_data_quality order by datetime desc

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO KafkaMysql_pipeline.kafkamysql_data_quality_mysql
// MAGIC     SELECT *
// MAGIC     from KafkaMysql_pipeline.kafkamysql_data_quality
// MAGIC     WHERE dataset = "02_silver_table_clean_user_info"
// MAGIC   

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from KafkaMysql_pipeline.kafkamysql_data_quality_mysql order by datetime desc

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO KafkaMysql_pipeline.kafkamysql_data_quality_kafka
// MAGIC     SELECT *
// MAGIC     from KafkaMysql_pipeline.kafkamysql_data_quality
// MAGIC     WHERE dataset = "03_silver_table_cleaned_user_events"

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from KafkaMysql_pipeline.kafkamysql_data_quality_kafka  order by datetime desc

// COMMAND ----------



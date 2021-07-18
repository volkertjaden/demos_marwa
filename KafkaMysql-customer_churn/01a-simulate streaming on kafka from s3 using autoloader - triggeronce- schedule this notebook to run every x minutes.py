# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://i.pinimg.com/originals/e1/3f/67/e13f6703e4a52f2421ce4d5473604e40.png" width="400">
# MAGIC 
# MAGIC | Specs                                                    |
# MAGIC |----------------------|-----------------------------------|
# MAGIC | Cluster Name         | oetrta-kafka                      |
# MAGIC | EBS Storage          | 300GiB                            |
# MAGIC | Broker Instance Type | kafka.m5.large                    |
# MAGIC | Broker Count         | 3                                 |
# MAGIC | Availability Zones   | us-west-2a/b/c                    |
# MAGIC | Apache Kafka Version | 2.2.1                             |
# MAGIC | Encryption           | Both TLS and plaintext            |
# MAGIC | Authentication       | None                              |
# MAGIC <br>
# MAGIC 1. Attach IAM role `oetrta-IAM-access` to this cluster
# MAGIC 2. Create your own kafka topic with your name (auto-topic-creation setting is turned on).
# MAGIC 3. Set `kafka.security.protocol` to `SSL` in the streaming configuration option.
# MAGIC 4. Note: for TLS, use port `9094` (default), for plaintext use port `9092`.
# MAGIC 
# MAGIC Note: We are using AWS MSK as a managed Kafka service.
# MAGIC 
# MAGIC Docs: https://docs.databricks.com/spark/latest/structured-streaming/kafka.html#apache-kafka

# COMMAND ----------

# DBTITLE 1,Get Secret Credentials
# You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# Just choose the set of corresponding endpoints to use.
# If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
kafka_bootstrap_servers_tls       = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-tls"       )
kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-plaintext" )

# COMMAND ----------

# DBTITLE 1,Create your a Kafka topic unique to your name
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# username = "marwa.krouma@databricks.com"

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/oetrta/kafka_test"

checkpoint_location = f"{project_dir}/kafka_checkpoint"

topic = f"{user}_oetrta_kafka_test"

# COMMAND ----------

print( username )
print( user )
print( project_dir )
print( checkpoint_location )
print( topic )

# COMMAND ----------

# MAGIC 
# MAGIC %fs rm /mnt/quentin-demo-resources/retail/test.csv 

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/test.csv /mnt/quentin-demo-resources/retail/test.csv

# COMMAND ----------

## Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)
from pyspark.sql import functions as F
input_path = "/mnt/quentin-demo-resources/retail"
input_schema = spark.read.csv(input_path).schema

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.maxFilesPerTrigger", 1)
  .schema(input_schema)
  .load(input_path)
  .select(F.col("_c0").alias("key"), F.to_json(F.struct(F.col('_c0'), F.col('_c1'), F.col('_c2'), F.col('_c3'), F.col('_c4'))).alias("value"))
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094" )
  .option("kafka.security.protocol", "SSL")
  .option("checkpointLocation", checkpoint_location )
  .option("topic", topic)
  .trigger(once=True)
  .start()
)
# display(input_stream)

# COMMAND ----------



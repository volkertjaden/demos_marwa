-- Databricks notebook source
-- MAGIC %python
-- MAGIC eventsDF = spark.readStream \
-- MAGIC                 .format("kafka") \
-- MAGIC                 .option("kafka.bootstrap.servers", "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094") \
-- MAGIC                 .option("kafka.security.protocol", "SSL") \
-- MAGIC                 .option("subscribe", "marwa_krouma_oetrta_kafka_test") \
-- MAGIC                 .option("startingOffsets", "earliest") \
-- MAGIC                 .load()
-- MAGIC 
-- MAGIC 
-- MAGIC eventsDF.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
-- MAGIC         .writeStream \
-- MAGIC         .option("checkpointLocation", "/mnt/delta/marwatestkafka") \
-- MAGIC         .option("ignoreChanges", "true") \
-- MAGIC         .table("events_bronze")

-- COMMAND ----------

select count(*) from events_bronze

-- COMMAND ----------



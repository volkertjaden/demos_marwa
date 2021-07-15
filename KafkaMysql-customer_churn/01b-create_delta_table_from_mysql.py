# Databricks notebook source
# MAGIC %md
# MAGIC ## Each night, Ingest data from Mysql DB and save it to delta table 
# MAGIC 
# MAGIC This otebook will be triggered using a scheduled job.
# MAGIC 
# MAGIC Each night, the job will ingest a table from mysqldb (*See cell n 2*) and save it into a delta table (*See cell n4*).
# MAGIC 
# MAGIC After each ingestion, we perform some optimize opeartions on the delta table to increase performance (*See cell n5*).

# COMMAND ----------

# DBTITLE 1,Change the code in this cell to consume data from your Mysql Database. In this example I am reading data directly from s3
#We are reading data from s3 
# df is a spark dataframe 
df = spark.read.csv("/mnt/quentin-demo-resources/retail/clients/raw_cdc")

# example to read from mysql db 
'''
# df is a spark dataframe 
df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/user_db") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "user_info") \
    .option("user", "me").option("password", "me").load()
'''


# COMMAND ----------

#display(df)

# COMMAND ----------

# DBTITLE 1,Save the content of the spark data frame df into a delta table
# simultaneously create a table and insert data into it from Spark DataFrame

# 1- save the spark dataframe in delta format
#choose the path of your delta table 
path_to_delta_table= "/mnt/delta/mysql/user_db/user_info"

#make sure the path does not exist and if it exists delete it 
dbutils.fs.rm(path_to_delta_table, True)
df.write.format("delta").save(path_to_delta_table)

# Drop the table that was created last night
spark.sql("DROP TABLE IF EXISTS Mysqldb.user_info")

# 2- Create a new table on the delta data that you just saved in the path. 
#In this example I am creating the table in the default database. You can create your own database.
spark.sql("CREATE DATABASE IF NOT EXISTS Mysqldb")
spark.sql("CREATE TABLE Mysqldb.user_info USING DELTA LOCATION '{}/'".format(path_to_delta_table))




# COMMAND ----------

# DBTITLE 1,Step 3: Increase the performance on the delta table
# MAGIC %sql
# MAGIC -- 3- Increase the performance on your table immediately after each save. 
# MAGIC -- Do optimize and zorder on the delta table 
# MAGIC -- Zorder on the column that you will use most in your filtering operations
# MAGIC OPTIMIZE default.user_info
# MAGIC ZORDER BY (_c4)

# COMMAND ----------



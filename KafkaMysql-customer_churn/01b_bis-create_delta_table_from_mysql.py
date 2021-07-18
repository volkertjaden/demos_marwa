# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://i0.wp.com/www.elearningworld.org/wp-content/uploads/2019/04/MySQL.svg.png?fit=600%2C400&ssl=1" width="300">
# MAGIC 
# MAGIC 
# MAGIC Docs: https://docs.databricks.com/data/data-sources/sql-databases.html#connecting-to-sql-databases-using-jdbc

# COMMAND ----------

# DBTITLE 1,Get connection info from secrets
hostname = dbutils.secrets.get( "oetrta", "mysql-hostname" )
port     = dbutils.secrets.get( "oetrta", "mysql-port"     )
database = dbutils.secrets.get( "oetrta", "mysql-database" )
username = dbutils.secrets.get( "oetrta", "mysql-username" )
password = dbutils.secrets.get( "oetrta", "mysql-password" )

# COMMAND ----------

# DBTITLE 1,Create trivial dataset for testing
people = spark.createDataFrame( [ ("Bilbo",     50, "07-07-2021"), 
                                  ("Gandalf", 1000, "07-07-2021"), 
                                  ("Thorin",   195, "07-07-2021"),  
                                  ("Balin",    178, "07-07-2021"), 
                                  ("Kili",      77, "07-07-2021"),
                                  ("Dwalin",   169, "08-07-2021"), 
                                  ("Oin",      167, "08-07-2021"), 
                                  ("Gloin",    158, "08-07-2021"), 
                                  ("Fili",      82, "09-07-2021"), 
                                  ("Bombur",  None, "09-07-2021")
                                ], 
                                ["name", "age", "date"] 
                              )

# COMMAND ----------

# DBTITLE 1,Construct MySQL JDBC URL
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(hostname, port, database)
connectionProperties = {
  "user" : username,
  "password" : password
}

# COMMAND ----------

# DBTITLE 1,Write to MySQL
( people.write 
    .format( "jdbc" ) 
    .option( "url"    , mysql_url ) 
    .option( "dbtable", "test" ) 
    .mode("overwrite")
    .save()
)

# COMMAND ----------

# DBTITLE 1,Read from MySQL
# The query to use when selecting data from mysql table 
pushdown_query = "(select * from oetrta.test where date = '10-07-2021') user_info_from_mysql_per_day"
#read the data from mysql using the defined query and put it into spark dataframe
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
#create a TEMPORARY table to put the new data from mysql into a delta table 
path_to_delta_table = "/mnt/delta/mysql/user_db/user_info"
df.write.format("delta").save(path_to_delta_table)
spark.sql("CREATE DATABASE IF NOT EXISTS Mysqldb")
spark.sql("CREATE TABLE Mysqldb.temp_user_info USING DELTA LOCATION '{}/'".format(path_to_delta_table))




# COMMAND ----------

# MAGIC %sql
# MAGIC --This will only create a database and a table in the first time because we put "IF NOT EXISTS in the query"
# MAGIC CREATE DATABASE IF NOT EXISTS Mysqldb;
# MAGIC CREATE TABLE IF NOT EXISTS Mysqldb.user_info (name string, age int)
# MAGIC     USING PARQUET PARTITIONED BY (date string);
# MAGIC -- insert the data from mysql into the table. This will append the new data to the old data 
# MAGIC INSERT INTO Mysqldb.user_info 
# MAGIC     SELECT * FROM Mysqldb.temp_user_info 
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Mysqldb.user_info

# COMMAND ----------

#delete the temporary delta table and clean the delta path
dbutils.fs.rm(path_to_delta_table, True)
spark.sql("DROP TABLE Mysqldb.temp_user_info")

# COMMAND ----------



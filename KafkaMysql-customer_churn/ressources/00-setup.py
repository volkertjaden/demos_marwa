# Databricks notebook source
#dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"])

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
logging.getLogger('py4j').setLevel(logging.ERROR)
from pyspark.sql.functions import to_date, col
import tempfile
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
import re

# COMMAND ----------

aws_bucket_name = "quentin-demo-resources"
mount_name = "marwa-demo-resources"

#dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
try:
  dbutils.fs.ls("/mnt/%s" % mount_name)
except:
  print("bucket isn't mounted, mount the demo bucket under %s" % mount_name)
  dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)

# COMMAND ----------



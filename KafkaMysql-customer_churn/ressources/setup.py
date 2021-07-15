# Databricks notebook source
import seaborn as sns 
import math
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans, AffinityPropagation
import matplotlib.pyplot as plt
import mlflow 
import plotly.express as px
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
import json
from typing import Iterator, Tuple
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



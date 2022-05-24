# Databricks notebook source
# libraries
from delta.tables import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs, col, collect_set, current_timestamp, explode, from_json, lit, to_json
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from typing import List

# COMMAND ----------

username = "nathan"

# COMMAND ----------

project_pipeline_path = f"/antrasep/{username}/"

#raw_path = project_pipeline_path + "raw/"
bronze_path = project_pipeline_path + "bronze/"
silver_path = project_pipeline_path + "silver/"
#silver_quarantine_path = project_pipeline_path + "silver_quarantine/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS antrasep_{username}")
spark.sql(f"USE antrasep_{username}")

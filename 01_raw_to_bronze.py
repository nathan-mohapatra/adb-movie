# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Raw to Bronze Notebook

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, explode, lit, to_json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configuration

# COMMAND ----------

username = "nathan"

# COMMAND ----------

project_pipeline_path = f"/antrasep/{username}/"

#raw_path = project_pipeline_path + "raw/"
bronze_path = project_pipeline_path + "bronze/"
silver_path = project_pipeline_path + "silver/"
silver_quarantine_path = project_pipeline_path + "silver_quarantine/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS antrasep_{username}")
spark.sql(f"USE antrasep_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronze_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingest Raw Data  
# MAGIC I uploaded the raw data to DBFS for access via notebooks:

# COMMAND ----------

raw_path = "dbfs:/FileStore/raw"
display(dbutils.fs.ls(raw_path))

# COMMAND ----------

print(dbutils.fs.head(
  dbutils.fs.ls(raw_path)[0].path
))

# COMMAND ----------

#kafka_schema = "value STRING"
                                    
raw_movie_data_df = (
     spark.read
    .option("inferSchema", "true")  # TODO: I would like to use `kafka_schema`, but it results in corrupt records
    .option("multiline", "true")    # address multiline format
    .json(raw_path)
)

# COMMAND ----------

raw_movie_data_df = raw_movie_data_df.select(
    explode("movie").alias("value")  # address nested format
)

# COMMAND ----------

raw_movie_data_df = raw_movie_data_df.select(
    to_json(col("value")).alias("value")  # bronze table contains raw data as json string
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display Raw Data

# COMMAND ----------

display(raw_movie_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingestion Metadata

# COMMAND ----------

raw_movie_data_df = raw_movie_data_df.select(
    "value",
    lit(f"{raw_path}").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Batch to Bronze Table

# COMMAND ----------

(
 raw_movie_data_df.select(
 "datasource",
 "ingesttime",
 "value",
 "status",
 col("ingestdate").alias("p_ingestdate")
 )
 .write
 .format("delta")
 .mode("append")
 .partitionBy("p_ingestdate")
 .save(bronze_path)
)

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Register Bronze Table in Metastore

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movie_bronze")

spark.sql(
    f"""
    CREATE TABLE movie_bronze
    USING DELTA
    LOCATION "{bronze_path}"
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

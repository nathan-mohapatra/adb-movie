# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read Batch Raw

# COMMAND ----------

def read_batch_raw(raw_path: str) -> DataFrame:
    """
    Read raw data from provided source into dataframe
    
    :raw_path: File path to raw data
    """
    
    raw_movie_data_df = (
        spark.read
        .option("inferSchema", "true")
        .option("multiline", "true")
        .json(raw_path)
    )

    raw_movie_data_df = raw_movie_data_df.select(
        explode("movie").alias("value")
    )

    return raw_movie_data_df.select(
        to_json(col("value")).alias("value")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transform Raw

# COMMAND ----------

def transform_raw(raw_df: DataFrame, raw_path: str) -> DataFrame:
    """
    Transform raw dataframe into bronze dataframe by adding metadata
    
    :raw_df: Raw dataframe
    :raw_path: File path to raw data
    """
    
    return raw_df.select(
        lit(f"{raw_path}").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "value",
        lit("new").alias("status"),
        current_timestamp().cast("date").alias("p_ingestdate")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Batch Writer

# COMMAND ----------

def batch_writer(
    df: DataFrame,
    partition_column: str = None,
    exclude_columns: List = [],
    mode: str = "append"
) -> DataFrame:
    """
    Prepare batch for write to file path
    
    :df: Dataframe containing batch data
    :partition_column: Column by which output is partitioned
    :exclude_columns: Columns to be excluded from write
    :mode: Specifies behavior when data already exists
    """
    
    if partition_column:
        return (
            df.drop(*exclude_columns)
            .write.format("delta")
            .mode(mode)
            .partitionBy(partition_column)
        )
        
    else:
        return (
            df.drop(*exclude_columns)
            .write.format("delta")
            .mode(mode)
        )

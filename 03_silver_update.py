# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Silver Update Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hardened Logic (Configuration and Operations)

# COMMAND ----------

# MAGIC %run ./includes/operations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Raw to Bronze  
# MAGIC Hardened Logic:

# COMMAND ----------

dbutils.fs.rm(bronze_path, recurse=True)

raw_path = "dbfs:/FileStore/raw"

raw_movie_data_df = read_batch_raw(raw_path)

bronze_movie_data_df = transform_raw(raw_movie_data_df, raw_path)

raw_to_bronze_writer = batch_writer(
    df=bronze_movie_data_df, partition_column="p_ingestdate"
)
raw_to_bronze_writer.save(bronze_path)

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
# MAGIC ## Bronze to Silver  
# MAGIC Hardened Logic:

# COMMAND ----------

dbutils.fs.rm(silver_path, recurse=True)

bronze_movie_data_df = read_batch_bronze()

silver_movie_data_df = transform_bronze(bronze_movie_data_df)

genres_lookup = get_lookup_table(silver_movie_data_df, "genres")
genres_lookup.createOrReplaceTempView("genres_lookup")

original_languages_lookup = get_lookup_table(silver_movie_data_df, "original_languages")
original_languages_lookup.createOrReplaceTempView("original_languages_lookup")

silver_movie_data_df = refactor_dataframe(silver_movie_data_df)

clean_silver_movie_data_df, quarantine_silver_movie_data_df = (
    generate_clean_and_quarantine_dataframes(silver_movie_data_df)
)

bronze_to_silver_writer = batch_writer(
    df=clean_silver_movie_data_df
)
bronze_to_silver_writer.save(silver_path)

spark.sql("DROP TABLE IF EXISTS movie_silver")

spark.sql(
    f"""
    CREATE TABLE movie_silver
    USING DELTA
    LOCATION "{silver_path}"
    """
)

update_bronze_table_status(spark, clean_silver_movie_data_df, "loaded")
update_bronze_table_status(spark, quarantine_silver_movie_data_df, "quarantined")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Quarantined Records From Bronze Table

# COMMAND ----------

quarantine_bronze_movie_data_df = spark.read.table("movie_bronze").where("status = 'quarantined'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform Quarantined Records

# COMMAND ----------

quarantine_silver_movie_data_df = transform_bronze(quarantine_bronze_movie_data_df)
quarantine_silver_movie_data_df = refactor_silver(quarantine_silver_movie_data_df)

display(quarantine_silver_movie_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Repair Quarantined Records

# COMMAND ----------

neg_runtime = quarantine_silver_movie_data_df.where("runtime < 0")

repaired_neg_runtime = (
    neg_runtime
    .withColumn("runtime", abs(col("runtime")))  # set runtime to absolute value of runtime
)

display(repaired_neg_runtime)

# COMMAND ----------

under_budget = quarantine_silver_movie_data_df.where("budget < 1000000")

repaired_under_budget = (
    under_budget
    .withColumn("budget", lit(1000000.00))  # set budget to 1000000
)

display(repaired_under_budget)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Batch Write Repaired (Formerly Quarantined) to Silver Table

# COMMAND ----------

bronze_to_silver_writer = batch_writer(
    df=repaired_neg_runtime, exclude_columns=["value"]
)
bronze_to_silver_writer.save(silver_path)

bronze_to_silver_writer = batch_writer(
    df=repaired_under_budget, exclude_columns=["value"]
)
bronze_to_silver_writer.save(silver_path)

update_bronze_table_status(spark, repaired_neg_runtime, "loaded")
update_bronze_table_status(spark, repaired_under_budget, "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display Quarantined Records

# COMMAND ----------

display(quarantine_bronze_movie_data_df)

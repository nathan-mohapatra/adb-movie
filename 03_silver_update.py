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

# COMMAND ----------

dbutils.fs.rm(silver_path, recurse=True)

bronze_movie_data_df = spark.read.table("movie_bronze").where("status = 'new'")

json_schema = StructType([
    StructField("BackdropUrl", StringType(), True),
    StructField("Budget", DoubleType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("Id", IntegerType(), True),
    StructField("ImdbUrl", StringType(), True),
    StructField("OriginalLanguage", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("PosterUrl", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("ReleaseDate", DateType(), True),
    StructField("Revenue", DoubleType(), True),
    StructField("RunTime", IntegerType(), True),
    StructField("Tagline", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("TmdbUrl", StringType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True)
])

augmented_bronze_movie_data_df = bronze_movie_data_df.withColumn(
    "nested_json", from_json(col("value"), json_schema)
)

silver_movie_data_df = augmented_bronze_movie_data_df.select("value", "nested_json.*")

genres_lookup = (
    silver_movie_data_df
    .withColumn("genres", explode(col("genres")))
    .agg(collect_set("genres").alias("distinct_genres"))
)

genres_lookup = genres_lookup.select(
    explode("distinct_genres").alias("distinct_genres")
)

genres_lookup = (
    genres_lookup
    .select(
        col("distinct_genres.id").alias("id"),
        col("distinct_genres.name").alias("name"))
    .where("name != ''")
    .orderBy(col("id").asc())
)

genres_lookup.createOrReplaceTempView("genres_lookup")

original_languages_lookup = (
    silver_movie_data_df.select(
        lit(1).alias("id"),
        lit("English").alias("language"))
    .distinct()
)

original_languages_lookup.createOrReplaceTempView("original_languages_lookup")

silver_movie_data_df = silver_movie_data_df.select(
    "value",
    col("BackdropUrl").alias("backdrop_url"),
    col("Budget").alias("budget"),
    col("CreatedDate").alias("created_time"),
    col("Id").alias("movie_id"),
    col("ImdbUrl").alias("imdb_url"),
    lit(1).alias("original_language_id"),
    col("Overview").alias("overview"),
    col("PosterUrl").alias("poster_url"),
    col("Price").alias("price"),
    col("ReleaseDate").alias("release_date"),
    col("Revenue").alias("revenue"),
    col("RunTime").alias("runtime"),
    col("Tagline").alias("tagline"),
    col("Title").alias("title"),
    col("TmdbUrl").alias("tmdb_url"),
    col("genres.id").alias("genre_id")
).dropDuplicates(["value"])

clean_silver_movie_data_df = silver_movie_data_df.where(
    (col("runtime") >= 0) & (col("budget") >= 1000000)
)

quarantine_silver_movie_data_df = silver_movie_data_df.where(
    (col("runtime") < 0) | (col("budget") < 1000000)
)

(
clean_silver_movie_data_df.drop("value")
    .write
    .format("delta")
    .mode("append")
    .save(silver_path)
)

spark.sql("DROP TABLE IF EXISTS movie_silver")

spark.sql(
    f"""
    CREATE TABLE movie_silver
    USING DELTA
    LOCATION "{silver_path}"
    """
)

bronze_table = DeltaTable.forPath(spark, bronze_path)

augmented_silver_movie_data_df = (
    clean_silver_movie_data_df
    .withColumn("status", lit("loaded"))
)

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
  bronze_table.alias("bronze")
  .merge(augmented_silver_movie_data_df.alias("clean"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)

augmented_silver_movie_data_df = (
    quarantine_silver_movie_data_df
    .withColumn("status", lit("quarantined"))
)

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
  bronze_table.alias("bronze")
  .merge(augmented_silver_movie_data_df.alias("quarantine"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)

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

augmented_quarantine_bronze_movie_data_df = quarantine_bronze_movie_data_df.withColumn(
    "nested_json", from_json(col("value"), json_schema)
)

quarantine_silver_movie_data_df = augmented_quarantine_bronze_movie_data_df.select("value", "nested_json.*")

quarantine_silver_movie_data_df = quarantine_silver_movie_data_df.select(
    "value",
    col("BackdropUrl").alias("backdrop_url"),
    col("Budget").alias("budget"),
    col("CreatedDate").alias("created_time"),
    col("Id").alias("movie_id"),
    col("ImdbUrl").alias("imdb_url"),
    lit(1).alias("original_language_id"),
    col("Overview").alias("overview"),
    col("PosterUrl").alias("poster_url"),
    col("Price").alias("price"),
    col("ReleaseDate").alias("release_date"),
    col("Revenue").alias("revenue"),
    col("RunTime").alias("runtime"),
    col("Tagline").alias("tagline"),
    col("Title").alias("title"),
    col("TmdbUrl").alias("tmdb_url"),
    col("genres.id").alias("genre_id")
).dropDuplicates(["value"])

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

(
repaired_neg_runtime.drop("value")
    .write
    .format("delta")
    .mode("append")
    .save(silver_path)
)

# COMMAND ----------

augmented_repaired_neg_runtime = (
    repaired_neg_runtime.withColumn("status", lit("loaded"))
)

update_match = "bronze.value = repair.value"
update = {"status": "repair.status"}

(
    bronze_table.alias("bronze")
    .merge(augmented_repaired_neg_runtime.alias("repair"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

(
repaired_under_budget.drop("value")
    .write
    .format("delta")
    .mode("append")
    .save(silver_path)
)

# COMMAND ----------

augmented_repaired_under_budget = (
    repaired_under_budget.withColumn("status", lit("loaded"))
)

update_match = "bronze.value = repair.value"
update = {"status": "repair.status"}

(
    bronze_table.alias("bronze")
    .merge(augmented_repaired_under_budget.alias("repair"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display Quarantined Records

# COMMAND ----------

display(quarantine_bronze_movie_data_df)

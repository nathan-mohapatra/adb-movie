# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Bronze to Silver Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hardened Logic (Configuration and Operations)

# COMMAND ----------

# MAGIC %run ./includes/operations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display Files in Bronze Path

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Raw to Bronze  
# MAGIC Hardened logic:

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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(silver_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load New Records From Bronze Table

# COMMAND ----------

bronze_movie_data_df = spark.read.table("movie_bronze").where("status = 'new'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Extract Nested JSON From Bronze Records

# COMMAND ----------

# json_schema = """
#     backdrop_url STRING,
#     budget DOUBLE,
#     created_by STRING,
#     created_date TIMESTAMP,
#     id INTEGER,
#     imdb_url STRING,
#     original_language STRING,
#     overview STRING,
#     poster_url STRING,
#     price DOUBLE,
#     release_date DATE,
#     revenue DOUBLE,
#     run_time INTEGER,
#     tagline STRING,
#     title STRING,
#     tmdb_url STRING,
#     updated_by STRING,
#     updated_date TIMESTAMP,
#     genres STRUCT
# """

# unfortunately I had to define the schema this way to avoid errors
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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Silver DataFrame by Unpacking Nested JSON

# COMMAND ----------

silver_movie_data_df = augmented_bronze_movie_data_df.select("value", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Certain movies have valid ID for the genre, but the name of the genre is missing. Do we need to fix this? And where should we fix this? If no, why do we not need to fix this?**  
# MAGIC No, we do not need to fix this if there is a genres lookup table. Once the genres lookup table is created, a valid ID for the genre will serve as a foreign key referencing the lookup table (which contains the corresponding name of the genre).

# COMMAND ----------

# genres lookup table
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

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM genres_lookup

# COMMAND ----------

silver_movie_data_df.select("OriginalLanguage").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There seems to be only one distinct original language... yet we are being asked to make a lookup table for it.

# COMMAND ----------

# original languages lookup table
original_languages_lookup = (
    silver_movie_data_df.select(
        lit(1).alias("id"),
        lit("English").alias("language"))
    .distinct()
)

original_languages_lookup.createOrReplaceTempView("original_languages_lookup")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM original_languages_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **We have some movies that are showing up in more than one movie files. How do we ensure that only one record shows up in our silver table?**  
# MAGIC We can remove duplicate rows from the silver dataframe based on the `value` column, ensuring that only one record shows up in our silver table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform Data

# COMMAND ----------

# more consistent naming convention
silver_movie_data_df = silver_movie_data_df.select(
    "value",
    col("BackdropUrl").alias("backdrop_url"),
    col("Budget").alias("budget"),
    col("CreatedDate").alias("created_time"),
    col("Id").alias("movie_id"),
    col("ImdbUrl").alias("imdb_url"),
    lit(1).alias("original_language_id"),  # foreign key to original languages lookup table
    col("Overview").alias("overview"),
    col("PosterUrl").alias("poster_url"),
    col("Price").alias("price"),
    col("ReleaseDate").alias("release_date"),
    col("Revenue").alias("revenue"),
    col("RunTime").alias("runtime"),
    col("Tagline").alias("tagline"),
    col("Title").alias("title"),
    col("TmdbUrl").alias("tmdb_url"),
    col("genres.id").alias("genre_id")  # foreign key to genres lookup table
).dropDuplicates(["value"])  # drop duplicates (same raw data)

display(silver_movie_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Quarantine Bad Data

# COMMAND ----------

silver_movie_data_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Some movies have a negative runtime:

# COMMAND ----------

silver_movie_data_df.select("*").where("runtime < 0").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let us assume that all of the movies should have a minimum budget of $1,000,000:

# COMMAND ----------

silver_movie_data_df.select("*").where("budget < 1000000").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Split Silver DataFrame

# COMMAND ----------

clean_silver_movie_data_df = silver_movie_data_df.where(
    (col("runtime") >= 0) & (col("budget") >= 1000000)
)

quarantine_silver_movie_data_df = silver_movie_data_df.where(
    (col("runtime") < 0) | (col("budget") < 1000000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display Quarantined Records

# COMMAND ----------

display(quarantine_silver_movie_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Clean Batch to Silver Table

# COMMAND ----------

(
clean_silver_movie_data_df.drop("value")
    .write
    .format("delta")
    .mode("append")
    .save(silver_path)
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movie_silver")

spark.sql(
    f"""
    CREATE TABLE movie_silver
    USING DELTA
    LOCATION "{silver_path}"
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update Clean Records

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update Quarantined Records

# COMMAND ----------

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

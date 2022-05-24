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
    :return: Raw dataframe
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
    :return: Bronze dataframe
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
    :return: Dataframe object for write
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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read Batch Bronze

# COMMAND ----------

def read_batch_bronze() -> DataFrame:
    """
    Read new bronze data from bronze table
    
    :return: Bronze dataframe
    """
    
    return spark.read.table("movie_bronze").where("status = 'new'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transform Bronze

# COMMAND ----------

def transform_bronze(bronze_df: DataFrame) -> DataFrame:
    """
    Transform bronze dataframe into silver dataframe by processing nested JSON
    
    :bronze_df: Bronze dataframe
    :return: Silver dataframe
    """
    
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
    
    augmented_bronze_df = bronze_df.withColumn(
        "nested_json", from_json(col("value"), json_schema)
    )
    
    return augmented_bronze_df.select("value", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Lookup Table

# COMMAND ----------

def get_lookup_table(silver_df: DataFrame, table_type: str) -> DataFrame:
    """
    Create specified lookup table, given silver dataframe
    
    :silver_df: Silver dataframe
    :table_type: Type of lookup table ('genres' or 'original_languages')
    :return: Specified lookup table
    """
    
    lookup_table_df = None
    
    if table_type == "genres":
        lookup_table_df = (
            silver_df
            .withColumn("genres", explode(col("genres")))
            .agg(collect_set("genres").alias("distinct_genres"))
        )

        lookup_table_df = lookup_table_df.select(
            explode("distinct_genres").alias("distinct_genres")
        )

        lookup_table_df = (
            lookup_table_df
            .select(
                col("distinct_genres.id").alias("id"),
                col("distinct_genres.name").alias("name"))
            .where("name != ''")
            .orderBy(col("id").asc())
        )
        
    elif table_type == "original_languages":
        lookup_table_df = (
            silver_df.select(
                lit(1).alias("id"),
                lit("English").alias("language"))
            .distinct()
        )
        
    return lookup_table_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Refactor Dataframe

# COMMAND ----------

def refactor_silver(silver_df: DataFrame) -> DataFrame:
    """
    Rename columns, set foreign keys, remove duplicates
    
    :silver_df: Silver dataframe
    :return: Refactored silver dataframe
    """
    
    return silver_df.select(
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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Generate Clean and Quarantine Dataframes

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    silver_df: DataFrame
) -> (DataFrame, DataFrame):
    """
    Split silver dataframe into clean and quarantine dataframes
    
    :silver_df: Silver dataframe
    :return: Tuple containing both dataframes
    """
    
    clean_df = silver_df.where(
        (col("runtime") >= 0) & (col("budget") >= 1000000)
    )

    quarantine_df = silver_df.where(
        (col("runtime") < 0) | (col("budget") < 1000000)
    )
    
    return clean_df, quarantine_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Update Bronze Table Status

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, df: DataFrame, status: str
) -> bool:
    """
    Update status in bronze table given status of provided dataframe
    
    :spark: Current spark session
    :df: Dataframe
    :status: Status of data in dataframe
    :return: Whether or not bronze table updated successfully
    """
    
    bronze_table = DeltaTable.forPath(spark, bronze_path)
    augmented_df = (df.withColumn("status", lit(status)))

    update_match = "bronze.value = dataframe.value"
    update = {"status": "dataframe.status"}

    (
      bronze_table.alias("bronze")
      .merge(augmented_df.alias("dataframe"), update_match)
      .whenMatchedUpdate(set=update)
      .execute()
    )
    
    return True

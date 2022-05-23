# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Configuration

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
# MAGIC ## Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS antrasep_{username}")
spark.sql(f"USE antrasep_{username}")

# Databricks notebook source
# MAGIC %md
# MAGIC ### Database set-up

# COMMAND ----------

my_name = <ENTER_YOUR_NAME>

# COMMAND ----------

spark.sql(f"create database if not exists db_{my_name}")

# COMMAND ----------

spark.sql(f"use db_{my_name}")
spark.sql(f"""
    CREATE OR REPLACE TABLE wine_data_{my_name} (
        wine STRING,
        winery STRING,
        category STRING,
        designation STRING,
        varietal STRING,
        appellation STRING,
        alcohol DECIMAL(10,1),
        price INT,
        rating INT,
        reviewer STRING,
        review STRING,
        country STRING,
        region STRING
    )
""")

# COMMAND ----------



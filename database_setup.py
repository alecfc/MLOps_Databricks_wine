# Databricks notebook source
# MAGIC %md
# MAGIC ### Database set-up

# COMMAND ----------

my_name = 'alec'

# COMMAND ----------

spark.sql(f"create database if not exists db_{my_name}")

# COMMAND ----------

spark.sql(f"use db_{my_name}")
spark.sql(f"""
    CREATE OR REPLACE TABLE wine_train_data_{my_name} (
        id LONG,
        year INT,
        wine STRING,
        winery STRING,
        category STRING,
        wine_name STRING,
        grape_variety STRING,
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

spark.sql(f"""
    CREATE OR REPLACE TABLE wine_test_data_{my_name} (
        id LONG,
        year INT,
        wine STRING,
        winery STRING,
        category STRING,
        wine_name STRING,
        grape_variety STRING,
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



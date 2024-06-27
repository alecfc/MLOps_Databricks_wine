# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Preprocessing steps
# MAGIC ** **
# MAGIC In this notebook we execute some preprocessing steps to get the wine dataset ready for analysis and training. Some preprocessing steps can be improved, yielding to better model performances!

# COMMAND ----------

import os
import zipfile
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

zip_file_path = 'data/wine_data.zip'
# Extract the zip file
if not os.path.exists('data/wine_first_batch.csv'):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall('data')

# COMMAND ----------

#my_name = <enter your name here>

# COMMAND ----------

my_name = "alec"

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS db_{my_name}.wine_test_data_{my_name}
""")

spark.sql(f"""
DROP TABLE IF EXISTS db_{my_name}.wine_train_data_{my_name}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC First we read the first batch data into a Spark dataframe and view the amount of rows and columns

# COMMAND ----------

df =  spark.read.option("delimiter", ",").option("header", True).csv("file:/Workspace/Users/alec.flesher-clark@brightcubes.nl/MLOps_Databricks_wine/data/wine_first_batch.csv")
df.display()

# COMMAND ----------

print((df.count(), len(df.columns)))
df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC As a first preprocessing step, we create two separate columns out of the appellation column: country and region

# COMMAND ----------

df = (df.withColumn("country", F.trim(F.element_at(F.split(F.col("appellation"), ","), -1)))
                    .withColumn("region", F.trim(F.element_at(F.split(F.col("appellation"), ","), -2)))
                  )
df = df.filter(df.region.contains("$") == False)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the null values in the alcohol column and convert alcohol price and rating columns to decimals and integers

# COMMAND ----------

df = df.na.drop(subset = ['alcohol'])
df = (df.withColumn("id", F.monotonically_increasing_id().cast(LongType()))
        .withColumn('alcohol', F.regexp_replace('alcohol', '%', '').cast(DecimalType(20,1)))
        .withColumn('price', F.regexp_replace('price', '\$', '').cast(DecimalType(20,0)))
        .withColumnRenamed('varietal', 'grape_variety')
        .withColumnRenamed('designation', 'wine_name')
        .withColumn('rating', df.rating.cast(IntegerType()))
        .withColumn('year', df.year.cast(IntegerType()))
)

# Get the list of columns
columns = df.columns

# Reorder columns: move 'ID' to the front
reordered_columns = ['id'] + [col for col in columns if col != 'id']

# Select reordered columns
df = df.select(reordered_columns)
df.display()

# COMMAND ----------

train, test = df.randomSplit([0.8,0.2], seed=42)
test.display()

# COMMAND ----------

train.createOrReplaceTempView('train_data')
test.createOrReplaceTempView('test_data')

# COMMAND ----------

# MAGIC %md
# MAGIC Merge preprocessed dataframe into a table uniquely identified by your name

# COMMAND ----------

spark.sql(f"""
MERGE INTO db_{my_name}.wine_train_data_{my_name}
USING train_data
ON wine_train_data_{my_name}.id = train_data.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

spark.sql(f"""
MERGE INTO db_{my_name}.wine_test_data_{my_name}
USING test_data
ON wine_test_data_{my_name}.id = test_data.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------

print((train.count(), len(train.columns)))
print((test.count(), len(test.columns)))

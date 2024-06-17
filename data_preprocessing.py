# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Preprocessing steps
# MAGIC ** **
# MAGIC In this notebook we execute some preprocessing steps to get the wine dataset ready for analysis and training. Some preprocessing steps can be improved, yielding to better model performances!

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

#my_name = <enter your name here>

# COMMAND ----------

my_name = "alec"

# COMMAND ----------

# MAGIC %md
# MAGIC First we read the data into a Spark dataframe and view the amount of rows and columns

# COMMAND ----------

df =  spark.read.option("delimiter", ",").option("header", True).csv("file:/Workspace/Users/alec.flesher-clark@brightcubes.nl/MLOps_Databricks_wine/data/wine.csv")
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
df = (df.withColumn('alcohol', F.regexp_replace('alcohol', '%', '').cast(DecimalType(20,1)))
        .withColumn('price', F.regexp_replace('price', '\$', '').cast(DecimalType(20,0)))
        .withColumnRenamed('varietal', 'grape variety')
        .withColumnRenamed('designation', 'name')
        .withColumn('rating', df.rating.cast(IntegerType()))
)
df.display()

# COMMAND ----------

# +df = df.replace({'N.V.': None}, subset=['year'])
# df=df.dropna()
# df.show()

# COMMAND ----------

df.createOrReplaceTempView('new_data')

# COMMAND ----------

# MAGIC %md
# MAGIC Merge preprocessed dataframe into a table uniquely identified by your name

# COMMAND ----------

spark.sql(f"""
MERGE INTO db_{my_name}.wine_data_{my_name}
USING new_data
ON wine_data_{my_name}.wine = new_data.wine
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------

df.display()

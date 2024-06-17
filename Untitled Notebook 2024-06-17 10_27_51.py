# Databricks notebook source
import numpy as np 
import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt
spark.sql('USE db_alec')
spark_df = spark.sql("SELECT * FROM wine_data_alec")
df = spark_df.toPandas()

# COMMAND ----------

print("Num of categories: " + str(df['category'].nunique()))

y = df["category"]
df["category_codes"] = y.astype('category').cat.codes
ax = sns.displot(df["category"], height=5)
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

print("Num of categories: " + str(df['region'].nunique()))
print(df['region'].unique())
y = df["region"]
ax = sns.displot(df["region"], height=5)
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

n = 15
df['category'].value_counts()[:n].index.tolist()

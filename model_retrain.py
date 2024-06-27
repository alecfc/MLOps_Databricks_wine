# Databricks notebook source
# MAGIC %md
# MAGIC ### Retraining model with new training data
# MAGIC ** **
# MAGIC Now that you have run experiments and tested your best performing model, it is time to retrain your model with more data on wines. Your current training data contains information about wines up to 2016, but now you have acquired the data of wines from 2017 until 2020.  
# MAGIC
# MAGIC First, let's preprocess the second batch of wine data and then retrain your model with the updated training dataset!

# COMMAND ----------

import mlflow
import mlflow.pyfunc
from pyspark.sql import SparkSession
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

my_name = "alec"
model_name = "price_predictor_alec"
model_version = 1
train_view_name = "db_alec.wine_train_data_alec"

# COMMAND ----------

wines_second_batch = spark.read.option("delimiter", ",").option("header", True).csv("file:/Workspace/Users/alec.flesher-clark@brightcubes.nl/MLOps_Databricks_wine/data/wine_second_batch.csv")
wines_second_batch.display()

# COMMAND ----------

print((wines_second_batch.count(), len(wines_second_batch.columns)))
wines_second_batch.describe()

# COMMAND ----------

wines_second_batch = (wines_second_batch.withColumn("country", F.trim(F.element_at(F.split(F.col("appellation"), ","), -1)))
                    .withColumn("region", F.trim(F.element_at(F.split(F.col("appellation"), ","), -2)))
                  )
wines_second_batch = wines_second_batch.filter(wines_second_batch.region.contains("$") == False)
wines_second_batch.display()

# COMMAND ----------

wines_second_batch = wines_second_batch.na.drop(subset = ['alcohol'])
wines_second_batch = (wines_second_batch.withColumn("id", F.monotonically_increasing_id().cast(LongType()))
        .withColumn('alcohol', F.regexp_replace('alcohol', '%', '').cast(DecimalType(20,1)))
        .withColumn('price', F.regexp_replace('price', '\$', '').cast(DecimalType(20,0)))
        .withColumnRenamed('varietal', 'grape_variety')
        .withColumnRenamed('designation', 'wine_name')
        .withColumn('rating', wines_second_batch.rating.cast(IntegerType()))
        .withColumn('year', wines_second_batch.year.cast(IntegerType()))
)

# Get the list of columns
columns = wines_second_batch.columns

# Reorder columns: move 'ID' to the front
reordered_columns = ['id'] + [col for col in columns if col != 'id']

# Select reordered columns
wines_second_batch = wines_second_batch.select(reordered_columns)
wines_second_batch.display()

# COMMAND ----------

train, test = wines_second_batch.randomSplit([0.8,0.2], seed=42)
train.createOrReplaceTempView('new_train_data')
test.createOrReplaceTempView('new_test_data')
test.display()

# COMMAND ----------

spark.sql(f"""
MERGE INTO db_{my_name}.wine_train_data_{my_name}
USING new_train_data
ON wine_train_data_{my_name}.id = new_train_data.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

spark.sql(f"""
MERGE INTO db_{my_name}.wine_test_data_{my_name}
USING new_test_data
ON wine_test_data_{my_name}.id = new_test_data.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Now the train and test data is updated with the wines from 2017 until 2020, it is time to run a new experiment on the training data and update your registered model with the best performing option!

# COMMAND ----------

full_training_data = spark.table(train_view_name).na.drop(subset=["price"]).toPandas()

# Split the data into features and target
# Remove the unnecessary features, you may choose which but below are some suggestions
# Make sure price (the target variable) is dropped from X_new and added to Y_new
X_new = full_training_data.drop(['id', 'review', 
                'reviewer', 'price'], axis=1)

y_new = full_training_data["price"]

# COMMAND ----------

# Load the pyfunc model
model_uri = f"models:/{model_name}/{model_version}"
pyfunc_model = mlflow.pyfunc.load_model(model_uri)

# Inspect the model directory to determine the original type of model
model_path = mlflow.artifacts.download_artifacts(model_uri)
requirements_path = os.path.join(model_path, "requirements.txt")
print("Model artifacts downloaded to:", model_path)
print("Directory contents:", os.listdir(model_path))


# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# If your model directory contains a .pkl file, we are dealing with a sklearn model. For this workshop we assume you are using an sklearn model
if "model.pkl" in os.listdir(model_path): 
    original_model = mlflow.sklearn.load_model(model_uri)
    print("Loaded scikit-learn model.")

# COMMAND ----------

#  Retrain the model
try:
    if hasattr(original_model, "fit"):
        original_model.fit(X_new, y_new)
    else:
        warnings.warn("The loaded model does not support retraining via 'fit' method.")
except Exception as e:
    print(f"An error occurred while retraining the model: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now the model is retrained, it is time to update the model registry with the newer version of your model

# COMMAND ----------

# Create a new experiment
experiment_name = f"/Users/{my_name}/Retrain Experiment"
experiment_id = mlflow.create_experiment(experiment_name)

# Step 4: Log and register the new version of the model
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(artifact_path="model", python_model=loaded_model)
    new_model_version = mlflow.register_model(
        model_uri=f"runs:/{run.info.run_id}/model",
        name=model_name
    )

print("New model version registered successfully.")

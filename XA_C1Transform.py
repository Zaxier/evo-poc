# Databricks notebook source
# from pyspark.sql.functions import *
# import datetime
# from pyspark.sql.window import *
# from pyspark.sql.types import *

# OR

import pyspark.sql.functions as F
import datetime
import pyspark.sql.window as W
import pyspark.sql.types as T
# and then calling F.<function-name> so you know where func comes from


# Input variables
#  - these don't need to change most likely and can be hard-coded (not needing widget params)
read_path = "" # The folder that contains the subdirectories and files of the csv dataset
write_path = "" # Needed for external tables 
checkpoint_path = write_path + "_checkpoints/"

# Load schema requirements
#  - see this link for an option for store and load schemas:  https://stackoverflow.com/a/56709071
schema_requirements_based_on_DW_table = "" # Load a schema from somewhere and validate it

# Read from container using Auto Loader (format="cloudFiles")
df = (
  spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", )
  .option("cloudFiles.inferColumnTypes", "true") # OPTIONAL
  .load(read_path)
)

# Remove whitespace from column names
for c in df.columns:
  df = df.withColumnRenamed(c, c.replace(" ", ""))
  
try:
  # Validate Schema
  #   - see this link for an option for store and load schemas:  https://stackoverflow.com/a/56709071
  df = validateSchema(df, schema_requirements) # If above isn't viable, assumed to be defined elsewhere
  
except Exception as e:
  # Schema validation failed
  #   - do something or ..?
  pass

else:
  # Write to delta lake
  (
    df.
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_path)
    .trigger(once=True)
  )
  


# COMMAND ----------

df = spark.table("xavier_armitage_ap_juice_db.features_oj_prediction_experiment")

# COMMAND ----------

for c in df.columns:
  df = df.withColumnRenamed(c, c.replace("f", "x"))

# COMMAND ----------



# Databricks notebook source
# DBTITLE 1,Imports and variables
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

# COMMAND ----------

# DBTITLE 1,Processing
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

  assert df.count() > 0
  
except Exception as e:
  # Schema validation failed
  #   - do something or ..?
  pass

except AssertionError as e:
  log.error()
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

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY coingecko.brz_get_coins_markets

# COMMAND ----------

# DBTITLE 1,Clean setup and cleanup
# MAGIC %sql
# MAGIC DROP SCHEMA evo_mining CASCADE;
# MAGIC CREATE SCHEMA evo_mining

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE coingecko.brz_get_coins_markets

# COMMAND ----------

# DBTITLE 1,loading the first few days
spark.conf.set("spark.default.parallelism", sc.defaultParallelism)

read_path = "/mnt/raw/coingecko/get_coins_markets/"
write_path = "/mnt/dlake/evo_mining.db/brz_get_coins_markets/" # Corresponds to evo_mining.brz_get_coins_markets table
checkpoint_path = write_path + "_checkpoints/"

df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("multiline", "true")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("cloudFiles.inferColumnTypes", "true")
  .option("path", read_path)
  .load()
)

(df.writeStream
  .format("delta")
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", checkpoint_path)
  .option("path", write_path)
  .trigger(once=True)
  .start()
)

spark.sql("CREATE TABLE IF NOT EXISTS evo_mining.brz_get_coins_markets USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from evo_mining.brz_get_coins_markets

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from evo_mining.brz_get_coins_markets

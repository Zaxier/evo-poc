# Databricks notebook source
# MAGIC %run "/Shared/Common/Functions/KeyVaultFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/StorageFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/DWHFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/SchemaTransformFunctions"

# COMMAND ----------

# DBTITLE 1,Import References
from pyspark.sql.functions import *
import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Parameters
# Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("TransformInstanceID","")
dbutils.widgets.text("InputRawBlobContainer", "")
dbutils.widgets.text("InputRawBlobFolder", "")
dbutils.widgets.text("InputRawBlobFile", "")
dbutils.widgets.text("InputRawFileDelimiter", "")
dbutils.widgets.text("InputFileHeaderFlag", "true")

dbutils.widgets.text("OutputCuratedBlobContainer", "")
dbutils.widgets.text("OutputCuratedBlobFolder", "")
dbutils.widgets.text("OutputCuratedBlobFile", "")
dbutils.widgets.text("OutputCuratedFileDelimiter", "")

dbutils.widgets.dropdown(name="OutputCuratedFileFormat", defaultValue="parquet", choices=["csv", "parquet", "orc", "json"]) 
dbutils.widgets.dropdown(name="OutputCuratedFileWriteMode", defaultValue="overwrite", choices=["append", "overwrite", "ignore", "error", "errorifexists"]) 

dbutils.widgets.text("DWStagingTable", "")
dbutils.widgets.text("LookupColumns", "")
dbutils.widgets.text("DeltaName","")
dbutils.widgets.text("DWTable", "")
dbutils.widgets.dropdown(name="DWTableWriteMode", defaultValue="append", choices=["append", "overwrite", "ignore", "error", "errorifexists"])

dbutils.widgets.text("CustomParameters", "")

# Get Parameters
transformInstanceID = dbutils.widgets.get("TransformInstanceID")

inputRawBlobContainer = dbutils.widgets.get("InputRawBlobContainer")
inputRawBlobFolder = dbutils.widgets.get("InputRawBlobFolder")
inputRawBlobFile = dbutils.widgets.get("InputRawBlobFile")
inputRawFileDelimiter = dbutils.widgets.get("InputRawFileDelimiter")
inputFileHeaderFlag = dbutils.widgets.get("InputFileHeaderFlag")

outputCuratedBlobContainer = dbutils.widgets.get("OutputCuratedBlobContainer")
outputCuratedBlobFolder = dbutils.widgets.get("OutputCuratedBlobFolder")
outputCuratedBlobFile = dbutils.widgets.get("OutputCuratedBlobFile")
outputCuratedFileDelimiter = dbutils.widgets.get("OutputCuratedFileDelimiter")
outputCuratedFileFormat = dbutils.widgets.get("OutputCuratedFileFormat")
outputCuratedFileWriteMode = dbutils.widgets.get("OutputCuratedFileWriteMode")

outputDWStagingTable = dbutils.widgets.get("DWStagingTable")
lookupColumns  = dbutils.widgets.get("LookupColumns")
deltaName = dbutils.widgets.get("DeltaName")
outputDWTable = dbutils.widgets.get("DWTable")
outputDWTableWriteMode = dbutils.widgets.get("DWTableWriteMode")

customParameters = dbutils.widgets.get("CustomParameters")

if inputFileHeaderFlag =="1" or inputFileHeaderFlag.lower() == "true":
  inputFileHeaderFlag = "true"
else:
  inputFileHeaderFlag = "false"

# COMMAND ----------

# DBTITLE 1,Raw Dataframe
df=readRawFile(inputRawBlobContainer, inputRawBlobFolder, inputRawBlobFile, inputRawFileDelimiter, inputFileHeaderFlag)
sourceCount = df.count()
print("sourceCount={}".format(sourceCount))

# COMMAND ----------

# DBTITLE 1,Rename Invalid Columns for Parquet files
if outputCuratedFileFormat == 'parquet':
  for c in df.columns:
      df = df.withColumnRenamed(c, c.replace(" ", ""))
else:
  None

# COMMAND ----------

# DBTITLE 1,Transform Dataframe Schema
df = validateSchema(df, outputDWTable)

# COMMAND ----------

# DBTITLE 1,Audit Data Points
l1TransformCount = df.count()
if l1TransformCount >0:
  rawFile = getRawFile(inputRawBlobContainer, inputRawBlobFolder, inputRawBlobFile)
  df= df.withColumn("ELT_TransformInstanceID",lit(transformInstanceID).cast("integer")) \
        .withColumn("ELT_SourceFile",lit(rawFile))\
        .withColumn("ELT_Timestamp",lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
#   display(df)

# COMMAND ----------

# DBTITLE 1,Upsert DWH (Conditional)
#  Upsert Criteria
# 1. Dataframe has records
# 2. Staging Table, Lookup columns and Target Table available/configured
if l1TransformCount>0 and outputDWStagingTable is not None and len(outputDWStagingTable) >0 and lookupColumns is not None and len(lookupColumns)>0 and outputDWTable is not None and len(outputDWTable) >0 :
  print("Upserting Data to DWH")
  upsertDWH(df,outputDWStagingTable,outputDWTable,lookupColumns,deltaName)
  print("Upsert to DWH completed")
else:
   print("Upserting Skipped")

# COMMAND ----------

# DBTITLE 1,Insert DWH
# Insert Criteria
# 1. Dataframe has records
# 2. Staging Table and  Lookup columns not avaialble
# 3. Target Table is available/configured
if l1TransformCount>0 and (outputDWStagingTable is None or len(outputDWStagingTable) ==0) and outputDWTable is not None and len(outputDWTable) >0 :
  print("Inserting to DWH")
  insertDWH(df,outputDWTable,outputDWTableWriteMode)
  print("Insert to DWH completed")
else:
  print("Inserting Skipped")

# COMMAND ----------

# DBTITLE 1,Write to Data Lake - L1Curated 
if l1TransformCount >0:
  writeC1File(df,outputCuratedBlobContainer, outputCuratedBlobFolder, outputCuratedBlobFile, outputCuratedFileFormat, outputCuratedFileWriteMode,outputCuratedFileDelimiter)
# display(df)

# COMMAND ----------

# DBTITLE 1,Return Values
import json
dbutils.notebook.exit(json.dumps({
  "sourceCount": sourceCount,
  "l1TransformCount": l1TransformCount
}))

# COMMAND ----------



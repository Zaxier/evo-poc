# Databricks notebook source
# MAGIC %run "/Shared/Common/Functions/KeyVaultFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/StorageFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/DWHFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/SchemaTransformFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/JSONFunctions

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

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

dbutils.widgets.dropdown(name="OutputCuratedFileFormat", defaultValue="parquet", choices=["csv", "parquet", "orc", "json", "delta"]) 
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

df=readRawFile(inputRawBlobContainer, inputRawBlobFolder, inputRawBlobFile, inputRawFileDelimiter, inputFileHeaderFlag)
sourceCount = df.count()
print("sourceCount={}".format(sourceCount))

# COMMAND ----------

df= json_flatten(df)

# COMMAND ----------

df.display()

# COMMAND ----------

if outputCuratedFileFormat in ['parquet','delta']:
  for c in df.columns:
      df = df.withColumnRenamed(c, c.replace(" ", ""))
else:
  None

# COMMAND ----------

df = validateSchema(df, outputDWTable)

# COMMAND ----------

company_code=inputRawBlobContainer.upper()

l1TransformCount = df.count()
if l1TransformCount >0:
  rawFile = getRawFile(inputRawBlobContainer, inputRawBlobFolder, inputRawBlobFile)
  df= df.withColumn("company_code",lit(company_code))\
        .withColumn("ELT_TransformInstanceID",lit(transformInstanceID).cast("integer")) \
        .withColumn("ELT_SourceFile",lit(rawFile))\
        .withColumn("ELT_Timestamp",lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
#   display(df)

# COMMAND ----------

def writeC1File(df,container, folder=None, file=None, fileFormat=None, writeMode=None,colSeparator=None):
  # ##########################################################################################################################  
  # Function: writeC1File
  # Writes the input dataframe to a file in curated1 zone of Azure Blob
  # 
  # Parameters:
  # df= input dataframe
  # container = File System/Container of Azure Blob Storage
  # folder = folder name
  # file = file name including extension
  # fileFormat = File extension. Supported formats are csv/txt/parquet/orc/json  
  # writeMode= mode of writing the curated file. Allowed values - append/overwrite/ignore/error/errorifexists
  # colSeparator = Column separator for text files
  # 
  # Returns:
  # A dataframe of the raw file
  # ########################################################################################################################## 
  c1File= getC1File(container, folder, file)
  if "csv" in fileFormat or 'txt' in fileFormat:
    df.write.csv(c1File,mode=writeMode,sep=colSeparator,header="true", nullValue="0", timestampFormat ="yyyy-MM-dd HH:mm:ss")
  elif "parquet" in fileFormat:
    df.write.parquet(c1File,mode=writeMode)
  elif "orc" in fileFormat:
    df.write.orc(c1File,mode=writeMode)
  elif "json" in fileFormat:
    df.write.json(c1File, mode=writeMode)
  elif "delta" in fileFormat:
    df.write.format("delta").mode(writeMode).save(c1File)
  else:
    df.write.save(path=c1File,format=fileFormat,mode=writeMode)
  return

# COMMAND ----------

if l1TransformCount >0:
  writeC1File(df,outputCuratedBlobContainer, outputCuratedBlobFolder, outputCuratedBlobFile, outputCuratedFileFormat, outputCuratedFileWriteMode,outputCuratedFileDelimiter)
# display(df) 

# COMMAND ----------



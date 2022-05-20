# Databricks notebook source
# DBTITLE 1,readRawFile Function
from tkinter.tix import Select


zone_dict = {
    "c1": (
        getSecret("Curated2ZoneStorageAccountName"),
        getSecret("Curated2ZoneStorageAccessKey")
    )
}

def getQualifiedFile(zone, container, folder, file):
  spark.conf.set("fs.azure.account.key." + zone + ".blob.core.windows.net", curated2ZoneKey)
  if folder is not None and file is not None:
    fileName ="wasbs://" + container + "@"+ zone + ".blob.core.windows.net/" + folder + "/" + file
  elif folder is None and file is not None:
    fileName ="wasbs://" + container + "@"+ zone + ".blob.core.windows.net/" + file
  elif folder is not None and file is None:
    fileName ="wasbs://" + container + "@"+ zone + ".blob.core.windows.net/" + folder
  return fileName



def readRawFile(zone, 
    container, 
    folder=None, 
    file=None, colSeparator=None, headerFlag=None, fileType=None):
  # ##########################################################################################################################  
  # Function: readRawFile
  # Reads a file from raw zone of Azure Blob Storage and returns as dataframe
  # 
  # Parameters:
  # container = File System/Container of Azure Blob Storage
  # folder = folder name
  # file = file name including extension
  # colSeparator = Column separator for text files
  # headerFlag = boolean flag to indicate whether the text file has a header or not  
  # 
  # Returns:
  # A dataframe of the raw file
  # ##########################################################################################################################    
  rawFile = getQualifiedFile(zone, container, folder, file)
  if ".csv" in rawFile or ".txt" in rawFile:
    df = spark.read.csv(path=rawFile, sep=colSeparator, header=headerFlag, inferSchema="true", multiLine="true")
  elif ".parquet" in rawFile:
    df = spark.read.parquet(rawFile)
  elif ".orc" in rawFile:
    df = spark.read.orc(rawFile)
  elif ".json" in rawFile:
    df = spark.read.json(rawFile)
  else:
    df = spark.read.format("csv").load(rawFile)
  
  df =df.dropDuplicates()
  return df



processA/month/day/hour/1.csv
processA/month/day/hour/2.csv
processA/month/day/hour/3.csv
processA/month/day/hour/4.csv
processA/month/day/hour/5.csv
processA/month/day/hour/6.csv

point autoloader at processA/


processB/month/day/hour/1.csv
processB/month/day/hour/2.csv
processB/month/day/hour/3.csv
processB/month/day/hour/4.csv
processB/month/day/hour/5.csv
processB/month/day/hour/6.csv

point autoloader at processB/


df.write.format("delta").partitionBy(["month", "day"])...

processA_output/ _delta_log/

processA_
_delta_log/ ... statistics

processA_output/ ...parquet # feb min/max
processA_output/ ...parquet # min/max 
processA_output/ ...parquet # feb
processA_output/ ...parquet min 7 -100
processA_output/ ...parquet
processA_output/ ...parquet # feb var>50

df = spark.sql("select * from table_name")
df.write(tablename)

Select
WHERE 6-10



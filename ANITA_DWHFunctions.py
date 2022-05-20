# Databricks notebook source
# MAGIC %run "/Shared/Common/Functions/KeyVaultFunctions"

# COMMAND ----------

# MAGIC %run "/Shared/Common/Functions/StorageFunctions"

# COMMAND ----------

# DBTITLE 1,readTableDWH Function
def readTableDWH(dwhTable):
  # ##########################################################################################################################  
  # Function: readTableDWH
  # Reads all the rows and columns of an Azure Datawarehouse table/view and returns the records as dataframe
  # 
  # Parameters:
  # dwhTable = Input Table/View including the schema name. E.g soccer.goalpost, afl.goalpost
  #
  # Returns:
  # Dataframe containing all rows and columns of a table/view in Azure Datawarehouse
  # ##########################################################################################################################  
  
  sqldwJDBC = getSecret('JDBCConnectDW')
  tempSQLDWFolder = getTempSQLDWFolder()
  df = spark.read\
      .format("com.databricks.spark.sqldw")\
      .option("url",sqldwJDBC )\
      .option("tempDir",tempSQLDWFolder )\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhTable).load()
  return df

# COMMAND ----------

# DBTITLE 1,readQueryDWH Function
def readQueryDWH(dwhQuery):
  # ##########################################################################################################################  
  # Function: readQueryDWH
  # Executes the input query at Azure Datawarehouse and returns the records as dataframe
  # 
  # Parameters:
  # dwhQuery = A valid input query
  #
  # Returns:
  # Dataframe containing all rows and columns returned by query from Azure Datawarehouse
  # ##########################################################################################################################  
  
  sqldwJDBC = getSecret('JDBCConnectDW')
  tempSQLDWFolder = getTempSQLDWFolder()
  df = spark.read\
      .format("com.databricks.spark.sqldw")\
      .option("url",sqldwJDBC )\
      .option("tempDir",tempSQLDWFolder )\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("query",dwhQuery).load()
  return df

# COMMAND ----------

# DBTITLE 1,upsertDWH Function
def upsertDWH(df,dwhStagingTable,dwhTargetTable,lookupColumns,deltaName,dwhStagingDistributionColumn=None):
    # ##########################################################################################################################  
    # Function: upsertDWH
    # Performs a Merge/Upsert action on a Azure Datawarehouse table
    # 
    # Parameters:
    # df = Input dataframe
    # dwhStagingTable = Azure Datawarehouse Table used to stage the input dataframe for merge/upsert operation
    # dwhTargetTable = Azure Datawarehouse Target table where the dataframe is merged/upserted
    # lookupColumns = pipe separated columns that uniquely defines a record in input dataframe
    # deltaName = Name of watermark column in input dataframe
    # dwhStagingDistributionColumn = Name of the column used as hash distribution column in staging table of DWH. 
    #                                This column will help improve upsert performance by minimizing data movement
    #                                provided the dwhTargetTable is also hash distributed on the same column.
    #
    # Returns:
    # None
    # ##########################################################################################################################  
    print("Upsert to {} started ...".format(dwhTargetTable))
   
    sqldwJDBC = getSecret('JDBCConnectDW')
    tempSQLDWFolder = getTempSQLDWFolder()
    
    #Create schema_temp table as workaround to preserve staging table schema required by SQL DW to prevent data movement and hence prevent performance degradation.
    #This is a tactical fix to cover bug in SQL DW
    
    truncateSQL = "TRUNCATE TABLE " + dwhStagingTable + ";"
    
    #Writing only 1 record to STG*_schema_temp as the staging table is only used for preActions
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhStagingTable + "_schema_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("preActions", truncateSQL)\
      .mode("overwrite").save()
    
    df.write.format("com.databricks.spark.sqldw")\
         .option("url", sqldwJDBC)\
         .option("forwardSparkAzureStorageCredentials", "true")\
         .option("dbTable",dwhStagingTable)\
         .option("tempDir",tempSQLDWFolder)\
         .option("maxStrLength",4000)\
         .mode("append")\
         .save()
    
    #UPSERT/MERGE
    #Derive dynamic delete statement to delete existing record from TARGET if the source record is newer
    lookupCols =lookupColumns.split("|")
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhStagingTable  +".["+ col  + "]="+ dwhTargetTable +".[" + col + "] and "

    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is greater than existing record
      whereClause= whereClause + dwhStagingTable  +".["+ deltaName  + "] >="+ dwhTargetTable +".[" + deltaName +"]"
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]

    #print(whereClause)
    deleteSQL = "delete from " + dwhTargetTable + " where exists (select 1 from " + dwhStagingTable + " where " +whereClause +");"
    #print("deleteSQL={}".format(deleteSQL))

    #Delete existing records but outdated records from SOURCE
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhTargetTable  +".["+ col  + "]="+ dwhStagingTable +".[" + col + "] and "

    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is lesser than existing record
      whereClause= whereClause + dwhTargetTable  +".["+ deltaName  + "] > "+ dwhStagingTable +".[" + deltaName +"]"
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]

    deleteOutdatedSQL = "delete from " + dwhStagingTable + " where exists (select 1 from " + dwhTargetTable + " where " + whereClause + " );"
    #print("deleteOutdatedSQL={}".format(deleteOutdatedSQL))

    #Insert SQL
    insertSQL ="Insert Into " + dwhTargetTable + " select * from " + dwhStagingTable +";"
    #print("insertSQL={}".format(insertSQL))

    #consolidate post actions SQL
    postActionsSQL = deleteSQL + deleteOutdatedSQL + insertSQL
    print("postActionsSQL={}".format(postActionsSQL))
 
    #Commented out stgTableOptions because of schema_temp table workaround , this is not relevant anymore
    ##Use Hash Distribution on STG table where possible
    #if dwhStagingDistributionColumn is not None and len(dwhStagingDistributionColumn) > 0:
     #stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  dwhStagingDistributionColumn + ")"
    #else:
      #stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
    
    #Upsert/Merge to Target using STG*_upsert_temp postActions. 
    #Writing only 1 record to STG*_upsert_temp as the staging table is only used for postActions
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC).option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhStagingTable+"_upsert_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("postActions",postActionsSQL)\
      .mode("overwrite").save()
    
    print("Upsert to {} completed".format(dwhTargetTable))

# COMMAND ----------

# DBTITLE 1,upsertSFDWH Function
def upsertSFDWH(df,dwhStagingTable,dwhTargetTable,lookupColumns,deltaName,dwhStagingDistributionColumn=None):
    # ##########################################################################################################################  
    # Function: upsertDWH
    # Performs a Merge/Upsert action on a Azure Datawarehouse table
    # 
    # Parameters:
    # df = Input dataframe
    # dwhStagingTable = Azure Datawarehouse Table used to stage the input dataframe for merge/upsert operation
    # dwhTargetTable = Azure Datawarehouse Target table where the dataframe is merged/upserted
    # lookupColumns = pipe separated columns that uniquely defines a record in input dataframe
    # deltaName = Name of watermark column in input dataframe
    # dwhStagingDistributionColumn = Name of the column used as hash distribution column in staging table of DWH. 
    #                                This column will help improve upsert performance by minimizing data movement
    #                                provided the dwhTargetTable is also hash distributed on the same column.
    #
    # Returns:
    # None
    # ##########################################################################################################################  
    print("Upsert to {} started ...".format(dwhTargetTable))
   
    sqldwJDBC = getSecret('JDBCConnectDW')
    tempSQLDWFolder = getTempSQLDWFolder()
    
    #Create schema_temp table as workaround to preserve staging table schema required by SQL DW to prevent data movement and hence prevent performance degradation.
    #This is a tactical fix to cover bug in SQL DW
    
    truncateSQL = "TRUNCATE TABLE " + dwhStagingTable + ";"

    
    #Writing only 1 record to STG*_schema_temp as the staging table is only used for preActions
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhStagingTable + "_schema_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("preActions", truncateSQL)\
      .mode("overwrite").save()
    
    df.write.format("com.databricks.spark.sqldw")\
         .option("url", sqldwJDBC)\
         .option("forwardSparkAzureStorageCredentials", "true")\
         .option("dbTable",dwhStagingTable)\
         .option("tempDir",tempSQLDWFolder)\
         .option("maxStrLength",4000)\
         .mode("append")\
         .save()
    
    #UPSERT/MERGE
    #Derive dynamic delete statement to delete existing record from TARGET if the source record is newer
    lookupCols =lookupColumns.split("|")
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhStagingTable  +".["+ col  + "]="+ dwhTargetTable +".[" + col + "] and "

    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is greater than existing record
      whereClause= whereClause + dwhStagingTable  +".["+ deltaName  + "] >="+ dwhTargetTable +".[" + deltaName +"]"
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]

    #print(whereClause)
    deleteSQL = "update " + dwhTargetTable + " set ELT_ActiveFlag = 0 where exists (select 1 from " + dwhStagingTable + " where " +whereClause +");"
    #print("deleteSQL={}".format(deleteSQL))

    #Delete existing records but outdated records from SOURCE
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhTargetTable  +".["+ col  + "]="+ dwhStagingTable +".[" + col + "] and "

    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is lesser than existing record
      whereClause= whereClause + dwhTargetTable  +".["+ deltaName  + "] > "+ dwhStagingTable +".[" + deltaName +"]"
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]

    deleteOutdatedSQL = "update " + dwhStagingTable + " set ELT_ActiveFlag = 0 where exists (select 1 from " + dwhTargetTable + " where " + whereClause + " );"
    #print("deleteOutdatedSQL={}".format(deleteOutdatedSQL))

    #Insert SQL
    insertSQL ="Insert Into " + dwhTargetTable + " select * from " + dwhStagingTable +";"
    #print("insertSQL={}".format(insertSQL))

    #consolidate post actions SQL
    postActionsSQL = deleteSQL + deleteOutdatedSQL + insertSQL
    print("postActionsSQL={}".format(postActionsSQL))
 
    #Commented out stgTableOptions because of schema_temp table workaround , this is not relevant anymore
    ##Use Hash Distribution on STG table where possible
    #if dwhStagingDistributionColumn is not None and len(dwhStagingDistributionColumn) > 0:
     #stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  dwhStagingDistributionColumn + ")"
    #else:
      #stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
    
    #Upsert/Merge to Target using STG*_upsert_temp postActions. 
    #Writing only 1 record to STG*_upsert_temp as the staging table is only used for postActions
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC).option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhStagingTable+"_upsert_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("postActions",postActionsSQL)\
      .mode("overwrite").save()
    
    print("Upsert to {} completed".format(dwhTargetTable))

# COMMAND ----------

# DBTITLE 1,insertDWH Function
def insertDWH(df,dwhTable=None,writeMode=None,dwhTempTable=None,preActionsSQL=None,postActionsSQL=None,hashColumn=None):
    #df,dwhTable,writeMode
    # ##########################################################################################################################  
    # Function: insertDWH
    # Inserts a dataframe to a Azure Datawarehouse table
    # 
    # Parameters:
    # df = Input dataframe
    # dwhTable = Target table where the datafframe is loaded
    # writeMode = Describes how data from dataframe is inserted to datawarehouse table. Allowed values are - append/overwrite/ignore/error/errorifexists
    # dwhTempTable = Azure Datawarehouse Table used to stage Data for a Databricks Notebook.
    # preActionsSQL = SQL Script to be excuted before insert into Staging Table
    # postActionsSQL = SQL Script to be Excuted following insert into Staging Table
    # hashColumn = Name of the column used as hash distribution column in staging table of DWH. 
    #                                This column will help improve upsert performance by minimizing data movement
    #                                provided the dwhTargetTable is also hash distributed on the same column.
    # Returns:
    # None
    # ########################################################################################################################## 
  sqldwJDBC = getSecret('JDBCConnectDW')
  tempSQLDWFolder = getTempSQLDWFolder()
  
#Staging Table Options
  if hashColumn is not None and len(hashColumn) > 0:
    stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  hashColumn + ")"
  if hashColumn is None or len(hashColumn) == 0:
    stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
  
#Insert to DWH Table
  if writeMode == "overwrite" and dwhTable is not None:
    #If write mode is overwite, truncate and append instead of default overwrite to prevent databricks overriding the user defined datatypes and string lengths
    truncateSQL = "TRUNCATE TABLE " + dwhTable + ";"
    print("Truncating Synapse Table : {} using : {}".format(dwhTable,"STG_"+ dwhTable))
    df.write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable","STG_"+ dwhTable)\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("preActions", truncateSQL)\
      .mode("overwrite").save()
    
    print("Appending Synapse Table : {}".format(dwhTable)) 
    df.write.format("com.databricks.spark.sqldw")\
         .option("url", sqldwJDBC)\
         .option("forwardSparkAzureStorageCredentials", "true")\
         .option("dbTable",dwhTable)\
         .option("tempDir",tempSQLDWFolder)\
         .option("maxStrLength",4000)\
         .mode("append")\
         .save()
    
    
#Custom Pre SQL Action Upsert
  if writeMode == "append" and dwhTable is not None and preActionsSQL is not None and postActionsSQL is None:
    print("Executing Pre SQL Action: {} using: {}".format(preActionsSQL,"STG_" + dwhTable))
#     df.limit(1).write.format("com.databricks.spark.sqldw")\
#       .option("url", sqldwJDBC).option("forwardSparkAzureStorageCredentials", "true")\
#       .option("dbTable","STG_" + dwhTable)\
#       .option("tableOptions",stgTableOptions)\
#       .option("tempDir",tempSQLDWFolder)\
#       .option("maxStrLength",4000)\
#       .option("preActions",preActionsSQL)\
#       .mode("overwrite").save()
    
    print("Appending Synapse Table: {}".format(dwhTable))
    df.write.format("com.databricks.spark.sqldw")\
          .option("url", sqldwJDBC)\
          .option("forwardSparkAzureStorageCredentials", "true")\
          .option("dbTable",dwhTable)\
          .option("tempDir",tempSQLDWFolder)\
          .option("maxStrLength",4000)\
          .option("preActions",preActionsSQL)\
          .mode(writeMode)\
          .save()

    
#Custom Post SQL Action Upsert
  if writeMode == "append" and dwhTable is not None and postActionsSQL is not None and preActionsSQL is None:
    print("Inserting Staging Table {}/{}/{} and Executing Post SQL Action".format(dwhTable + "_schema_temp","STG_"+dwhTable, dwhTable))
    #Truncate Staging Table
    truncateSQL = "TRUNCATE TABLE STG_" +dwhTable + ";"
    #Writing only 1 record to STG*_schema_temp as the staging table is only used for preActions
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC)\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhTable + "_schema_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("preActions", truncateSQL)\
      .mode("overwrite").save()
    
    df.write.format("com.databricks.spark.sqldw")\
       .option("url", sqldwJDBC)\
       .option("forwardSparkAzureStorageCredentials", "true")\
       .option("dbTable","STG_"+dwhTable)\
       .option("tempDir",tempSQLDWFolder)\
       .option("maxStrLength",4000)\
       .mode("append")\
       .save()
    
    df.limit(1).write.format("com.databricks.spark.sqldw")\
      .option("url", sqldwJDBC).option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhTable+"_postSql_temp")\
      .option("tempDir",tempSQLDWFolder)\
      .option("maxStrLength",4000)\
      .option("postActions",postActionsSQL)\
      .mode("overwrite").save()
    
#Insert to Temp Table Only
  if writeMode == "overwrite" and dwhTable is None and dwhTempTable is not None and postActionsSQL is None and preActionsSQL is None:
    print("Inserting Into Temp Table {} - Databricks Use Only".format(dwhTempTable))    
    df.write.format("com.databricks.spark.sqldw")\
     .option("url", sqldwJDBC).option("forwardSparkAzureStorageCredentials", "true")\
     .option("dbTable",dwhTempTable)\
     .option("tableOptions",stgTableOptions)\
     .option("tempDir",tempSQLDWFolder)\
     .option("maxStrLength",4000)\
     .mode("overwrite").save()
  
#Append to DWH Table
  if writeMode == "append" and dwhTable is not None and dwhTempTable is None and postActionsSQL is None and preActionsSQL is None:
    print("Appending Synapse Table : {}".format(dwhTable))
    df.write.format("com.databricks.spark.sqldw")\
          .option("url", sqldwJDBC)\
          .option("forwardSparkAzureStorageCredentials", "true")\
          .option("dbTable",dwhTable)\
          .option("tempDir",tempSQLDWFolder)\
          .option("maxStrLength",4000)\
          .mode(writeMode)\
          .save()

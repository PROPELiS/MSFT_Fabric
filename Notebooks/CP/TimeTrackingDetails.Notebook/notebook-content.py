# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aabf914c-0501-4c58-ba5b-4b0f05f4420f",
# META       "default_lakehouse_name": "SILVER",
# META       "default_lakehouse_workspace_id": "c8d75176-b949-4f7e-a658-b996603ec8c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "aabf914c-0501-4c58-ba5b-4b0f05f4420f"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

in_mode = "FULL" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import concat_ws, expr, sha2, size, lit, col, array, struct, udf, current_timestamp, max as spark_max
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *#
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
orderschema = StructType([
    StructField("EmployeeNumber", StringType(), True),
    StructField("CalyearMonth", StringType(), True),
    StructField("SimplifiedGroupName", StringType(), True),
    StructField("PortfolioGroupName", StringType(), True),
    StructField("Hours", StringType(), True),
    StructField("Percentage", StringType(), True),
    StructField("Comments", StringType(), True),
    StructField("NonClientActivityTotalHours", StringType(), True),
    StructField("SaveDraftInd", StringType(), True),
    StructField("CreatedOn", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("LastUpdatedOn", StringType(), True),
    StructField("LastUpdatedBy", StringType(), True),
    StructField("SerialRowNum", StringType(), True),
    StructField("ClientGeography", StringType(), True),
    StructField("ClientDivision", StringType(), True),
    StructField("RevenueStream", StringType(), True),
    StructField("SubService", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Empty Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define new schema with only the required columns
schema = StructType([
    StructField("EmployeeNumber", StringType(), True),
    StructField("CalyearMonth", StringType(), True),
    StructField("SimplifiedGroupName", StringType(), True),
    StructField("PortfolioGroupName", StringType(), True),
    StructField("Hours", StringType(), True),
    StructField("Percentage", StringType(), True),
    StructField("Comments", StringType(), True),
    StructField("NonClientActivityTotalHours", StringType(), True),
    StructField("SaveDraftInd", StringType(), True),
    StructField("CreatedOn", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("LastUpdatedOn", StringType(), True),
    StructField("LastUpdatedBy", StringType(), True),
    StructField("SerialRowNum", StringType(), True),
    StructField("ClientGeography", StringType(), True),
    StructField("ClientDivision", StringType(), True),
    StructField("RevenueStream", StringType(), True),
    StructField("SubService", StringType(), True)
])



df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/CP/timetrackingdetails"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("CP.TimeTrackingDetails")
    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/TimeTracking_Details"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    # Merge data from Bronze to Silver with only the specified columns
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.EmployeeNumber = source.EmployeeNumber"
).whenMatchedUpdate(set={
    "EmployeeNumber": col("source.EmployeeNumber"),
    "CalyearMonth": col("source.CalyearMonth"),
    "SimplifiedGroupName": col("source.SimplifiedGroupName"),
    "PortfolioGroupName": col("source.PortfolioGroupName"),
    "Hours": col("source.Hours"),
    "Percentage": col("source.Percentage"),
    "Comments": col("source.Comments"),
    "NonClientActivityTotalHours": col("source.NonClientActivityTotalHours"),
    "SaveDraftInd": col("source.SaveDraftInd"),
    "CreatedOn": col("source.CreatedOn"),
    "CreatedBy": col("source.CreatedBy"),
    "LastUpdatedOn": col("source.LastUpdatedOn"),
    "LastUpdatedBy": col("source.LastUpdatedBy"),
    "SerialRowNum": col("source.SerialRowNum"),
    "ClientGeography": col("source.ClientGeography"),
    "ClientDivision": col("source.ClientDivision"),
    "RevenueStream": col("source.RevenueStream"),
    "SubService": col("source.SubService")
}).whenNotMatchedInsert(values={
    "EmployeeNumber": col("source.EmployeeNumber"),
    "CalyearMonth": col("source.CalyearMonth"),
    "SimplifiedGroupName": col("source.SimplifiedGroupName"),
    "PortfolioGroupName": col("source.PortfolioGroupName"),
    "Hours": col("source.Hours"),
    "Percentage": col("source.Percentage"),
    "Comments": col("source.Comments"),
    "NonClientActivityTotalHours": col("source.NonClientActivityTotalHours"),
    "SaveDraftInd": col("source.SaveDraftInd"),
    "CreatedOn": col("source.CreatedOn"),
    "CreatedBy": col("source.CreatedBy"),
    "LastUpdatedOn": col("source.LastUpdatedOn"),
    "LastUpdatedBy": col("source.LastUpdatedBy"),
    "SerialRowNum": col("source.SerialRowNum"),
    "ClientGeography": col("source.ClientGeography"),
    "ClientDivision": col("source.ClientDivision"),
    "RevenueStream": col("source.RevenueStream"),
    "SubService": col("source.SubService")
}).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("CP.TimeTrackingDetails")
    
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/TimeTrackingDetails"
    source_df_delta = spark.read.format("delta").load(source_path)

    silver_table.alias("target").merge(
    source_df_delta.alias("source"),
    "target.EmployeeNumber = source.EmployeeNumber"
).whenMatchedUpdate(set={
    "EmployeeNumber": col("source.EmployeeNumber"),
    "CalyearMonth": col("source.CalyearMonth"),
    "SimplifiedGroupName": col("source.SimplifiedGroupName"),
    "PortfolioGroupName": col("source.PortfolioGroupName"),
    "Hours": col("source.Hours"),
    "Percentage": col("source.Percentage"),
    "Comments": col("source.Comments"),
    "NonClientActivityTotalHours": col("source.NonClientActivityTotalHours"),
    "SaveDraftInd": col("source.SaveDraftInd"),
    "CreatedOn": col("source.CreatedOn"),
    "CreatedBy": col("source.CreatedBy"),
    "LastUpdatedOn": col("source.LastUpdatedOn"),
    "LastUpdatedBy": col("source.LastUpdatedBy"),
    "SerialRowNum": col("source.SerialRowNum"),
    "ClientGeography": col("source.ClientGeography"),
    "ClientDivision": col("source.ClientDivision"),
    "RevenueStream": col("source.RevenueStream"),
    "SubService": col("source.SubService")
}).whenNotMatchedInsert(values={
    "EmployeeNumber": col("source.EmployeeNumber"),
    "CalyearMonth": col("source.CalyearMonth"),
    "SimplifiedGroupName": col("source.SimplifiedGroupName"),
    "PortfolioGroupName": col("source.PortfolioGroupName"),
    "Hours": col("source.Hours"),
    "Percentage": col("source.Percentage"),
    "Comments": col("source.Comments"),
    "NonClientActivityTotalHours": col("source.NonClientActivityTotalHours"),
    "SaveDraftInd": col("source.SaveDraftInd"),
    "CreatedOn": col("source.CreatedOn"),
    "CreatedBy": col("source.CreatedBy"),
    "LastUpdatedOn": col("source.LastUpdatedOn"),
    "LastUpdatedBy": col("source.LastUpdatedBy"),
    "SerialRowNum": col("source.SerialRowNum"),
    "ClientGeography": col("source.ClientGeography"),
    "ClientDivision": col("source.ClientDivision"),
    "RevenueStream": col("source.RevenueStream"),
    "SubService": col("source.SubService")
}).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

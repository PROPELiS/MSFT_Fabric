# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59aba330-314f-4ff0-8c5b-ad0582b3dc9e",
# META       "default_lakehouse_name": "SILVER",
# META       "default_lakehouse_workspace_id": "de3e35d4-28a5-4df0-a8d1-00feff73469d"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

in_mode = "FULL"  # Replace with the actual IN_MODE value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat_ws, expr, sha2, size, lit, col, array, struct, udf, current_timestamp, max as spark_max
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_JobTaskHistoryId", StringType(), True),
    StructField("JobTaskHistoryId", StringType(), True),
    StructField("JobTaskId", StringType(), True),
    StructField("JobTaskHistoryStatus", StringType(), True),
    StructField("JobTaskHistoryUserId", StringType(), True),
    StructField("JobTaskHistoryDateTime", TimestampType(), True),
    StructField("JobTaskHistoryWorkHours", DoubleType(), True),
    StructField("Comments", StringType(), True),
    StructField("JobTaskHistoryCalendarWorkHours", DoubleType(), True),
    StructField("SourceSystem", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    # Define the schema
schema = StructType([
   StructField("SYS_CHANGE_VERSION", StringType(), True),
   StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("JobTaskHistoryId", StringType(), True),
    StructField("JobTaskId", StringType(), True),
    StructField("JobTaskHistoryStatus", StringType(), True),
    StructField("JobTaskHistoryUserId", StringType(), True),
    StructField("JobTaskHistoryDateTime", TimestampType(), True),
    StructField("JobTaskHistoryWorkHours", DoubleType(), True),
    StructField("Comments", StringType(), True),
    StructField("JobTaskHistoryCalendarWorkHours", DoubleType(), True),
    StructField("SourceSystem", StringType(), True) 
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_task_history"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_task_history")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_task_history_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobTaskHistoryId = source.JobTaskHistoryId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskHistoryId": col("source.JobTaskHistoryId"),
        "JobTaskId": col("source.JobTaskId"),
        "JobTaskHistoryStatus": col("source.JobTaskHistoryStatus"),
        "JobTaskHistoryUserId": col("source.JobTaskHistoryUserId"),
        "JobTaskHistoryDateTime": col("source.JobTaskHistoryDateTime"),
        "JobTaskHistoryWorkHours": col("source.JobTaskHistoryWorkHours"),
        "Comments": col("source.Comments"),
        "JobTaskHistoryCalendarWorkHours": col("source.JobTaskHistoryCalendarWorkHours"),
        "SourceSystem": col("source.SourceSystem")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskHistoryId": col("source.JobTaskHistoryId"),
        "JobTaskId": col("source.JobTaskId"),
        "JobTaskHistoryStatus": col("source.JobTaskHistoryStatus"),
        "JobTaskHistoryUserId": col("source.JobTaskHistoryUserId"),
        "JobTaskHistoryDateTime": col("source.JobTaskHistoryDateTime"),
        "JobTaskHistoryWorkHours": col("source.JobTaskHistoryWorkHours"),
        "Comments": col("source.Comments"),
        "JobTaskHistoryCalendarWorkHours": col("source.JobTaskHistoryCalendarWorkHours"),
        "SourceSystem": col("source.SourceSystem")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_job_task_history")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_task_history_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobTaskHistoryId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobTaskHistoryId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobTaskHistoryId = S.CT_JobTaskHistoryId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobTaskHistoryId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobTaskHistoryId") == col("b.JobTaskHistoryId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobTaskHistoryId"),
        col("a.JobTaskId"),
        col("a.JobTaskHistoryStatus"),
        col("a.JobTaskHistoryUserId"),
        col("a.JobTaskHistoryDateTime"),
        col("a.JobTaskHistoryWorkHours"),
        col("a.Comments"),
        col("a.JobTaskHistoryCalendarWorkHours"),
        col("a.SourceSystem")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobTaskHistoryId = source.JobTaskHistoryId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskHistoryId": col("source.JobTaskHistoryId"),
        "JobTaskId": col("source.JobTaskId"),
        "JobTaskHistoryStatus": col("source.JobTaskHistoryStatus"),
        "JobTaskHistoryUserId": col("source.JobTaskHistoryUserId"),
        "JobTaskHistoryDateTime": col("source.JobTaskHistoryDateTime"),
        "JobTaskHistoryWorkHours": col("source.JobTaskHistoryWorkHours"),
        "Comments": col("source.Comments"),
        "JobTaskHistoryCalendarWorkHours": col("source.JobTaskHistoryCalendarWorkHours"),
        "SourceSystem": col("source.SourceSystem")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskHistoryId": col("source.JobTaskHistoryId"),
        "JobTaskId": col("source.JobTaskId"),
        "JobTaskHistoryStatus": col("source.JobTaskHistoryStatus"),
        "JobTaskHistoryUserId": col("source.JobTaskHistoryUserId"),
        "JobTaskHistoryDateTime": col("source.JobTaskHistoryDateTime"),
        "JobTaskHistoryWorkHours": col("source.JobTaskHistoryWorkHours"),
        "Comments": col("source.Comments"),
        "JobTaskHistoryCalendarWorkHours": col("source.JobTaskHistoryCalendarWorkHours"),
        "SourceSystem": col("source.SourceSystem")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

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
    StructField("CT_JobTaskId", StringType(), True),
    StructField("JobTaskId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("ProductionTaskId", StringType(), True),
    StructField("TaskAssignedTo", StringType(), True),
    StructField("TaskNeedsApproval", StringType(), True),
    StructField("TaskOrder", StringType(), True),
    StructField("TaskStatus", StringType(), True),
    StructField("TaskStatusDateTime", StringType(), True),
    StructField("TaskPaused", StringType(), True),
    StructField("TaskOffsite", StringType(), True),
    StructField("AccountManagerLoginId", StringType(), True),
    StructField("TaskAssignedToTeam", StringType(), True),
    StructField("EstimatedDuration", StringType(), True),
    StructField("TaskActive", StringType(), True),
    StructField("JobTaskStatus", StringType(), True),
    StructField("AITaskEstimate", StringType(), True),
    StructField("TaskDueDate", StringType(), True),
    StructField("MaterialCaptureRequired", StringType(), True)
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
    StructField("JobTaskId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("ProductionTaskId", StringType(), True),
    StructField("TaskAssignedTo", StringType(), True),
    StructField("TaskNeedsApproval", StringType(), True),
    StructField("TaskOrder", StringType(), True),
    StructField("TaskStatus", StringType(), True),
    StructField("TaskStatusDateTime", StringType(), True),
    StructField("TaskPaused", StringType(), True),
    StructField("TaskOffsite", StringType(), True),
    StructField("AccountManagerLoginId", StringType(), True),
    StructField("TaskAssignedToTeam", StringType(), True),
    StructField("EstimatedDuration", StringType(), True),
    StructField("TaskActive", StringType(), True),
    StructField("JobTaskStatus", StringType(), True),
    StructField("AITaskEstimate", StringType(), True),
    StructField("TaskDueDate", StringType(), True),
    StructField("MaterialCaptureRequired", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_job_tasks"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_job_tasks")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tasks_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
     
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobTaskId = source.JobTaskId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskId": col("source.JobTaskId"),
        "JobItemId": col("source.JobItemId"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "TaskAssignedTo": col("source.TaskAssignedTo"),
        "TaskNeedsApproval": col("source.TaskNeedsApproval"),
        "TaskOrder": col("source.TaskOrder"),
        "TaskStatus": col("source.TaskStatus"),
        "TaskStatusDateTime": col("source.TaskStatusDateTime"),
        "TaskPaused": col("source.TaskPaused"),
        "TaskOffsite": col("source.TaskOffsite"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "TaskAssignedToTeam": col("source.TaskAssignedToTeam"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "TaskActive": col("source.TaskActive"),
        "JobTaskStatus": col("source.JobTaskStatus"),
        "AITaskEstimate": col("source.AITaskEstimate"),
        "TaskDueDate": col("source.TaskDueDate"),
        "MaterialCaptureRequired": col("source.MaterialCaptureRequired")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskId": col("source.JobTaskId"),
        "JobItemId": col("source.JobItemId"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "TaskAssignedTo": col("source.TaskAssignedTo"),
        "TaskNeedsApproval": col("source.TaskNeedsApproval"),
        "TaskOrder": col("source.TaskOrder"),
        "TaskStatus": col("source.TaskStatus"),
        "TaskStatusDateTime": col("source.TaskStatusDateTime"),
        "TaskPaused": col("source.TaskPaused"),
        "TaskOffsite": col("source.TaskOffsite"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "TaskAssignedToTeam": col("source.TaskAssignedToTeam"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "TaskActive": col("source.TaskActive"),
        "JobTaskStatus": col("source.JobTaskStatus"),
        "AITaskEstimate": col("source.AITaskEstimate"),
        "TaskDueDate": col("source.TaskDueDate"),
        "MaterialCaptureRequired": col("source.MaterialCaptureRequired")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_job_tasks")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tasks_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobTaskId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobTaskId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobTaskId = S.CT_JobTaskId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobTaskId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobTaskId") == col("b.JobTaskId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobTaskId"),
        col("a.JobItemId"),
        col("a.ProductionTaskId"),
        col("a.TaskAssignedTo"),
        col("a.TaskNeedsApproval"),
        col("a.TaskOrder"),
        col("a.TaskStatus"),
        col("a.TaskStatusDateTime"),
        col("a.TaskPaused"),
        col("a.TaskOffsite"),
        col("a.AccountManagerLoginId"),
        col("a.TaskAssignedToTeam"),
        col("a.EstimatedDuration"),
        col("a.TaskActive"),
        col("a.JobTaskStatus"),
        col("a.AITaskEstimate"),
        col("a.TaskDueDate"),
        col("a.MaterialCaptureRequired")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobTaskId = source.JobTaskId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskId": col("source.JobTaskId"),
        "JobItemId": col("source.JobItemId"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "TaskAssignedTo": col("source.TaskAssignedTo"),
        "TaskNeedsApproval": col("source.TaskNeedsApproval"),
        "TaskOrder": col("source.TaskOrder"),
        "TaskStatus": col("source.TaskStatus"),
        "TaskStatusDateTime": col("source.TaskStatusDateTime"),
        "TaskPaused": col("source.TaskPaused"),
        "TaskOffsite": col("source.TaskOffsite"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "TaskAssignedToTeam": col("source.TaskAssignedToTeam"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "TaskActive": col("source.TaskActive"),
        "JobTaskStatus": col("source.JobTaskStatus"),
        "AITaskEstimate": col("source.AITaskEstimate"),
        "TaskDueDate": col("source.TaskDueDate"),
        "MaterialCaptureRequired": col("source.MaterialCaptureRequired")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTaskId": col("source.JobTaskId"),
        "JobItemId": col("source.JobItemId"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "TaskAssignedTo": col("source.TaskAssignedTo"),
        "TaskNeedsApproval": col("source.TaskNeedsApproval"),
        "TaskOrder": col("source.TaskOrder"),
        "TaskStatus": col("source.TaskStatus"),
        "TaskStatusDateTime": col("source.TaskStatusDateTime"),
        "TaskPaused": col("source.TaskPaused"),
        "TaskOffsite": col("source.TaskOffsite"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "TaskAssignedToTeam": col("source.TaskAssignedToTeam"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "TaskActive": col("source.TaskActive"),
        "JobTaskStatus": col("source.JobTaskStatus"),
        "AITaskEstimate": col("source.AITaskEstimate"),
        "TaskDueDate": col("source.TaskDueDate"),
        "MaterialCaptureRequired": col("source.MaterialCaptureRequired")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# META       "default_lakehouse_workspace_id": "de3e35d4-28a5-4df0-a8d1-00feff73469d",
# META       "known_lakehouses": [
# META         {
# META           "id": "59aba330-314f-4ff0-8c5b-ad0582b3dc9e"
# META         },
# META         {
# META           "id": "59693f16-ceb1-40c6-b096-d37b5fbbbd26"
# META         }
# META       ]
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
    StructField("CT_JobTechChecklistResponseId", StringType(), True),
    StructField("JobTechChecklistResponseId", StringType(), True),
    StructField("ChecklistItemId", StringType(), True),
    StructField("ResponseValue", StringType(), True),
    StructField("ResponseBy", StringType(), True),
    StructField("ResponseDateTime", StringType(), True),
    StructField("ChecklistRevisionId", StringType(), True),
    StructField("ItemText", StringType(), True)
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
    StructField("JobTechChecklistResponseId", StringType(), True),
    StructField("ChecklistItemId", StringType(), True),
    StructField("ResponseValue", StringType(), True),
    StructField("ResponseBy", StringType(), True),
    StructField("ResponseDateTime", StringType(), True),
    StructField("ChecklistRevisionId", StringType(), True),
    StructField("ItemText", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_tech_spec_checklist_responses"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_tech_spec_checklist_responses")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_checklist_responses_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.JobTechChecklistResponseId = source.JobTechChecklistResponseId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobTechChecklistResponseId": "source.JobTechChecklistResponseId",
        "ChecklistItemId": "source.ChecklistItemId",
        "ResponseValue": "source.ResponseValue",
        "ResponseBy": "source.ResponseBy",
        "ResponseDateTime": "source.ResponseDateTime",
        "ChecklistRevisionId": "source.ChecklistRevisionId",
        "ItemText": "source.ItemText"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobTechChecklistResponseId": "source.JobTechChecklistResponseId",
        "ChecklistItemId": "source.ChecklistItemId",
        "ResponseValue": "source.ResponseValue",
        "ResponseBy": "source.ResponseBy",
        "ResponseDateTime": "source.ResponseDateTime",
        "ChecklistRevisionId": "source.ChecklistRevisionId",
        "ItemText": "source.ItemText"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_job_tech_spec_checklist_responses")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_checklist_responses_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobTechChecklistResponseId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobTechChecklistResponseId = S.CT_JobTechChecklistResponseId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobTechChecklistResponseId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobTechChecklistResponseId") == col("b.JobTechChecklistResponseId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.JobTechChecklistResponseId").alias("JobTechChecklistResponseId"),
        col("a.ChecklistItemId").alias("ChecklistItemId"),
        col("a.ResponseValue").alias("ResponseValue"),
        col("a.ResponseBy").alias("ResponseBy"),
        col("a.ResponseDateTime").alias("ResponseDateTime"),
        col("a.ChecklistRevisionId").alias("ChecklistRevisionId"),
        col("a.ItemText").alias("ItemText")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobTechChecklistResponseId = source.JobTechChecklistResponseId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechChecklistResponseId": col("source.JobTechChecklistResponseId"),
        "ChecklistItemId": col("source.ChecklistItemId"),
        "ResponseValue": col("source.ResponseValue"),
        "ResponseBy": col("source.ResponseBy"),
        "ResponseDateTime": col("source.ResponseDateTime"),
        "ChecklistRevisionId": col("source.ChecklistRevisionId"),
        "ItemText": col("source.ItemText")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechChecklistResponseId": col("source.JobTechChecklistResponseId"),
        "ChecklistItemId": col("source.ChecklistItemId"),
        "ResponseValue": col("source.ResponseValue"),
        "ResponseBy": col("source.ResponseBy"),
        "ResponseDateTime": col("source.ResponseDateTime"),
        "ChecklistRevisionId": col("source.ChecklistRevisionId"),
        "ItemText": col("source.ItemText")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM SILVER.dbo.tbl_job_tech_spec_checklist_responses")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

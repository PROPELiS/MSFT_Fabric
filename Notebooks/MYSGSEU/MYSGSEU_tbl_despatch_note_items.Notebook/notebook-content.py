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

# Welcome to your new notebook
# Type here in the cell editor to add code!
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
    StructField("CT_DespatchNoteItemId", StringType(), True),
    StructField("DespatchNoteItemId", StringType(), True),
    StructField("DespatchNoteId", StringType(), True),
    StructField("DespatchItemDesc", StringType(), True),
    StructField("DespatchItemQty", StringType(), True),
    StructField("JobTaskId", StringType(), True),
    StructField("JobStageId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("OriginalDespatchNoteItemId", StringType(), True),
    StructField("FinalProduct", StringType(), True)
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
    StructField("DespatchNoteItemId", StringType(), True),
    StructField("DespatchNoteId", StringType(), True),
    StructField("DespatchItemDesc", StringType(), True),
    StructField("DespatchItemQty", StringType(), True),
    StructField("JobTaskId", StringType(), True),
    StructField("JobStageId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("OriginalDespatchNoteItemId", StringType(), True),
    StructField("FinalProduct", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_despatch_note_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_despatch_note_items")
    # Write the DataFrame as a Delta table
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_despatch_note_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.DespatchNoteItemId = source.DespatchNoteItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "DespatchNoteItemId": "source.DespatchNoteItemId",
        "DespatchNoteId": "source.DespatchNoteId",
        "DespatchItemDesc": "source.DespatchItemDesc",
        "DespatchItemQty": "source.DespatchItemQty",
        "JobTaskId": "source.JobTaskId",
        "JobStageId": "source.JobStageId",
        "JobItemId": "source.JobItemId",
        "OriginalDespatchNoteItemId": "source.OriginalDespatchNoteItemId",
        "FinalProduct": "source.FinalProduct"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "DespatchNoteItemId": "source.DespatchNoteItemId",
        "DespatchNoteId": "source.DespatchNoteId",
        "DespatchItemDesc": "source.DespatchItemDesc",
        "DespatchItemQty": "source.DespatchItemQty",
        "JobTaskId": "source.JobTaskId",
        "JobStageId": "source.JobStageId",
        "JobItemId": "source.JobItemId",
        "OriginalDespatchNoteItemId": "source.OriginalDespatchNoteItemId",
        "FinalProduct": "source.FinalProduct"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_despatch_note_items")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_despatch_note_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_DespatchNoteItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.DespatchNoteItemId = S.CT_DespatchNoteItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("DespatchNoteItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.DespatchNoteItemId") == col("b.DespatchNoteItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.DespatchNoteItemId").alias("DespatchNoteItemId"),
        col("a.DespatchNoteId").alias("DespatchNoteId"),
        col("a.DespatchItemDesc").alias("DespatchItemDesc"),
        col("a.DespatchItemQty").alias("DespatchItemQty"),
        col("a.JobTaskId").alias("JobTaskId"),
        col("a.JobStageId").alias("JobStageId"),
        col("a.JobItemId").alias("JobItemId"),
        col("a.OriginalDespatchNoteItemId").alias("OriginalDespatchNoteItemId"),
        col("a.FinalProduct").alias("FinalProduct")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.DespatchNoteItemId = source.DespatchNoteItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DespatchNoteItemId": col("source.DespatchNoteItemId"),
        "DespatchNoteId": col("source.DespatchNoteId"),
        "DespatchItemDesc": col("source.DespatchItemDesc"),
        "DespatchItemQty": col("source.DespatchItemQty"),
        "JobTaskId": col("source.JobTaskId"),
        "JobStageId": col("source.JobStageId"),
        "JobItemId": col("source.JobItemId"),
        "OriginalDespatchNoteItemId": col("source.OriginalDespatchNoteItemId"),
        "FinalProduct": col("source.FinalProduct")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DespatchNoteItemId": col("source.DespatchNoteItemId"),
        "DespatchNoteId": col("source.DespatchNoteId"),
        "DespatchItemDesc": col("source.DespatchItemDesc"),
        "DespatchItemQty": col("source.DespatchItemQty"),
        "JobTaskId": col("source.JobTaskId"),
        "JobStageId": col("source.JobStageId"),
        "JobItemId": col("source.JobItemId"),
        "OriginalDespatchNoteItemId": col("source.OriginalDespatchNoteItemId"),
        "FinalProduct": col("source.FinalProduct")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

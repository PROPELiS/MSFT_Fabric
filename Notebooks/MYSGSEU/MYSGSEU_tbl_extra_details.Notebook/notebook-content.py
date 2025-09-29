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
from pyspark.sql.functions import row_number


# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_ExtraDetailsId", StringType(), True),
    StructField("ExtraDetailsId", StringType(), True),
    StructField("ExtraDetailsName", StringType(), True),
    StructField("CustomerGroupId", StringType(), True),
    StructField("CaptureAuditTrail", StringType(), True),
    StructField("LockAfterInitialSave", StringType(), True)
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
    StructField("ExtraDetailsId", StringType(), True),
    StructField("ExtraDetailsName", StringType(), True),
    StructField("CustomerGroupId", StringType(), True),
    StructField("CaptureAuditTrail", StringType(), True),
    StructField("LockAfterInitialSave", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_extra_details"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_extra_details")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_extra_details_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.ExtraDetailsId = source.ExtraDetailsId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ExtraDetailsId": "source.ExtraDetailsId",
        "ExtraDetailsName": "source.ExtraDetailsName",
        "CustomerGroupId": "source.CustomerGroupId",
        "CaptureAuditTrail": "source.CaptureAuditTrail",
        "LockAfterInitialSave": "source.LockAfterInitialSave"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ExtraDetailsId": "source.ExtraDetailsId",
        "ExtraDetailsName": "source.ExtraDetailsName",
        "CustomerGroupId": "source.CustomerGroupId",
        "CaptureAuditTrail": "source.CaptureAuditTrail",
        "LockAfterInitialSave": "source.LockAfterInitialSave"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_extra_details")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_extra_details_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_ExtraDetailsId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.ExtraDetailsId = S.CT_ExtraDetailsId") \
    .whenMatchedDelete() \
    .execute()

   # Step 2: Prepare latest rows only (non-delete) using row_number
    window_spec = Window.partitionBy("ExtraDetailsId").orderBy(col("SYS_CHANGE_VERSION").cast("bigint").desc())

    transformed_df = source_df_delta \
    .filter(col("SYS_CHANGE_OPERATION") != 'D') \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select(
        col("SYS_CHANGE_VERSION").cast("bigint").alias("SYS_CHANGE_VERSION"),
        col("SYS_CHANGE_OPERATION"),
        col("ExtraDetailsId"),
        col("ExtraDetailsName"),
        col("CustomerGroupId"),
        col("CaptureAuditTrail"),
        col("LockAfterInitialSave")
    )

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ExtraDetailsId = source.ExtraDetailsId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExtraDetailsId": col("source.ExtraDetailsId"),
        "ExtraDetailsName": col("source.ExtraDetailsName"),
        "CustomerGroupId": col("source.CustomerGroupId"),
        "CaptureAuditTrail": col("source.CaptureAuditTrail"),
        "LockAfterInitialSave": col("source.LockAfterInitialSave")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExtraDetailsId": col("source.ExtraDetailsId"),
        "ExtraDetailsName": col("source.ExtraDetailsName"),
        "CustomerGroupId": col("source.CustomerGroupId"),
        "CaptureAuditTrail": col("source.CaptureAuditTrail"),
        "LockAfterInitialSave": col("source.LockAfterInitialSave")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

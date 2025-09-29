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
	StructField("CT_DAXExportId", StringType(), True),
	StructField("DAXExportId", StringType(), True),
    StructField("ExportId", StringType(), True),
    StructField("ExportStatus", StringType(), True),
    StructField("Exported", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("IsTransfer", StringType(), True)
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
   StructField("DAXExportId", StringType(), True),
   StructField("ExportId", StringType(), True),
   StructField("ExportStatus", StringType(), True),
   StructField("Exported", StringType(), True),
   StructField("CreatedDate", StringType(), True),
   StructField("IsTransfer", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_accounts_daxexport"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_accounts_DAXExport")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_accounts_DAXExport_FULL"

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.DAXExportId = source.DAXExportId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXExportId": col("source.DAXExportId"),
        "ExportId": col("source.ExportId"),
        "ExportStatus": col("source.ExportStatus"),
        "Exported": col("source.Exported"),
        "CreatedDate": col("source.CreatedDate"),
        "IsTransfer": col("source.IsTransfer")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXExportId": col("source.DAXExportId"),
        "ExportId": col("source.ExportId"),
        "ExportStatus": col("source.ExportStatus"),
        "Exported": col("source.Exported"),
        "CreatedDate": col("source.CreatedDate"),
        "IsTransfer": col("source.IsTransfer")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_accounts_DAXExport")
    
   # Define paths
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_accounts_DAXExport_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobFolderId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_DAXExportId") \
    .distinct()


   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.DAXExportId = S.CT_DAXExportId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("DAXExportId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.DAXExportId") == col("b.DAXExportId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.DAXExportId"),
        col("a.ExportId"),
        col("a.ExportStatus"),
        col("a.Exported"),
        col("a.CreatedDate"),
        col("a.IsTransfer")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.DAXExportId = source.DAXExportId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXExportId": col("source.DAXExportId"),
        "ExportId": col("source.ExportId"),
        "ExportStatus": col("source.ExportStatus"),
        "Exported": col("source.Exported"),
        "CreatedDate": col("source.CreatedDate"),
        "IsTransfer": col("source.IsTransfer")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXExportId": col("source.DAXExportId"),
        "ExportId": col("source.ExportId"),
        "ExportStatus": col("source.ExportStatus"),
        "Exported": col("source.Exported"),
        "CreatedDate": col("source.CreatedDate"),
        "IsTransfer": col("source.IsTransfer")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

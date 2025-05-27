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
    StructField("CT_KpiDataId", StringType(), True),
    StructField("KpiDataId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("KpiTypeId", StringType(), True),
    StructField("KpiEstDuration", StringType(), True),
    StructField("KpiCompleted", StringType(), True),
    StructField("KpiActDuration", StringType(), True)
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
    StructField("KpiDataId", StringType(), True), 
    StructField("JobItemId", StringType(), True), 
    StructField("KpiTypeId", StringType(), True), 
    StructField("KpiEstDuration", StringType(), True), 
    StructField("KpiCompleted", StringType(), True), 
    StructField("KpiActDuration", StringType(), True), 
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_kpi_data"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_kpi_data")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_kpi_data_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.KpiDataId = source.KpiDataId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "KpiDataId": col("source.KpiDataId"),
        "JobItemId": col("source.JobItemId"),
        "KpiTypeId": col("source.KpiTypeId"),
        "KpiEstDuration": col("source.KpiEstDuration"),
        "KpiCompleted": col("source.KpiCompleted"),
        "KpiActDuration": col("source.KpiActDuration"),
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "KpiDataId": col("source.KpiDataId"),
        "JobItemId": col("source.JobItemId"),
        "KpiTypeId": col("source.KpiTypeId"),
        "KpiEstDuration": col("source.KpiEstDuration"),
        "KpiCompleted": col("source.KpiCompleted"),
        "KpiActDuration": col("source.KpiActDuration"),
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_kpi_data")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_kpi_data_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_KpiDataId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_KpiDataId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.KpiDataId = S.CT_KpiDataId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("KpiDataId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.KpiDataId") == col("b.KpiDataId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.KpiDataId"),
        col("a.JobItemId"),
        col("a.KpiTypeId"),
        col("a.KpiEstDuration"),
        col("a.KpiCompleted"),
        col("a.KpiActDuration"),
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.KpiDataId = source.KpiDataId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "KpiDataId": col("source.KpiDataId"),
        "JobItemId": col("source.JobItemId"),
        "KpiTypeId": col("source.KpiTypeId"),
        "KpiEstDuration": col("source.KpiEstDuration"),
        "KpiCompleted": col("source.KpiCompleted"),
        "KpiActDuration": col("source.KpiActDuration"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "KpiDataId": col("source.KpiDataId"),
        "JobItemId": col("source.JobItemId"),
        "KpiTypeId": col("source.KpiTypeId"),
        "KpiEstDuration": col("source.KpiEstDuration"),
        "KpiCompleted": col("source.KpiCompleted"),
        "KpiActDuration": col("source.KpiActDuration"),
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

in_mode = "FULL"  # Replace with the actual IN_MODE va


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
# Define new schema with only the required columns
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_FaultId", StringType(), True),
    StructField("FaultId", StringType(), True),
    StructField("FaultDesc", StringType(), True)
])
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("FaultId", StringType(), True),
    StructField("FaultDesc", StringType(), True)
])

# Create an empty DataFrame
df = spark.createDataFrame([], schema)

# Define paths
silver_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_customer_complaint_fault_types"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
if in_mode == "FULL":

    df.write.format("delta").mode("overwrite").saveAsTable("tbl_customer_complaint_fault_types")

    # Read Bronze Delta Table
    bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_complaint_fault_types_FULL"

    bronze_df = spark.read.format("delta").load(bronze_path)

    

    # UPSERT: Merge latest records into Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.FaultId = source.FaultId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "FaultDesc": col("source.FaultDesc")  # Do not update FaultId
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "FaultId": col("source.FaultId"),
        "FaultDesc": col("source.FaultDesc")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("tbl_customer_complaint_fault_types")

    # Read Delta source
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_complaint_fault_types_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # DELETE: Remove records marked as 'D' in SYS_CHANGE_OPERATION
    filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
        .select("CT_FaultId").distinct()

    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.FaultId = S.CT_FaultId") \
        .whenMatchedDelete() \
        .execute()

    # UPSERT: Select only the latest versions of records
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("FaultId")
            .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
            .alias("b"),
        (col("a.FaultId") == col("b.FaultId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
        .select(
            col("a.SYS_CHANGE_VERSION"),
            col("a.SYS_CHANGE_OPERATION"),
            col("a.FaultId"),
            col("a.FaultDesc")
        ).distinct()

    # MERGE into Silver table
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.FaultId = source.FaultId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "FaultId": col("source.FaultId"),
        "FaultDesc": col("source.FaultDesc")  
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "FaultId": col("source.FaultId"),
        "FaultDesc": col("source.FaultDesc")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

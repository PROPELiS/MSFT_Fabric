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
    StructField("CT_JobVersionPGId", StringType(), True),
    StructField("JobVersionPGId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("POAId", StringType(), True)
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
    StructField("JobVersionPGId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("POAId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
    # Write the DataFrame as a Delta table
bronze_Path= "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_version_pg_FULL"
silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_version_pg"

# Load Silver table as a DeltaTable (not a DataFrame)
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
      # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_version_pg")

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobVersionPGId = source.JobVersionPGId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobVersionPGId": "source.JobVersionPGId",
        "JobVersionId": "source.JobVersionId",
        "POAId": "source.POAId"
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobVersionPGId": "source.JobVersionPGId",
        "JobVersionId": "source.JobVersionId",
        "POAId": "source.POAId"
        }).execute()
else:
    # df = spark.createDataFrame([], schema)
    df.write.format("delta").mode("append").saveAsTable("tbl_job_version_pg")
    
   # Define paths
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_version_pg_DELTA"
   # Read source and target as Delta Tables
    silver_df_delta = DeltaTable.forPath(spark, silver_path)
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobFolderId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobVersionPGId") \
    .distinct()


   # Perform the MERGE operation
    silver_df_delta.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobVersionPGId = S.CT_JobVersionPGId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobVersionPGId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobVersionPGId") == col("b.JobVersionPGId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.JobVersionPGId").alias("JobVersionPGId"),
        col("a.JobVersionId").alias("JobVersionId"),
        col("a.POAId").alias("POAId")
    ).distinct()
    
    # Perform the MERGE operation
    silver_df_delta.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobVersionPGId = source.JobVersionPGId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobVersionPGId": "source.JobVersionPGId",
        "JobVersionId": "source.JobVersionId",
        "POAId": "source.POAId"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "JobVersionPGId": "source.JobVersionPGId",
        "JobVersionId": "source.JobVersionId",
        "POAId": "source.POAId"
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
    StructField("CT_KPITypeId", StringType(), True),
    StructField("KPITypeId", StringType(), True),
    StructField("KPITypeName", StringType(), True),
    StructField("KPITypeNotInUse", StringType(), True),
    StructField("KPITypeGroupId", StringType(), True),
    StructField("KPITypeAmendCategoryId", StringType(), True)
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
   StructField("KPITypeId", StringType(), True),
   StructField("KPITypeName", StringType(), True),
   StructField("KPITypeNotInUse", StringType(), True),
   StructField("KPITypeGroupId", StringType(), True),
   StructField("KPITypeAmendCategoryId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_kpi_types"
silver_table = DeltaTable.forPath(
    spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").save(silver_path)
    # Load Delta tables correctly
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_kpi_types_FULL"

    bronze_df = spark.read.format("delta").load(bronze_Path)

        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.KPITypeId = source.KPITypeId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "KPITypeId": "source.KPITypeId",
        "KPITypeName": "source.KPITypeName",
        "KPITypeNotInUse": "source.KPITypeNotInUse",
        "KPITypeGroupId": "source.KPITypeGroupId",
        "KPITypeAmendCategoryId": "source.KPITypeAmendCategoryId"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "KPITypeId": "source.KPITypeId",
        "KPITypeName": "source.KPITypeName",
        "KPITypeNotInUse": "source.KPITypeNotInUse",
        "KPITypeGroupId": "source.KPITypeGroupId",
        "KPITypeAmendCategoryId": "source.KPITypeAmendCategoryId"
    }).execute()
else:
    # df = spark.createDataFrame([], schema)
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_kpi_types")
    
   # Define paths
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_kpi_types_DELTA"
   # Read source and target as Delta Tables
    silver_df_delta = DeltaTable.forPath(spark, silver_path)
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobFolderId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_KPITypeId") \
    .distinct()


   # Perform the MERGE operation
    silver_df_delta.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.KPITypeId = S.CT_KPITypeId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("KPITypeId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.KPITypeId") == col("b.KPITypeId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.KPITypeId").alias("KPITypeId"),
        col("a.KPITypeName").alias("KPITypeName"),
        col("a.KPITypeNotInUse").alias("KPITypeNotInUse"),
        col("a.KPITypeGroupId").alias("KPITypeGroupId"),
        col("a.KPITypeAmendCategoryId").alias("KPITypeAmendCategoryId")
    ).distinct()

    # Perform the MERGE operation
    silver_df_delta.alias("target").merge(
        transformed_df.alias("source"),
        "target.KPITypeId = source.KPITypeId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "KPITypeId": "source.KPITypeId",
        "KPITypeName": "source.KPITypeName",
        "KPITypeNotInUse": "source.KPITypeNotInUse",
        "KPITypeGroupId": "source.KPITypeGroupId",
        "KPITypeAmendCategoryId": "source.KPITypeAmendCategoryId"
    }).whenNotMatchedInsert(values={
       "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
       "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
       "KPITypeId": "source.KPITypeId",
       "KPITypeName": "source.KPITypeName",
       "KPITypeNotInUse": "source.KPITypeNotInUse",
       "KPITypeGroupId": "source.KPITypeGroupId",
       "KPITypeAmendCategoryId": "source.KPITypeAmendCategoryId"
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

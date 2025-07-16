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
# META           "id": "59693f16-ceb1-40c6-b096-d37b5fbbbd26"
# META         },
# META         {
# META           "id": "aabf914c-0501-4c58-ba5b-4b0f05f4420f"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

in_mode = "FULL" 

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

# Define the schema
orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_CurrencyId", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("CurrencyDesc", StringType(), True),
    StructField("CurrencySymbol", StringType(), True),
    StructField("ExchangeRate", DoubleType(), True),  # Assuming ExchangeRate is numeric
    StructField("CurrencyCode", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("CT_CurrencyId", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("CurrencyDesc", StringType(), True),
    StructField("CurrencySymbol", StringType(), True),
    StructField("ExchangeRate", DoubleType(), True),
    StructField("CurrencyCode", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

# Paths for Silver and Bronze tables
silver_table_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_currencies"

# Load the Silver data (Delta table)
silver_table = DeltaTable.forPath(spark, silver_table_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
     df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_currencies")
     bronze_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_currencies_FULL"


     # Load Bronze data
     bronze_df = spark.read.format("delta").load(bronze_path)

# Merge Full Data
     silver_table.alias("target").merge(
     bronze_df.alias("source"),
     "target.CurrencyId = source.CurrencyId"
     ).whenMatchedUpdate(set={
     "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
     "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
     "CurrencyId": col("source.CurrencyId"),
     "CurrencyDesc": col("source.CurrencyDesc"),
     "CurrencySymbol": col("source.CurrencySymbol"),
     "ExchangeRate": col("source.ExchangeRate"),
     "CurrencyCode": col("source.CurrencyCode")
     }).whenNotMatchedInsert(values={
     "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
     "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
     "CurrencyId": col("source.CurrencyId"),
     "CurrencyDesc": col("source.CurrencyDesc"),
     "CurrencySymbol": col("source.CurrencySymbol"),
     "ExchangeRate": col("source.ExchangeRate"),
     "CurrencyCode": col("source.CurrencyCode")
     }).execute()

else:          
     df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_currencies")
                                                   
     source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_currencies_DELTA"
     source_df_delta = spark.read.format("delta").load(source_path)

     # Step 1: DELETE Records from Target based on SYS_CHANGE_OPERATION = 'D'
     filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
        .select("CT_CurrencyId").distinct()

     silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.CurrencyId = S.CT_CurrencyId") \
        .whenMatchedDelete() \
        .execute()

     # Step 2: Prepare Latest Records for MERGE
     transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("CurrencyId")
            .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
            .alias("b"),
        (col("a.CurrencyId") == col("b.CurrencyId")) &
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
     ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
        .select(
            col("a.SYS_CHANGE_VERSION"),
            col("a.SYS_CHANGE_OPERATION"),
            col("a.CurrencyId"),
            col("a.CurrencyDesc"),
            col("a.CurrencySymbol"),
            col("a.ExchangeRate"),
            col("a.CurrencyCode")
        ).distinct()

     silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.CurrencyId = source.CurrencyId"
     ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyDesc": col("source.CurrencyDesc"),
        "CurrencySymbol": col("source.CurrencySymbol"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyCode": col("source.CurrencyCode")
     }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyDesc": col("source.CurrencyDesc"),
        "CurrencySymbol": col("source.CurrencySymbol"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyCode": col("source.CurrencyCode")
     }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

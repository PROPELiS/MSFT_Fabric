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

# Define updated schema without the specified columns
orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("CT_CreditNoteExId", StringType(), True),
    StructField("CreditNoteExId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("CreditNoteAuthorId", StringType(), True),
    StructField("CreditNoteCustomerId", StringType(), True),
    StructField("CreditNoteContactId", StringType(), True)
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
    StructField("CreditNoteExId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("CreditNoteAuthorId", StringType(), True),
    StructField("CreditNoteCustomerId", StringType(), True),
    StructField("CreditNoteContactId", StringType(), True)
])

# Write the DataFrame as a Delta table
df = spark.createDataFrame([], schema)

silver_table_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_credit_notes_ex"
silver_table = DeltaTable.forPath(spark, silver_table_path)

# Parameters
param = ""  # Replace with the actual PARAM value
#in_mode = "FULL"  # Replace with the actual IN_MODE value

if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_credit_notes_ex")
    bronze_Path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_notes_ex_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
  
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.CreditNoteExId = source.CreditNoteExId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteExId": col("source.CreditNoteExId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "CreditNoteAuthorId": col("source.CreditNoteAuthorId"),
        "CreditNoteCustomerId": col("source.CreditNoteCustomerId"),
        "CreditNoteContactId": col("source.CreditNoteContactId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteExId": col("source.CreditNoteExId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "CreditNoteAuthorId": col("source.CreditNoteAuthorId"),
        "CreditNoteCustomerId": col("source.CreditNoteCustomerId"),
        "CreditNoteContactId": col("source.CreditNoteContactId")
     }).execute()
else:
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_notes_ex_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
        .select("CT_CreditNoteExId").distinct()

    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.CreditNoteExId = S.CT_CreditNoteExId") \
        .whenMatchedDelete() \
        .execute()

    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("CreditNoteExId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.CreditNoteExId") == col("b.CreditNoteExId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("b.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.CreditNoteExId"),
        col("a.CreditNoteId"),
        col("a.CreditNoteAuthorId"),
        col("a.CreditNoteCustomerId"),
        col("a.CreditNoteContactId")
    ).distinct()

    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.CreditNoteExId = source.CreditNoteExId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteExId": col("source.CreditNoteExId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "CreditNoteAuthorId": col("source.CreditNoteAuthorId"),
        "CreditNoteCustomerId": col("source.CreditNoteCustomerId"),
        "CreditNoteContactId": col("source.CreditNoteContactId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteExId": col("source.CreditNoteExId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "CreditNoteAuthorId": col("source.CreditNoteAuthorId"),
        "CreditNoteCustomerId": col("source.CreditNoteCustomerId"),
        "CreditNoteContactId": col("source.CreditNoteContactId")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

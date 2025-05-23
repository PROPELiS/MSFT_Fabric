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
    StructField("CT_IgnoredInvoiceItemId", StringType(), True),
	StructField("IgnoredInvoiceItemId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
	StructField("JobItemId", StringType(), True),
	StructField("ReasonCode", StringType(), True),
	StructField("IgnoredDateTime", StringType(), True)
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
   StructField("IgnoredInvoiceItemId", StringType(), True),
   StructField("JobVersionId", StringType(), True),
   StructField("JobItemId", StringType(), True),
   StructField("ReasonCode", StringType(), True),
   StructField("IgnoredDateTime", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_ignored_invoice_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_ignored_invoice_items")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_ignored_invoice_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.IgnoredInvoiceItemId = source.IgnoredInvoiceItemId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "IgnoredInvoiceItemId": col("source.IgnoredInvoiceItemId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobItemId": col("source.JobItemId"),
        "ReasonCode": col("source.ReasonCode"),
		"IgnoredDateTime": col("source.IgnoredDateTime")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "IgnoredInvoiceItemId": col("source.IgnoredInvoiceItemId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobItemId": col("source.JobItemId"),
        "ReasonCode": col("source.ReasonCode"),
		"IgnoredDateTime": col("source.IgnoredDateTime")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_ignored_invoice_items")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_ignored_invoice_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_IgnoredInvoiceItemId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_IgnoredInvoiceItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.IgnoredInvoiceItemId = S.CT_IgnoredInvoiceItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("IgnoredInvoiceItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.IgnoredInvoiceItemId") == col("b.IgnoredInvoiceItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.IgnoredInvoiceItemId"),
        col("a.JobVersionId"),
        col("a.JobItemId"),
        col("a.ReasonCode"),
		col("a.IgnoredDateTime")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.IgnoredInvoiceItemId = source.IgnoredInvoiceItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "IgnoredInvoiceItemId": col("source.IgnoredInvoiceItemId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobItemId": col("source.JobItemId"),
        "ReasonCode": col("source.ReasonCode"),
		"IgnoredDateTime": col("source.IgnoredDateTime")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "IgnoredInvoiceItemId": col("source.IgnoredInvoiceItemId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobItemId": col("source.JobItemId"),
        "ReasonCode": col("source.ReasonCode"),
		"IgnoredDateTime": col("source.IgnoredDateTime")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

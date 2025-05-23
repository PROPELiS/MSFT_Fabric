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
    StructField("CT_InvoiceItemId", StringType(), True),
    StructField("InvoiceItemId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("InvoiceItemSalesAcct", DoubleType(), True),  # Assuming InvoiceItemSalesAcct is numeric
    StructField("SiteId", StringType(), True),
	StructField("ItemOrder", StringType(), True),
    StructField("InvoiceRollUpItemId", StringType(), True)
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
    StructField("InvoiceItemId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("InvoiceItemSalesAcct", DoubleType(), True),
    StructField("SiteId", StringType(), True),
	StructField("ItemOrder", StringType(), True),
    StructField("InvoiceRollUpItemId", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

# Paths for Silver and Bronze tables
silver_table_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_invoice_items"

# Load the Silver data (Delta table)
silver_table = DeltaTable.forPath(spark, silver_table_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
     df.write.format("delta").mode("overwrite").saveAsTable("tbl_invoice_items")
     bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoice_items_FULL"


     # Load Bronze data
     bronze_df = spark.read.format("delta").load(bronze_path)

# Merge Full Data
     silver_table.alias("target").merge(
     bronze_df.alias("source"),
     "target.InvoiceItemId = source.InvoiceItemId"
     ).whenMatchedUpdate(set={
     "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
     "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
     "InvoiceItemId": col("source.InvoiceItemId"),
     "InvoiceId": col("source.InvoiceId"),
     "JobItemId": col("source.JobItemId"),
     "InvoiceItemSalesAcct": col("source.InvoiceItemSalesAcct"),
     "SiteId": col("source.SiteId"),
	 "ItemOrder": col("source.ItemOrder"),
	 "InvoiceRollUpItemId": col("source.InvoiceRollUpItemId")
     }).whenNotMatchedInsert(values={
     "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
     "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
     "InvoiceItemId": col("source.InvoiceItemId"),
     "InvoiceId": col("source.InvoiceId"),
     "JobItemId": col("source.JobItemId"),
     "InvoiceItemSalesAcct": col("source.InvoiceItemSalesAcct"),
     "SiteId": col("source.SiteId"),
	 "ItemOrder": col("source.ItemOrder"),
	 "InvoiceRollUpItemId": col("source.InvoiceRollUpItemId")
     }).execute()

else:          
     df.write.format("delta").mode("append").saveAsTable("tbl_invoice_items")
                                                   
     source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoice_items_DELTA"
     source_df_delta = spark.read.format("delta").load(source_path)

     # Step 1: DELETE Records from Target based on SYS_CHANGE_OPERATION = 'D'
     filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
        .select("CT_InvoiceItemId").distinct()

     silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.InvoiceItemId = S.CT_InvoiceItemId") \
        .whenMatchedDelete() \
        .execute()

     # Step 2: Prepare Latest Records for MERGE
     transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("InvoiceItemId")
            .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
            .alias("b"),
        (col("a.InvoiceItemId") == col("b.InvoiceItemId")) &
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
     ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
        .select(
            col("a.SYS_CHANGE_VERSION"),
            col("a.SYS_CHANGE_OPERATION"),
            col("a.InvoiceItemId"),
            col("a.InvoiceId"),
            col("a.JobItemId"),
            col("a.InvoiceItemSalesAcct"),
            col("a.SiteId"),
			col("a.ItemOrder"),
			col("a.InvoiceRollUpItemId")
        ).distinct()

     silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.InvoiceItemId = source.InvoiceItemId"
     ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "InvoiceId": col("source.InvoiceId"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemSalesAcct": col("source.InvoiceItemSalesAcct"),
        "SiteId": col("source.SiteId"),
	    "ItemOrder": col("source.ItemOrder"),
	    "InvoiceRollUpItemId": col("source.InvoiceRollUpItemId")
     }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "InvoiceId": col("source.InvoiceId"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemSalesAcct": col("source.InvoiceItemSalesAcct"),
        "SiteId": col("source.SiteId"),
		"ItemOrder": col("source.ItemOrder"),
	    "InvoiceRollUpItemId": col("source.InvoiceRollUpItemId")
     }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(*) FROM SILVER.dbo.tbl_invoice_items")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

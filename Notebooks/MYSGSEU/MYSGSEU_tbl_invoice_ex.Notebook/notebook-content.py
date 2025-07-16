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

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_InvoiceExId", StringType(), True),
    StructField("InvoiceExId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("InvoiceAuthorId", StringType(), True),
    StructField("InvoiceCustomerId", StringType(), True),
    StructField("InvoiceContactId", StringType(), True),
    StructField("InvoicePONo", StringType(), True),
    StructField("ImportType", StringType(), True),
    StructField("DAXCreditCardId", StringType(), True),
    StructField("InvoiceAdditionalPONo", StringType(), True),
    StructField("OverrideInvoiceNo", StringType(), True)
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
   StructField("InvoiceExId", StringType(), True),
   StructField("InvoiceId", StringType(), True),
   StructField("InvoiceAuthorId", StringType(), True),
   StructField("InvoiceCustomerId", StringType(), True),
   StructField("InvoiceContactId", StringType(), True),
   StructField("InvoicePONo", StringType(), True),
   StructField("ImportType", StringType(), True),
   StructField("DAXCreditCardId", StringType(), True),
   StructField("InvoiceAdditionalPONo", StringType(), True),
   StructField("OverrideInvoiceNo", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_invoice_ex"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_invoice_ex")
    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoice_ex_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.InvoiceExId = source.InvoiceExId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "InvoiceExId": "source.InvoiceExId",
        "InvoiceId": "source.InvoiceId",
        "InvoiceAuthorId": "source.InvoiceAuthorId",
        "InvoiceCustomerId": "source.InvoiceCustomerId",
        "InvoiceContactId": "source.InvoiceContactId",
        "InvoicePONo": "source.InvoicePONo",
        "ImportType": "source.ImportType",
        "DAXCreditCardId": "source.DAXCreditCardId",
        "InvoiceAdditionalPONo": "source.InvoiceAdditionalPONo",
        "OverrideInvoiceNo": "source.OverrideInvoiceNo"
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "InvoiceExId": "source.InvoiceExId",
        "InvoiceId": "source.InvoiceId",
        "InvoiceAuthorId": "source.InvoiceAuthorId",
        "InvoiceCustomerId": "source.InvoiceCustomerId",
        "InvoiceContactId": "source.InvoiceContactId",
        "InvoicePONo": "source.InvoicePONo",
        "ImportType": "source.ImportType",
        "DAXCreditCardId": "source.DAXCreditCardId",
        "InvoiceAdditionalPONo": "source.InvoiceAdditionalPONo",
        "OverrideInvoiceNo": "source.OverrideInvoiceNo"
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_invoice_ex")
    
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoice_ex_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_InvoiceExId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.InvoiceExId = S.CT_InvoiceExId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("InvoiceExId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.InvoiceExId") == col("b.InvoiceExId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
       col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
       col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
       col("a.InvoiceExId").alias("InvoiceExId"),
       col("a.InvoiceId").alias("InvoiceId"),
       col("a.InvoiceAuthorId").alias("InvoiceAuthorId"),
       col("a.InvoiceCustomerId").alias("InvoiceCustomerId"),
       col("a.InvoiceContactId").alias("InvoiceContactId"),
       col("a.InvoicePONo").alias("InvoicePONo"),
       col("a.ImportType").alias("ImportType"),
       col("a.DAXCreditCardId").alias("DAXCreditCardId"),
       col("a.InvoiceAdditionalPONo").alias("InvoiceAdditionalPONo"),
       col("a.OverrideInvoiceNo").alias("OverrideInvoiceNo")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.InvoiceExId = source.InvoiceExId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceExId": col("source.InvoiceExId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceAuthorId": col("source.InvoiceAuthorId"),
        "InvoiceCustomerId": col("source.InvoiceCustomerId"),
        "InvoiceContactId": col("source.InvoiceContactId"),
        "InvoicePONo": col("source.InvoicePONo"),
        "ImportType": col("source.ImportType"),
        "DAXCreditCardId": col("source.DAXCreditCardId"),
        "InvoiceAdditionalPONo": col("source.InvoiceAdditionalPONo"),
        "OverrideInvoiceNo": col("source.OverrideInvoiceNo")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceExId": col("source.InvoiceExId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceAuthorId": col("source.InvoiceAuthorId"),
        "InvoiceCustomerId": col("source.InvoiceCustomerId"),
        "InvoiceContactId": col("source.InvoiceContactId"),
        "InvoicePONo": col("source.InvoicePONo"),
        "ImportType": col("source.ImportType"),
        "DAXCreditCardId": col("source.DAXCreditCardId"),
        "InvoiceAdditionalPONo": col("source.InvoiceAdditionalPONo"),
        "OverrideInvoiceNo": col("source.OverrideInvoiceNo")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
    StructField("CT_TransferId", StringType(), True),
    StructField("TransferId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("TransferFromSite", StringType(), True),
    StructField("TransferToSite", StringType(), True),
    StructField("TransferCustomer", StringType(), True),
    StructField("TransferRaisedBy", StringType(), True),
    StructField("TransferRaisedDate", StringType(), True),
    StructField("TransferReadyForExport", StringType(), True),
    StructField("TransferReadyForExportDate", StringType(), True),
    StructField("TransferExported", StringType(), True),
    StructField("TransferExportedDate", StringType(), True),
    StructField("SessionId", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
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
    StructField("TransferId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("TransferFromSite", StringType(), True),
    StructField("TransferToSite", StringType(), True),
    StructField("TransferCustomer", StringType(), True),
    StructField("TransferRaisedBy", StringType(), True),
    StructField("TransferRaisedDate", StringType(), True),
    StructField("TransferReadyForExport", StringType(), True),
    StructField("TransferReadyForExportDate", StringType(), True),
    StructField("TransferExported", StringType(), True),
    StructField("TransferExportedDate", StringType(), True),
    StructField("SessionId", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_inter_company_transfer"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_inter_company_transfer")
    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_inter_company_transfer_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.TransferId = source.TransferId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "TransferId": "source.TransferId",
        "JobVersionId": "source.JobVersionId",
        "TransferFromSite": "source.TransferFromSite",
        "TransferToSite": "source.TransferToSite",
        "TransferCustomer": "source.TransferCustomer",
        "TransferRaisedBy": "source.TransferRaisedBy",
        "TransferRaisedDate": "source.TransferRaisedDate",
        "TransferReadyForExport": "source.TransferReadyForExport",
        "TransferReadyForExportDate": "source.TransferReadyForExportDate",
        "TransferExported": "source.TransferExported",
        "TransferExportedDate": "source.TransferExportedDate",
        "SessionId": "source.SessionId",
        "CurrencyId": "source.CurrencyId",
        "InvoiceId": "source.InvoiceId",
        "CreditNoteId": "source.CreditNoteId"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "TransferId": "source.TransferId",
        "JobVersionId": "source.JobVersionId",
        "TransferFromSite": "source.TransferFromSite",
        "TransferToSite": "source.TransferToSite",
        "TransferCustomer": "source.TransferCustomer",
        "TransferRaisedBy": "source.TransferRaisedBy",
        "TransferRaisedDate": "source.TransferRaisedDate",
        "TransferReadyForExport": "source.TransferReadyForExport",
        "TransferReadyForExportDate": "source.TransferReadyForExportDate",
        "TransferExported": "source.TransferExported",
        "TransferExportedDate": "source.TransferExportedDate",
        "SessionId": "source.SessionId",
        "CurrencyId": "source.CurrencyId",
        "InvoiceId": "source.InvoiceId",
        "CreditNoteId": "source.CreditNoteId"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_inter_company_transfer")
    
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_inter_company_transfer_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_TransferId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.TransferId = S.CT_TransferId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("TransferId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.TransferId") == col("b.TransferId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.TransferId").alias("TransferId"),
        col("a.JobVersionId").alias("JobVersionId"),
        col("a.TransferFromSite").alias("TransferFromSite"),
        col("a.TransferToSite").alias("TransferToSite"),
        col("a.TransferCustomer").alias("TransferCustomer"),
        col("a.TransferRaisedBy").alias("TransferRaisedBy"),
        col("a.TransferRaisedDate").alias("TransferRaisedDate"),
        col("a.TransferReadyForExport").alias("TransferReadyForExport"),
        col("a.TransferReadyForExportDate").alias("TransferReadyForExportDate"),
        col("a.TransferExported").alias("TransferExported"),
        col("a.TransferExportedDate").alias("TransferExportedDate"),
        col("a.SessionId").alias("SessionId"),
        col("a.CurrencyId").alias("CurrencyId"),
        col("a.InvoiceId").alias("InvoiceId"),
        col("a.CreditNoteId").alias("CreditNoteId")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.TransferId = source.TransferId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TransferId": col("source.TransferId"),
        "JobVersionId": col("source.JobVersionId"),
        "TransferFromSite": col("source.TransferFromSite"),
        "TransferToSite": col("source.TransferToSite"),
        "TransferCustomer": col("source.TransferCustomer"),
        "TransferRaisedBy": col("source.TransferRaisedBy"),
        "TransferRaisedDate": col("source.TransferRaisedDate"),
        "TransferReadyForExport": col("source.TransferReadyForExport"),
        "TransferReadyForExportDate": col("source.TransferReadyForExportDate"),
        "TransferExported": col("source.TransferExported"),
        "TransferExportedDate": col("source.TransferExportedDate"),
        "SessionId": col("source.SessionId"),
        "CurrencyId": col("source.CurrencyId"),
        "InvoiceId": col("source.InvoiceId"),
        "CreditNoteId": col("source.CreditNoteId"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TransferId": col("source.TransferId"),
        "JobVersionId": col("source.JobVersionId"),
        "TransferFromSite": col("source.TransferFromSite"),
        "TransferToSite": col("source.TransferToSite"),
        "TransferCustomer": col("source.TransferCustomer"),
        "TransferRaisedBy": col("source.TransferRaisedBy"),
        "TransferRaisedDate": col("source.TransferRaisedDate"),
        "TransferReadyForExport": col("source.TransferReadyForExport"),
        "TransferReadyForExportDate": col("source.TransferReadyForExportDate"),
        "TransferExported": col("source.TransferExported"),
        "TransferExportedDate": col("source.TransferExportedDate"),
        "SessionId": col("source.SessionId"),
        "CurrencyId": col("source.CurrencyId"),
        "InvoiceId": col("source.InvoiceId"),
        "CreditNoteId": col("source.CreditNoteId"),
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

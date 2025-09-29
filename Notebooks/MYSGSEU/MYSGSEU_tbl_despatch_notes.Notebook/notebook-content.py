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
    StructField("CT_DespatchNoteId", StringType(), True),
    StructField("DespatchNoteId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("JobContactId", StringType(), True),
    StructField("ContactName", StringType(), True),
    StructField("DespatchAddress", StringType(), True),
    StructField("ExpectedDespatchDateTime", StringType(), True),
    StructField("DespatchMethod", StringType(), True),
    StructField("DespatchReference", StringType(), True),
    StructField("ActualDespatchDateTime", StringType(), True),
    StructField("Notes", StringType(), True),
    StructField("RangeId", StringType(), True),
    StructField("SessionId", StringType(), True),
    StructField("DespatchNotePONo", StringType(), True),
    StructField("RMSDespatchNumber", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("ReceiptRequired", StringType(), True),
    StructField("ReceivedBy", StringType(), True),
    StructField("ReceivedDate", StringType(), True),
    StructField("RejectReason", StringType(), True),
    StructField("ContactId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("UpdatedBy", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("SGSContact", StringType(), True),
    StructField("AlternativeCustomerName", StringType(), True),
    StructField("AddressVerified", StringType(), True),
    StructField("CountryId", StringType(), True)
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
    StructField("DespatchNoteId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("JobContactId", StringType(), True),
    StructField("ContactName", StringType(), True),
    StructField("DespatchAddress", StringType(), True),
    StructField("ExpectedDespatchDateTime", StringType(), True),
    StructField("DespatchMethod", StringType(), True),
    StructField("DespatchReference", StringType(), True),
    StructField("ActualDespatchDateTime", StringType(), True),
    StructField("Notes", StringType(), True),
    StructField("RangeId", StringType(), True),
    StructField("SessionId", StringType(), True),
    StructField("DespatchNotePONo", StringType(), True),
    StructField("RMSDespatchNumber", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("ReceiptRequired", StringType(), True),
    StructField("ReceivedBy", StringType(), True),
    StructField("ReceivedDate", StringType(), True),
    StructField("RejectReason", StringType(), True),
    StructField("ContactId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("UpdatedBy", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("SGSContact", StringType(), True),
    StructField("AlternativeCustomerName", StringType(), True),
    StructField("AddressVerified", StringType(), True),
    StructField("CountryId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_despatch_notes"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_despatch_notes")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_despatch_notes_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.DespatchNoteId = source.DespatchNoteId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "DespatchNoteId": "source.DespatchNoteId",
        "JobVersionId": "source.JobVersionId",
        "JobContactId": "source.JobContactId",
        "ContactName": "source.ContactName",
        "DespatchAddress": "source.DespatchAddress",
        "ExpectedDespatchDateTime": "source.ExpectedDespatchDateTime",
        "DespatchMethod": "source.DespatchMethod",
        "DespatchReference": "source.DespatchReference",
        "ActualDespatchDateTime": "source.ActualDespatchDateTime",
        "Notes": "source.Notes",
        "RangeId": "source.RangeId",
        "SessionId": "source.SessionId",
        "DespatchNotePONo": "source.DespatchNotePONo",
        "RMSDespatchNumber": "source.RMSDespatchNumber",
        "SiteId": "source.SiteId",
        "ReceiptRequired": "source.ReceiptRequired",
        "ReceivedBy": "source.ReceivedBy",
        "ReceivedDate": "source.ReceivedDate",
        "RejectReason": "source.RejectReason",
        "ContactId": "source.ContactId",
        "CustomerId": "source.CustomerId",
        "CreatedBy": "source.CreatedBy",
        "UpdatedBy": "source.UpdatedBy",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "SGSContact": "source.SGSContact",
        "AlternativeCustomerName": "source.AlternativeCustomerName",
        "AddressVerified": "source.AddressVerified",
        "CountryId": "source.CountryId"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "DespatchNoteId": "source.DespatchNoteId",
        "JobVersionId": "source.JobVersionId",
        "JobContactId": "source.JobContactId",
        "ContactName": "source.ContactName",
        "DespatchAddress": "source.DespatchAddress",
        "ExpectedDespatchDateTime": "source.ExpectedDespatchDateTime",
        "DespatchMethod": "source.DespatchMethod",
        "DespatchReference": "source.DespatchReference",
        "ActualDespatchDateTime": "source.ActualDespatchDateTime",
        "Notes": "source.Notes",
        "RangeId": "source.RangeId",
        "SessionId": "source.SessionId",
        "DespatchNotePONo": "source.DespatchNotePONo",
        "RMSDespatchNumber": "source.RMSDespatchNumber",
        "SiteId": "source.SiteId",
        "ReceiptRequired": "source.ReceiptRequired",
        "ReceivedBy": "source.ReceivedBy",
        "ReceivedDate": "source.ReceivedDate",
        "RejectReason": "source.RejectReason",
        "ContactId": "source.ContactId",
        "CustomerId": "source.CustomerId",
        "CreatedBy": "source.CreatedBy",
        "UpdatedBy": "source.UpdatedBy",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "SGSContact": "source.SGSContact",
        "AlternativeCustomerName": "source.AlternativeCustomerName",
        "AddressVerified": "source.AddressVerified",
        "CountryId": "source.CountryId"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_despatch_notes")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_despatch_notes_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_DespatchNoteId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.DespatchNoteId = S.CT_DespatchNoteId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("DespatchNoteId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.DespatchNoteId") == col("b.DespatchNoteId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.DespatchNoteId").alias("DespatchNoteId"),
        col("a.JobVersionId").alias("JobVersionId"),
        col("a.JobContactId").alias("JobContactId"),
        col("a.ContactName").alias("ContactName"),
        col("a.DespatchAddress").alias("DespatchAddress"),
        col("a.ExpectedDespatchDateTime").alias("ExpectedDespatchDateTime"),
        col("a.DespatchMethod").alias("DespatchMethod"),
        col("a.DespatchReference").alias("DespatchReference"),
        col("a.ActualDespatchDateTime").alias("ActualDespatchDateTime"),
        col("a.Notes").alias("Notes"),
        col("a.RangeId").alias("RangeId"),
        col("a.SessionId").alias("SessionId"),
        col("a.DespatchNotePONo").alias("DespatchNotePONo"),
        col("a.RMSDespatchNumber").alias("RMSDespatchNumber"),
        col("a.SiteId").alias("SiteId"),
        col("a.ReceiptRequired").alias("ReceiptRequired"),
        col("a.ReceivedBy").alias("ReceivedBy"),
        col("a.ReceivedDate").alias("ReceivedDate"),
        col("a.RejectReason").alias("RejectReason"),
        col("a.ContactId").alias("ContactId"),
        col("a.CustomerId").alias("CustomerId"),
        col("a.CreatedBy").alias("CreatedBy"),
        col("a.UpdatedBy").alias("UpdatedBy"),
        col("a.CreatedDate").alias("CreatedDate"),
        col("a.UpdatedDate").alias("UpdatedDate"),
        col("a.SGSContact").alias("SGSContact"),
        col("a.AlternativeCustomerName").alias("AlternativeCustomerName"),
        col("a.AddressVerified").alias("AddressVerified"),
        col("a.CountryId").alias("CountryId")

    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.DespatchNoteId = source.DespatchNoteId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DespatchNoteId": col("source.DespatchNoteId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobContactId": col("source.JobContactId"),
        "ContactName": col("source.ContactName"),
        "DespatchAddress": col("source.DespatchAddress"),
        "ExpectedDespatchDateTime": col("source.ExpectedDespatchDateTime"),
        "DespatchMethod": col("source.DespatchMethod"),
        "DespatchReference": col("source.DespatchReference"),
        "ActualDespatchDateTime": col("source.ActualDespatchDateTime"),
        "Notes": col("source.Notes"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "DespatchNotePONo": col("source.DespatchNotePONo"),
        "RMSDespatchNumber": col("source.RMSDespatchNumber"),
        "SiteId": col("source.SiteId"),
        "ReceiptRequired": col("source.ReceiptRequired"),
        "ReceivedBy": col("source.ReceivedBy"),
        "ReceivedDate": col("source.ReceivedDate"),
        "RejectReason": col("source.RejectReason"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "SGSContact": col("source.SGSContact"),
        "AlternativeCustomerName": col("source.AlternativeCustomerName"),
        "AddressVerified": col("source.AddressVerified"),
        "CountryId": col("source.CountryId"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DespatchNoteId": col("source.DespatchNoteId"),
        "JobVersionId": col("source.JobVersionId"),
        "JobContactId": col("source.JobContactId"),
        "ContactName": col("source.ContactName"),
        "DespatchAddress": col("source.DespatchAddress"),
        "ExpectedDespatchDateTime": col("source.ExpectedDespatchDateTime"),
        "DespatchMethod": col("source.DespatchMethod"),
        "DespatchReference": col("source.DespatchReference"),
        "ActualDespatchDateTime": col("source.ActualDespatchDateTime"),
        "Notes": col("source.Notes"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "DespatchNotePONo": col("source.DespatchNotePONo"),
        "RMSDespatchNumber": col("source.RMSDespatchNumber"),
        "SiteId": col("source.SiteId"),
        "ReceiptRequired": col("source.ReceiptRequired"),
        "ReceivedBy": col("source.ReceivedBy"),
        "ReceivedDate": col("source.ReceivedDate"),
        "RejectReason": col("source.RejectReason"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "SGSContact": col("source.SGSContact"),
        "AlternativeCustomerName": col("source.AlternativeCustomerName"),
        "AddressVerified": col("source.AddressVerified"),
        "CountryId": col("source.CountryId"),
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

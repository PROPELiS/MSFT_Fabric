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

# CELL ********************

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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType# Define the schema with the updated columns
# Updated schema with additional columns
orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_CreditNoteId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("CreditNoteDateTime", StringType(), True),
    StructField("CreditNoteStatus", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("JobContactId", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("ExchangeRateId", StringType(), True),
    StructField("ExchangeRateValue", StringType(), True),
    StructField("TaxCodeId", StringType(), True),
    StructField("TaxCodeRate", StringType(), True),
    StructField("CreditReason", StringType(), True),
    StructField("CreditNoteNotes", StringType(), True),
    StructField("ApprovedBy", StringType(), True),
    StructField("ApprovedDateTime", StringType(), True),
    StructField("CreditNoteReadyForExport", StringType(), True),
    StructField("CreditNoteExported", StringType(), True),
    StructField("CreditNoteExportedDateTime", StringType(), True),
    StructField("CreditNoteTitle", StringType(), True),
    StructField("ClientPON", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("PostingAddress", StringType(), True),
    StructField("ExportStatus", StringType(), True),
    StructField("EntityId", StringType(), True),
    StructField("TaxExemptionOverride", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Updated schema with additional columns
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("CreditNoteDateTime", StringType(), True),
    StructField("CreditNoteStatus", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("JobContactId", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("ExchangeRateId", StringType(), True),
    StructField("ExchangeRateValue", StringType(), True),
    StructField("TaxCodeId", StringType(), True),
    StructField("TaxCodeRate", StringType(), True),
    StructField("CreditReason", StringType(), True),
    StructField("CreditNoteNotes", StringType(), True),
    StructField("ApprovedBy", StringType(), True),
    StructField("ApprovedDateTime", StringType(), True),
    StructField("CreditNoteReadyForExport", StringType(), True),
    StructField("CreditNoteExported", StringType(), True),
    StructField("CreditNoteExportedDateTime", StringType(), True),
    StructField("CreditNoteTitle", StringType(), True),
    StructField("ClientPON", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("PostingAddress", StringType(), True),
    StructField("ExportStatus", StringType(), True),
    StructField("EntityId", StringType(), True),
    StructField("TaxExemptionOverride", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_credit_notes"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_credit_notes")
    # Load Delta tables correctly
    bronze_Path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_notes_FULL"
    bronze_df = spark.read.format("delta").load(bronze_Path)

    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.CreditNoteId = source.CreditNoteId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "CreditNoteId": "source.CreditNoteId",
    "CreditNoteDateTime": "source.CreditNoteDateTime",
    "CreditNoteStatus": "source.CreditNoteStatus",
    "JobVersionId": "source.JobVersionId",
    "InvoiceId": "source.InvoiceId",
    "JobContactId": "source.JobContactId",
    "Address": "source.Address",
    "ExchangeRateId": "source.ExchangeRateId",
    "ExchangeRateValue": "source.ExchangeRateValue",
    "TaxCodeId": "source.TaxCodeId",
    "TaxCodeRate": "source.TaxCodeRate",
    "CreditReason": "source.CreditReason",
    "CreditNoteNotes": "source.CreditNoteNotes",
    "ApprovedBy": "source.ApprovedBy",
    "ApprovedDateTime": "source.ApprovedDateTime",
    "CreditNoteReadyForExport": "source.CreditNoteReadyForExport",
    "CreditNoteExported": "source.CreditNoteExported",
    "CreditNoteExportedDateTime": "source.CreditNoteExportedDateTime",
    "CreditNoteTitle": "source.CreditNoteTitle",
    "ClientPON": "source.ClientPON",
    "CurrencyId": "source.CurrencyId",
    "PostingAddress": "source.PostingAddress",
    "ExportStatus": "source.ExportStatus",
    "EntityId": "source.EntityId",
    "TaxExemptionOverride": "source.TaxExemptionOverride"
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "CreditNoteId": "source.CreditNoteId",
    "CreditNoteDateTime": "source.CreditNoteDateTime",
    "CreditNoteStatus": "source.CreditNoteStatus",
    "JobVersionId": "source.JobVersionId",
    "InvoiceId": "source.InvoiceId",
    "JobContactId": "source.JobContactId",
    "Address": "source.Address",
    "ExchangeRateId": "source.ExchangeRateId",
    "ExchangeRateValue": "source.ExchangeRateValue",
    "TaxCodeId": "source.TaxCodeId",
    "TaxCodeRate": "source.TaxCodeRate",
    "CreditReason": "source.CreditReason",
    "CreditNoteNotes": "source.CreditNoteNotes",
    "ApprovedBy": "source.ApprovedBy",
    "ApprovedDateTime": "source.ApprovedDateTime",
    "CreditNoteReadyForExport": "source.CreditNoteReadyForExport",
    "CreditNoteExported": "source.CreditNoteExported",
    "CreditNoteExportedDateTime": "source.CreditNoteExportedDateTime",
    "CreditNoteTitle": "source.CreditNoteTitle",
    "ClientPON": "source.ClientPON",
    "CurrencyId": "source.CurrencyId",
    "PostingAddress": "source.PostingAddress",
    "ExportStatus": "source.ExportStatus",
    "EntityId": "source.EntityId",
    "TaxExemptionOverride": "source.TaxExemptionOverride"
}).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_credit_notes")

    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_notes_DELTA"
    
    source_df_delta = spark.read.format("delta").load(source_path)

    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
        .select("CT_CreditNoteId").distinct()

    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.CreditNoteId = S.CT_CreditNoteId") \
        .whenMatchedDelete() \
        .execute()

    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("CreditNoteId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.CreditNoteId") == col("b.CreditNoteId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
    col("a.SYS_CHANGE_VERSION"),
    col("a.SYS_CHANGE_OPERATION"),
    col("a.CreditNoteId"),
    col("a.CreditNoteDateTime"),
    col("a.CreditNoteStatus"),
    col("a.JobVersionId"),
    col("a.InvoiceId"),
    col("a.JobContactId"),
    col("a.Address"),
    col("a.ExchangeRateId"),
    col("a.ExchangeRateValue"),
    col("a.TaxCodeId"),
    col("a.TaxCodeRate"),
    col("a.CreditReason"),
    col("a.CreditNoteNotes"),
    col("a.ApprovedBy"),
    col("a.ApprovedDateTime"),
    col("a.CreditNoteReadyForExport"),
    col("a.CreditNoteExported"),
    col("a.CreditNoteExportedDateTime"),
    col("a.CreditNoteTitle"),
    col("a.ClientPON"),
    col("a.CurrencyId"),
    col("a.PostingAddress"),
    col("a.ExportStatus"),
    col("a.EntityId"),
    col("a.TaxExemptionOverride")
).distinct()


    # Perform the MERGE operation
    silver_table.alias("target").merge(
    transformed_df.alias("source"),
    "target.CreditNoteId = source.CreditNoteId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "CreditNoteId": "source.CreditNoteId",
    "CreditNoteDateTime": "source.CreditNoteDateTime",
    "CreditNoteStatus": "source.CreditNoteStatus",
    "JobVersionId": "source.JobVersionId",
    "InvoiceId": "source.InvoiceId",
    "JobContactId": "source.JobContactId",
    "Address": "source.Address",
    "ExchangeRateId": "source.ExchangeRateId",
    "ExchangeRateValue": "source.ExchangeRateValue",
    "TaxCodeId": "source.TaxCodeId",
    "TaxCodeRate": "source.TaxCodeRate",
    "CreditReason": "source.CreditReason",
    "CreditNoteNotes": "source.CreditNoteNotes",
    "ApprovedBy": "source.ApprovedBy",
    "ApprovedDateTime": "source.ApprovedDateTime",
    "CreditNoteReadyForExport": "source.CreditNoteReadyForExport",
    "CreditNoteExported": "source.CreditNoteExported",
    "CreditNoteExportedDateTime": "source.CreditNoteExportedDateTime",
    "CreditNoteTitle": "source.CreditNoteTitle",
    "ClientPON": "source.ClientPON",
    "CurrencyId": "source.CurrencyId",
    "PostingAddress": "source.PostingAddress",
    "ExportStatus": "source.ExportStatus",
    "EntityId": "source.EntityId",
    "TaxExemptionOverride": "source.TaxExemptionOverride"
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "CreditNoteId": "source.CreditNoteId",
    "CreditNoteDateTime": "source.CreditNoteDateTime",
    "CreditNoteStatus": "source.CreditNoteStatus",
    "JobVersionId": "source.JobVersionId",
    "InvoiceId": "source.InvoiceId",
    "JobContactId": "source.JobContactId",
    "Address": "source.Address",
    "ExchangeRateId": "source.ExchangeRateId",
    "ExchangeRateValue": "source.ExchangeRateValue",
    "TaxCodeId": "source.TaxCodeId",
    "TaxCodeRate": "source.TaxCodeRate",
    "CreditReason": "source.CreditReason",
    "CreditNoteNotes": "source.CreditNoteNotes",
    "ApprovedBy": "source.ApprovedBy",
    "ApprovedDateTime": "source.ApprovedDateTime",
    "CreditNoteReadyForExport": "source.CreditNoteReadyForExport",
    "CreditNoteExported": "source.CreditNoteExported",
    "CreditNoteExportedDateTime": "source.CreditNoteExportedDateTime",
    "CreditNoteTitle": "source.CreditNoteTitle",
    "ClientPON": "source.ClientPON",
    "CurrencyId": "source.CurrencyId",
    "PostingAddress": "source.PostingAddress",
    "ExportStatus": "source.ExportStatus",
    "EntityId": "source.EntityId",
    "TaxExemptionOverride": "source.TaxExemptionOverride"
}).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM SILVER.dbo.tbl_credit_notes")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_credit_notes_DELTA ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_credit_notes_FULL")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BRONZE.MYSGSEU.tbl_credit_notes_DELTA ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

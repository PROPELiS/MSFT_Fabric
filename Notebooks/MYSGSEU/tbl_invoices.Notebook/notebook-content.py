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
# META         },
# META         {
# META           "id": "5db3d583-e11f-4ac4-9781-65ee3ee820a0"
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

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_InvoiceId", StringType(), True),
	StructField("InvoiceId", StringType(), True),
    StructField("InvoiceDateTime", StringType(), True),
	StructField("JobVersionId", StringType(), True),
	StructField("InvoiceTitle", StringType(), True),
	StructField("JobContactId", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("ExchangeRateId", StringType(), True),
    StructField("ExchangeRateValue", StringType(), True),
    StructField("TaxCodeId", StringType(), True),
    StructField("TaxCodeRate", StringType(), True),
    StructField("Notes", StringType(), True),
    StructField("InvoiceReadyForExport", StringType(), True),
    StructField("InvoiceExported", StringType(), True),
    StructField("InvoiceExportDateTime", StringType(), True),
	StructField("RangeId", StringType(), True),
	StructField("SessionId", StringType(), True),
	StructField("PostingAddress", StringType(), True),
	StructField("CurrencyId", StringType(), True),
	StructField("ProForma", StringType(), True),
	StructField("Reissued", StringType(), True),
	StructField("OriginalInvoiceId", StringType(), True),
    StructField("InvoiceTypeId", StringType(), True),
	StructField("PurchaseOrderDate", StringType(), True),
	StructField("AutomatedCreation", StringType(), True),
	StructField("Paid", StringType(), True),
    StructField("InvoiceScoroId", StringType(), True),
    StructField("ExportStatus", StringType(), True),
    StructField("EntityId", StringType(), True),
    StructField("ProformaType", StringType(), True),
    StructField("ProformaStatus", StringType(), True),
    StructField("ProformaStatusChangedDateTime", StringType(), True),
    StructField("VoucherNumber", StringType(), True),
    StructField("ProformaStatusSentDateTime", StringType(), True),
    StructField("VoucherAmount", StringType(), True)
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
   StructField("InvoiceId", StringType(), True),
   StructField("InvoiceDateTime", StringType(), True),
   StructField("JobVersionId", StringType(), True),
   StructField("InvoiceTitle", StringType(), True),
   StructField("JobContactId", StringType(), True),
   StructField("Address", StringType(), True),
   StructField("ExchangeRateId", StringType(), True),
   StructField("ExchangeRateValue", StringType(), True),
   StructField("TaxCodeId", StringType(), True),
   StructField("TaxCodeRate", StringType(), True),
   StructField("Notes", StringType(), True),
   StructField("InvoiceReadyForExport", StringType(), True),
   StructField("InvoiceExported", StringType(), True),
   StructField("InvoiceExportDateTime", StringType(), True),
   StructField("RangeId", StringType(), True),
   StructField("SessionId", StringType(), True),
   StructField("PostingAddress", StringType(), True),
   StructField("CurrencyId", StringType(), True),
   StructField("ProForma", StringType(), True),
   StructField("Reissued", StringType(), True),
   StructField("OriginalInvoiceId", StringType(), True),
   StructField("InvoiceTypeId", StringType(), True),
   StructField("PurchaseOrderDate", StringType(), True),
   StructField("AutomatedCreation", StringType(), True),
   StructField("Paid", StringType(), True),
   StructField("InvoiceScoroId", StringType(), True),
   StructField("ExportStatus", StringType(), True),
   StructField("EntityId", StringType(), True),
   StructField("ProformaType", StringType(), True),
   StructField("ProformaStatus", StringType(), True),
   StructField("ProformaStatusChangedDateTime", StringType(), True),
   StructField("VoucherNumber", StringType(), True),
   StructField("ProformaStatusSentDateTime", StringType(), True),
   StructField("VoucherAmount", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_invoices"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_invoices")

    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoices_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.InvoiceId = source.InvoiceId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDateTime": col("source.InvoiceDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "InvoiceTitle": col("source.InvoiceTitle"),
		"JobContactId": col("source.JobContactId"),
        "Address": col("source.Address"),
        "ExchangeRateId": col("source.ExchangeRateId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "TaxCodeId": col("source.TaxCodeId"),
        "TaxCodeRate": col("source.TaxCodeRate"),
        "Notes": col("source.Notes"),
        "InvoiceReadyForExport": col("source.InvoiceReadyForExport"),
        "InvoiceExported": col("source.InvoiceExported"),
        "InvoiceExportDateTime": col("source.InvoiceExportDateTime"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "PostingAddress": col("source.PostingAddress"),
        "CurrencyId": col("source.CurrencyId"),
        "ProForma": col("source.ProForma"),
        "Reissued": col("source.Reissued"),
        "OriginalInvoiceId": col("source.OriginalInvoiceId"),
        "InvoiceTypeId": col("source.InvoiceTypeId"),
        "PurchaseOrderDate": col("source.PurchaseOrderDate"),
        "AutomatedCreation": col("source.AutomatedCreation"),
		"Paid": col("source.Paid"),
        "InvoiceScoroId": col("source.InvoiceScoroId"),
        "ExportStatus": col("source.ExportStatus"),
        "EntityId": col("source.EntityId"),
        "ProformaType": col("source.ProformaType"),
        "ProformaStatus": col("source.ProformaStatus"),
        "ProformaStatusChangedDateTime": col("source.ProformaStatusChangedDateTime"),
        "VoucherNumber": col("source.VoucherNumber"),
        "ProformaStatusSentDateTime": col("source.ProformaStatusSentDateTime"),
        "VoucherAmount": col("source.VoucherAmount")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDateTime": col("source.InvoiceDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "InvoiceTitle": col("source.InvoiceTitle"),
		"JobContactId": col("source.JobContactId"),
        "Address": col("source.Address"),
        "ExchangeRateId": col("source.ExchangeRateId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "TaxCodeId": col("source.TaxCodeId"),
        "TaxCodeRate": col("source.TaxCodeRate"),
        "Notes": col("source.Notes"),
        "InvoiceReadyForExport": col("source.InvoiceReadyForExport"),
        "InvoiceExported": col("source.InvoiceExported"),
        "InvoiceExportDateTime": col("source.InvoiceExportDateTime"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "PostingAddress": col("source.PostingAddress"),
        "CurrencyId": col("source.CurrencyId"),
        "ProForma": col("source.ProForma"),
        "Reissued": col("source.Reissued"),
        "OriginalInvoiceId": col("source.OriginalInvoiceId"),
        "InvoiceTypeId": col("source.InvoiceTypeId"),
        "PurchaseOrderDate": col("source.PurchaseOrderDate"),
        "AutomatedCreation": col("source.AutomatedCreation"),
		"Paid": col("source.Paid"),
        "InvoiceScoroId": col("source.InvoiceScoroId"),
        "ExportStatus": col("source.ExportStatus"),
        "EntityId": col("source.EntityId"),
        "ProformaType": col("source.ProformaType"),
        "ProformaStatus": col("source.ProformaStatus"),
        "ProformaStatusChangedDateTime": col("source.ProformaStatusChangedDateTime"),
        "VoucherNumber": col("source.VoucherNumber"),
        "ProformaStatusSentDateTime": col("source.ProformaStatusSentDateTime"),
        "VoucherAmount": col("source.VoucherAmount")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_invoices")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_invoices_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_InvoiceId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_InvoiceId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.InvoiceId = S.CT_InvoiceId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("InvoiceId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.InvoiceId") == col("b.InvoiceId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.InvoiceId"),
        col("a.InvoiceDateTime"),
        col("a.JobVersionId"),
        col("a.InvoiceTitle"),
		col("a.JobContactId"),
        col("a.Address"),
        col("a.ExchangeRateId"),
        col("a.ExchangeRateValue"),
        col("a.TaxCodeId"),
        col("a.TaxCodeRate"),
        col("a.Notes"),
        col("a.InvoiceReadyForExport"),
        col("a.InvoiceExported"),
        col("a.InvoiceExportDateTime"),
        col("a.RangeId"),
        col("a.SessionId"),
        col("a.PostingAddress"),
        col("a.CurrencyId"),
        col("a.ProForma"),
        col("a.Reissued"),
        col("a.OriginalInvoiceId"),
        col("a.InvoiceTypeId"),
        col("a.PurchaseOrderDate"),
        col("a.AutomatedCreation"),
		col("a.Paid"),
        col("a.InvoiceScoroId"),
        col("a.ExportStatus"),
        col("a.EntityId"),
        col("a.ProformaType"),
        col("a.ProformaStatus"),
        col("a.ProformaStatusChangedDateTime"),
        col("a.VoucherNumber"),
        col("a.ProformaStatusSentDateTime"),
        col("a.VoucherAmount")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.InvoiceId = source.InvoiceId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDateTime": col("source.InvoiceDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "InvoiceTitle": col("source.InvoiceTitle"),
		"JobContactId": col("source.JobContactId"),
        "Address": col("source.Address"),
        "ExchangeRateId": col("source.ExchangeRateId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "TaxCodeId": col("source.TaxCodeId"),
        "TaxCodeRate": col("source.TaxCodeRate"),
        "Notes": col("source.Notes"),
        "InvoiceReadyForExport": col("source.InvoiceReadyForExport"),
        "InvoiceExported": col("source.InvoiceExported"),
        "InvoiceExportDateTime": col("source.InvoiceExportDateTime"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "PostingAddress": col("source.PostingAddress"),
        "CurrencyId": col("source.CurrencyId"),
        "ProForma": col("source.ProForma"),
        "Reissued": col("source.Reissued"),
        "OriginalInvoiceId": col("source.OriginalInvoiceId"),
        "InvoiceTypeId": col("source.InvoiceTypeId"),
        "PurchaseOrderDate": col("source.PurchaseOrderDate"),
        "AutomatedCreation": col("source.AutomatedCreation"),
		"Paid": col("source.Paid"),
        "InvoiceScoroId": col("source.InvoiceScoroId"),
        "ExportStatus": col("source.ExportStatus"),
        "EntityId": col("source.EntityId"),
        "ProformaType": col("source.ProformaType"),
        "ProformaStatus": col("source.ProformaStatus"),
        "ProformaStatusChangedDateTime": col("source.ProformaStatusChangedDateTime"),
        "VoucherNumber": col("source.VoucherNumber"),
        "ProformaStatusSentDateTime": col("source.ProformaStatusSentDateTime"),
        "VoucherAmount": col("source.VoucherAmount")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDateTime": col("source.InvoiceDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "InvoiceTitle": col("source.InvoiceTitle"),
		"JobContactId": col("source.JobContactId"),
        "Address": col("source.Address"),
        "ExchangeRateId": col("source.ExchangeRateId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "TaxCodeId": col("source.TaxCodeId"),
        "TaxCodeRate": col("source.TaxCodeRate"),
        "Notes": col("source.Notes"),
        "InvoiceReadyForExport": col("source.InvoiceReadyForExport"),
        "InvoiceExported": col("source.InvoiceExported"),
        "InvoiceExportDateTime": col("source.InvoiceExportDateTime"),
        "RangeId": col("source.RangeId"),
        "SessionId": col("source.SessionId"),
        "PostingAddress": col("source.PostingAddress"),
        "CurrencyId": col("source.CurrencyId"),
        "ProForma": col("source.ProForma"),
        "Reissued": col("source.Reissued"),
        "OriginalInvoiceId": col("source.OriginalInvoiceId"),
        "InvoiceTypeId": col("source.InvoiceTypeId"),
        "PurchaseOrderDate": col("source.PurchaseOrderDate"),
        "AutomatedCreation": col("source.AutomatedCreation"),
		"Paid": col("source.Paid"),
        "InvoiceScoroId": col("source.InvoiceScoroId"),
        "ExportStatus": col("source.ExportStatus"),
        "EntityId": col("source.EntityId"),
        "ProformaType": col("source.ProformaType"),
        "ProformaStatus": col("source.ProformaStatus"),
        "ProformaStatusChangedDateTime": col("source.ProformaStatusChangedDateTime"),
        "VoucherNumber": col("source.VoucherNumber"),
        "ProformaStatusSentDateTime": col("source.ProformaStatusSentDateTime"),
        "VoucherAmount": col("source.VoucherAmount")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

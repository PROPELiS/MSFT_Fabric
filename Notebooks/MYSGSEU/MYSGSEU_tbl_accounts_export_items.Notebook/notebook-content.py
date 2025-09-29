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
    StructField("CT_ExportItemId", StringType(), True),
    StructField("ExportItemId", StringType(), True),
    StructField("ExportId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("CustomerAccountNo", StringType(), True),
    StructField("TaxCode", StringType(), True),
    StructField("SalesAcct", StringType(), True),
    StructField("SalesAmt", StringType(), True),
    StructField("FunctionalSalesAmt", StringType(), True),
    StructField("TaxRate", StringType(), True),
    StructField("TaxAmt", StringType(), True),
    StructField("FunctionalTaxAmt", StringType(), True),
    StructField("PONo", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("JobDescription", StringType(), True),
    StructField("JobId", StringType(), True),
    StructField("JobVersion", StringType(), True),
    StructField("AccountManagerName", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("ItemDescription", StringType(), True),
    StructField("Discount", StringType(), True),
    StructField("SiteCode", StringType(), True),
    StructField("CurrencyValue", StringType(), True),
    StructField("ExchangeRate", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("SiteSubCode", StringType(), True),
    StructField("FunctionalCurrencyId", StringType(), True),
    StructField("FunctionalExchangeRate", StringType(), True),
    StructField("InvoiceItemId", StringType(), True),
    StructField("CreditNoteItemId", StringType(), True),
    StructField("TransferItemId", StringType(), True)
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
    StructField("ExportItemId", StringType(), True),
    StructField("ExportId", StringType(), True),
    StructField("InvoiceId", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("CustomerAccountNo", StringType(), True),
    StructField("TaxCode", StringType(), True),
    StructField("SalesAcct", StringType(), True),
    StructField("SalesAmt", StringType(), True),
    StructField("FunctionalSalesAmt", StringType(), True),
    StructField("TaxRate", StringType(), True),
    StructField("TaxAmt", StringType(), True),
    StructField("FunctionalTaxAmt", StringType(), True),
    StructField("PONo", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("JobDescription", StringType(), True),
    StructField("JobId", StringType(), True),
    StructField("JobVersion", StringType(), True),
    StructField("AccountManagerName", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("ItemDescription", StringType(), True),
    StructField("Discount", StringType(), True),
    StructField("SiteCode", StringType(), True),
    StructField("CurrencyValue", StringType(), True),
    StructField("ExchangeRate", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("SiteSubCode", StringType(), True),
    StructField("FunctionalCurrencyId", StringType(), True),
    StructField("FunctionalExchangeRate", StringType(), True),
    StructField("InvoiceItemId", StringType(), True),
    StructField("CreditNoteItemId", StringType(), True),
    StructField("TransferItemId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_accounts_export_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_accounts_export_items")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_accounts_export_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.ExportItemId = source.ExportItemId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExportItemId": col("source.ExportItemId"),
        "ExportId": col("source.ExportId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDate": col("source.InvoiceDate"),
        "CustomerAccountNo": col("source.CustomerAccountNo"),
        "TaxCode": col("source.TaxCode"),
        "SalesAcct": col("source.SalesAcct"),
        "SalesAmt": col("source.SalesAmt"),
        "FunctionalSalesAmt": col("source.FunctionalSalesAmt"),
        "TaxRate": col("source.TaxRate"),
        "TaxAmt": col("source.TaxAmt"),
        "FunctionalTaxAmt": col("source.FunctionalTaxAmt"),
        "PONo": col("source.PONo"),
        "Type": col("source.Type"),
        "JobDescription": col("source.JobDescription"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "AccountManagerName": col("source.AccountManagerName"),
        "CustomerName": col("source.CustomerName"),
        "ItemDescription": col("source.ItemDescription"),
        "Discount": col("source.Discount"),
        "SiteCode": col("source.SiteCode"),
        "CurrencyValue": col("source.CurrencyValue"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyCode": col("source.CurrencyCode"),
        "CountryCode": col("source.CountryCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "FunctionalExchangeRate": col("source.FunctionalExchangeRate"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "TransferItemId": col("source.TransferItemId"),
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExportItemId": col("source.ExportItemId"),
        "ExportId": col("source.ExportId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDate": col("source.InvoiceDate"),
        "CustomerAccountNo": col("source.CustomerAccountNo"),
        "TaxCode": col("source.TaxCode"),
        "SalesAcct": col("source.SalesAcct"),
        "SalesAmt": col("source.SalesAmt"),
        "FunctionalSalesAmt": col("source.FunctionalSalesAmt"),
        "TaxRate": col("source.TaxRate"),
        "TaxAmt": col("source.TaxAmt"),
        "FunctionalTaxAmt": col("source.FunctionalTaxAmt"),
        "PONo": col("source.PONo"),
        "Type": col("source.Type"),
        "JobDescription": col("source.JobDescription"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "AccountManagerName": col("source.AccountManagerName"),
        "CustomerName": col("source.CustomerName"),
        "ItemDescription": col("source.ItemDescription"),
        "Discount": col("source.Discount"),
        "SiteCode": col("source.SiteCode"),
        "CurrencyValue": col("source.CurrencyValue"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyCode": col("source.CurrencyCode"),
        "CountryCode": col("source.CountryCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "FunctionalExchangeRate": col("source.FunctionalExchangeRate"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "TransferItemId": col("source.TransferItemId"),
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_accounts_export_items")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_accounts_export_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_ExportItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.ExportItemId = S.CT_ExportItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("ExportItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.ExportItemId") == col("b.ExportItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.ExportItemId"),
        col("a.ExportId"),
        col("a.InvoiceId"),
        col("a.InvoiceDate"),
        col("a.CustomerAccountNo"),
        col("a.TaxCode"),
        col("a.SalesAcct"),
        col("a.SalesAmt"),
        col("a.FunctionalSalesAmt"),
        col("a.TaxRate"),
        col("a.TaxAmt"),
        col("a.FunctionalTaxAmt"),
        col("a.PONo"),
        col("a.Type"),
        col("a.JobDescription"),
        col("a.JobId"),
        col("a.JobVersion"),
        col("a.AccountManagerName"),
        col("a.CustomerName"),
        col("a.ItemDescription"),
        col("a.Discount"),
        col("a.SiteCode"),
        col("a.CurrencyValue"),
        col("a.ExchangeRate"),
        col("a.CurrencyId"),
        col("a.CurrencyCode"),
        col("a.CountryCode"),
        col("a.SiteSubCode"),
        col("a.FunctionalCurrencyId"),
        col("a.FunctionalExchangeRate"),
        col("a.InvoiceItemId"),
        col("a.CreditNoteItemId"),
        col("a.TransferItemId")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ExportItemId = source.ExportItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExportItemId": col("source.ExportItemId"),
        "ExportId": col("source.ExportId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDate": col("source.InvoiceDate"),
        "CustomerAccountNo": col("source.CustomerAccountNo"),
        "TaxCode": col("source.TaxCode"),
        "SalesAcct": col("source.SalesAcct"),
        "SalesAmt": col("source.SalesAmt"),
        "FunctionalSalesAmt": col("source.FunctionalSalesAmt"),
        "TaxRate": col("source.TaxRate"),
        "TaxAmt": col("source.TaxAmt"),
        "FunctionalTaxAmt": col("source.FunctionalTaxAmt"),
        "PONo": col("source.PONo"),
        "Type": col("source.Type"),
        "JobDescription": col("source.JobDescription"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "AccountManagerName": col("source.AccountManagerName"),
        "CustomerName": col("source.CustomerName"),
        "ItemDescription": col("source.ItemDescription"),
        "Discount": col("source.Discount"),
        "SiteCode": col("source.SiteCode"),
        "CurrencyValue": col("source.CurrencyValue"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyCode": col("source.CurrencyCode"),
        "CountryCode": col("source.CountryCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "FunctionalExchangeRate": col("source.FunctionalExchangeRate"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "TransferItemId": col("source.TransferItemId"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ExportItemId": col("source.ExportItemId"),
        "ExportId": col("source.ExportId"),
        "InvoiceId": col("source.InvoiceId"),
        "InvoiceDate": col("source.InvoiceDate"),
        "CustomerAccountNo": col("source.CustomerAccountNo"),
        "TaxCode": col("source.TaxCode"),
        "SalesAcct": col("source.SalesAcct"),
        "SalesAmt": col("source.SalesAmt"),
        "FunctionalSalesAmt": col("source.FunctionalSalesAmt"),
        "TaxRate": col("source.TaxRate"),
        "TaxAmt": col("source.TaxAmt"),
        "FunctionalTaxAmt": col("source.FunctionalTaxAmt"),
        "PONo": col("source.PONo"),
        "Type": col("source.Type"),
        "JobDescription": col("source.JobDescription"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "AccountManagerName": col("source.AccountManagerName"),
        "CustomerName": col("source.CustomerName"),
        "ItemDescription": col("source.ItemDescription"),
        "Discount": col("source.Discount"),
        "SiteCode": col("source.SiteCode"),
        "CurrencyValue": col("source.CurrencyValue"),
        "ExchangeRate": col("source.ExchangeRate"),
        "CurrencyId": col("source.CurrencyId"),
        "CurrencyCode": col("source.CurrencyCode"),
        "CountryCode": col("source.CountryCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "FunctionalExchangeRate": col("source.FunctionalExchangeRate"),
        "InvoiceItemId": col("source.InvoiceItemId"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "TransferItemId": col("source.TransferItemId"),
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

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
    StructField("CT_CreditNoteItemId", StringType(), True),
	StructField("CreditNoteItemId", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
	StructField("ItemDescription", StringType(), True),
	StructField("ItemQuantity", StringType(), True),
	StructField("ItemCost", StringType(), True),
    StructField("ItemNominalCode", StringType(), True),
    StructField("ItemExported", StringType(), True),
    StructField("ItemExportDateTime", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("InvoiceItemExId", StringType(), True),
    StructField("CurrencyCost", StringType(), True),
    StructField("ItemSalesDepartmentCode", StringType(), True),
    StructField("ItemSalesTaxCode", StringType(), True),
	StructField("ItemSalesTaxExempt", StringType(), True),
	StructField("SiteId", StringType(), True),
	StructField("ItemOrder", StringType(), True),
	StructField("CreditNoteRollUpItemId", StringType(), True)
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
   StructField("CreditNoteItemId", StringType(), True),
   StructField("CreditNoteId", StringType(), True),
   StructField("ItemDescription", StringType(), True),
   StructField("ItemQuantity", StringType(), True),
   StructField("ItemCost", StringType(), True),
   StructField("ItemNominalCode", StringType(), True),
   StructField("ItemExported", StringType(), True),
   StructField("ItemExportDateTime", StringType(), True),
   StructField("JobItemId", StringType(), True),
   StructField("InvoiceItemExId", StringType(), True),
   StructField("CurrencyCost", StringType(), True),
   StructField("ItemSalesDepartmentCode", StringType(), True),
   StructField("ItemSalesTaxCode", StringType(), True),
   StructField("ItemSalesTaxExempt", StringType(), True),
   StructField("SiteId", StringType(), True),
   StructField("ItemOrder", StringType(), True),
   StructField("CreditNoteRollUpItemId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_credit_note_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_credit_note_items")

    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_note_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.CreditNoteItemId = source.CreditNoteItemId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ItemDescription": col("source.ItemDescription"),
        "ItemQuantity": col("source.ItemQuantity"),
		"ItemCost": col("source.ItemCost"),
        "ItemNominalCode": col("source.ItemNominalCode"),
        "ItemExported": col("source.ItemExported"),
        "ItemExportDateTime": col("source.ItemExportDateTime"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemExId": col("source.InvoiceItemExId"),
        "CurrencyCost": col("source.CurrencyCost"),
        "ItemSalesDepartmentCode": col("source.ItemSalesDepartmentCode"),
		"ItemSalesTaxCode": col("source.ItemSalesTaxCode"),
        "ItemSalesTaxExempt": col("source.ItemSalesTaxExempt"),
        "SiteId": col("source.SiteId"),
        "ItemOrder": col("source.ItemOrder"),
        "CreditNoteRollUpItemId": col("source.CreditNoteRollUpItemId")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ItemDescription": col("source.ItemDescription"),
        "ItemQuantity": col("source.ItemQuantity"),
		"ItemCost": col("source.ItemCost"),
        "ItemNominalCode": col("source.ItemNominalCode"),
        "ItemExported": col("source.ItemExported"),
        "ItemExportDateTime": col("source.ItemExportDateTime"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemExId": col("source.InvoiceItemExId"),
        "CurrencyCost": col("source.CurrencyCost"),
        "ItemSalesDepartmentCode": col("source.ItemSalesDepartmentCode"),
		"ItemSalesTaxCode": col("source.ItemSalesTaxCode"),
        "ItemSalesTaxExempt": col("source.ItemSalesTaxExempt"),
        "SiteId": col("source.SiteId"),
        "ItemOrder": col("source.ItemOrder"),
        "CreditNoteRollUpItemId": col("source.CreditNoteRollUpItemId")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_credit_note_items")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_credit_note_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_CreditNoteItemId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_CreditNoteItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.CreditNoteItemId = S.CT_CreditNoteItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("CreditNoteItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.CreditNoteItemId") == col("b.CreditNoteItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.CreditNoteItemId"),
        col("a.CreditNoteId"),
        col("a.ItemDescription"),
        col("a.ItemQuantity"),
		col("a.ItemCost"),
        col("a.ItemNominalCode"),
        col("a.ItemExported"),
        col("a.ItemExportDateTime"),
        col("a.JobItemId"),
        col("a.InvoiceItemExId"),
        col("a.CurrencyCost"),
        col("a.ItemSalesDepartmentCode"),
		col("a.ItemSalesTaxCode"),
        col("a.ItemSalesTaxExempt"),
        col("a.SiteId"),
        col("a.ItemOrder"),
        col("a.CreditNoteRollUpItemId")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.CreditNoteItemId = source.CreditNoteItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ItemDescription": col("source.ItemDescription"),
        "ItemQuantity": col("source.ItemQuantity"),
		"ItemCost": col("source.ItemCost"),
        "ItemNominalCode": col("source.ItemNominalCode"),
        "ItemExported": col("source.ItemExported"),
        "ItemExportDateTime": col("source.ItemExportDateTime"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemExId": col("source.InvoiceItemExId"),
        "CurrencyCost": col("source.CurrencyCost"),
        "ItemSalesDepartmentCode": col("source.ItemSalesDepartmentCode"),
		"ItemSalesTaxCode": col("source.ItemSalesTaxCode"),
        "ItemSalesTaxExempt": col("source.ItemSalesTaxExempt"),
        "SiteId": col("source.SiteId"),
        "ItemOrder": col("source.ItemOrder"),
        "CreditNoteRollUpItemId": col("source.CreditNoteRollUpItemId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "CreditNoteItemId": col("source.CreditNoteItemId"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ItemDescription": col("source.ItemDescription"),
        "ItemQuantity": col("source.ItemQuantity"),
		"ItemCost": col("source.ItemCost"),
        "ItemNominalCode": col("source.ItemNominalCode"),
        "ItemExported": col("source.ItemExported"),
        "ItemExportDateTime": col("source.ItemExportDateTime"),
        "JobItemId": col("source.JobItemId"),
        "InvoiceItemExId": col("source.InvoiceItemExId"),
        "CurrencyCost": col("source.CurrencyCost"),
        "ItemSalesDepartmentCode": col("source.ItemSalesDepartmentCode"),
		"ItemSalesTaxCode": col("source.ItemSalesTaxCode"),
        "ItemSalesTaxExempt": col("source.ItemSalesTaxExempt"),
        "SiteId": col("source.SiteId"),
        "ItemOrder": col("source.ItemOrder"),
        "CreditNoteRollUpItemId": col("source.CreditNoteRollUpItemId")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

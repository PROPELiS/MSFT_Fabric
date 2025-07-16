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
    StructField("CT_JobItemId", StringType(), True),
    StructField("JobItemId", StringType(), True),
    StructField("JobStageId", StringType(), True),
    StructField("PriceMatrixItemId", StringType(), True),
    StructField("PriceMatrixMultiplierId", StringType(), True),
    StructField("JobItemStatus", StringType(), True),
    StructField("JobItemDescription", StringType(), True),
    StructField("JobItemAlias", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Cost", StringType(), True),
    StructField("PlateType", StringType(), True),
    StructField("ItemDueDateTime", StringType(), True),
    StructField("ItemDueReason", StringType(), True),
    StructField("ItemBookedDateTime", StringType(), True),
    StructField("ItemKPI", StringType(), True),
    StructField("ItemKPIDuration", StringType(), True),
    StructField("ItemCompletedDateTime", StringType(), True),
    StructField("TargetDeadline", StringType(), True),
    StructField("TargetDeadlineDate", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("ExchangeRateValue", StringType(), True),
    StructField("CurrencyCost", StringType(), True),
    StructField("PriceMatrixItemSalesTaxCode", StringType(), True),
    StructField("PriceMatrixItemSalesTaxExempt", StringType(), True),
    StructField("OriginalJobItemId", StringType(), True),
    StructField("AccountManagerLoginId", StringType(), True),
    StructField("AssignedTo", StringType(), True),
    StructField("PriceMatrixVariantItemId", StringType(), True),
    StructField("ItemOrder", StringType(), True),
    StructField("WorkflowTemplateItemId", StringType(), True),
    StructField("CreatedByInvoiceId", StringType(), True),
    StructField("POLineNumber", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("UpdatedBy", StringType(), True),
    StructField("UpdatedDateTime", StringType(), True),
    StructField("ItemComplexity", StringType(), True),
    StructField("DimensionComments", StringType(), True),
    StructField("PatchConfirmationLoginId", StringType(), True),
    StructField("PatchConfirmationLoginDate", StringType(), True),
    StructField("DimensionsLocked", StringType(), True)
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
    StructField("JobItemId", StringType(), True),
    StructField("JobStageId", StringType(), True),
    StructField("PriceMatrixItemId", StringType(), True),
    StructField("PriceMatrixMultiplierId", StringType(), True),
    StructField("JobItemStatus", StringType(), True),
    StructField("JobItemDescription", StringType(), True),
    StructField("JobItemAlias", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Cost", StringType(), True),
    StructField("PlateType", StringType(), True),
    StructField("ItemDueDateTime", StringType(), True),
    StructField("ItemDueReason", StringType(), True),
    StructField("ItemBookedDateTime", StringType(), True),
    StructField("ItemKPI", StringType(), True),
    StructField("ItemKPIDuration", StringType(), True),
    StructField("ItemCompletedDateTime", StringType(), True),
    StructField("TargetDeadline", StringType(), True),
    StructField("TargetDeadlineDate", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("ExchangeRateValue", StringType(), True),
    StructField("CurrencyCost", StringType(), True),
    StructField("PriceMatrixItemSalesTaxCode", StringType(), True),
    StructField("PriceMatrixItemSalesTaxExempt", StringType(), True),
    StructField("OriginalJobItemId", StringType(), True),
    StructField("AccountManagerLoginId", StringType(), True),
    StructField("AssignedTo", StringType(), True),
    StructField("PriceMatrixVariantItemId", StringType(), True),
    StructField("ItemOrder", StringType(), True),
    StructField("WorkflowTemplateItemId", StringType(), True),
    StructField("CreatedByInvoiceId", StringType(), True),
    StructField("POLineNumber", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("UpdatedBy", StringType(), True),
    StructField("UpdatedDateTime", StringType(), True),
    StructField("ItemComplexity", StringType(), True),
    StructField("DimensionComments", StringType(), True),
    StructField("PatchConfirmationLoginId", StringType(), True),
    StructField("PatchConfirmationLoginDate", StringType(), True),
    StructField("DimensionsLocked", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_job_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_job_items")
    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
   
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobItemId = source.JobItemId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobItemId": col("source.JobItemId"),
        "JobStageId": col("source.JobStageId"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixMultiplierId": col("source.PriceMatrixMultiplierId"),
        "JobItemStatus": col("source.JobItemStatus"),
        "JobItemDescription": col("source.JobItemDescription"),
        "JobItemAlias": col("source.JobItemAlias"),
        "Quantity": col("source.Quantity"),
        "Cost": col("source.Cost"),
        "PlateType": col("source.PlateType"),
        "ItemDueDateTime": col("source.ItemDueDateTime"),
        "ItemDueReason": col("source.ItemDueReason"),
        "ItemBookedDateTime": col("source.ItemBookedDateTime"),
        "ItemKPI": col("source.ItemKPI"),
        "ItemKPIDuration": col("source.ItemKPIDuration"),
        "ItemCompletedDateTime": col("source.ItemCompletedDateTime"),
        "TargetDeadline": col("source.TargetDeadline"),
        "TargetDeadlineDate": col("source.TargetDeadlineDate"),
        "CurrencyId": col("source.CurrencyId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "CurrencyCost": col("source.CurrencyCost"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "PriceMatrixItemSalesTaxExempt": col("source.PriceMatrixItemSalesTaxExempt"),
        "OriginalJobItemId": col("source.OriginalJobItemId"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "AssignedTo": col("source.AssignedTo"),
        "PriceMatrixVariantItemId": col("source.PriceMatrixVariantItemId"),
        "ItemOrder": col("source.ItemOrder"),
        "WorkflowTemplateItemId": col("source.WorkflowTemplateItemId"),
        "CreatedByInvoiceId": col("source.CreatedByInvoiceId"),
        "POLineNumber": col("source.POLineNumber"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "UpdatedDateTime": col("source.UpdatedDateTime"),
        "ItemComplexity": col("source.ItemComplexity"),
        "DimensionComments": col("source.DimensionComments"),
        "PatchConfirmationLoginId": col("source.PatchConfirmationLoginId"),
        "PatchConfirmationLoginDate": col("source.PatchConfirmationLoginDate"),
        "DimensionsLocked": col("source.DimensionsLocked")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobItemId": col("source.JobItemId"),
        "JobStageId": col("source.JobStageId"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixMultiplierId": col("source.PriceMatrixMultiplierId"),
        "JobItemStatus": col("source.JobItemStatus"),
        "JobItemDescription": col("source.JobItemDescription"),
        "JobItemAlias": col("source.JobItemAlias"),
        "Quantity": col("source.Quantity"),
        "Cost": col("source.Cost"),
        "PlateType": col("source.PlateType"),
        "ItemDueDateTime": col("source.ItemDueDateTime"),
        "ItemDueReason": col("source.ItemDueReason"),
        "ItemBookedDateTime": col("source.ItemBookedDateTime"),
        "ItemKPI": col("source.ItemKPI"),
        "ItemKPIDuration": col("source.ItemKPIDuration"),
        "ItemCompletedDateTime": col("source.ItemCompletedDateTime"),
        "TargetDeadline": col("source.TargetDeadline"),
        "TargetDeadlineDate": col("source.TargetDeadlineDate"),
        "CurrencyId": col("source.CurrencyId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "CurrencyCost": col("source.CurrencyCost"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "PriceMatrixItemSalesTaxExempt": col("source.PriceMatrixItemSalesTaxExempt"),
        "OriginalJobItemId": col("source.OriginalJobItemId"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "AssignedTo": col("source.AssignedTo"),
        "PriceMatrixVariantItemId": col("source.PriceMatrixVariantItemId"),
        "ItemOrder": col("source.ItemOrder"),
        "WorkflowTemplateItemId": col("source.WorkflowTemplateItemId"),
        "CreatedByInvoiceId": col("source.CreatedByInvoiceId"),
        "POLineNumber": col("source.POLineNumber"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "UpdatedDateTime": col("source.UpdatedDateTime"),
        "ItemComplexity": col("source.ItemComplexity"),
        "DimensionComments": col("source.DimensionComments"),
        "PatchConfirmationLoginId": col("source.PatchConfirmationLoginId"),
        "PatchConfirmationLoginDate": col("source.PatchConfirmationLoginDate"),
        "DimensionsLocked": col("source.DimensionsLocked")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_job_items")
    
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobItemId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobItemId = S.CT_JobItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobItemId") == col("b.JobItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobItemId"),
        col("a.JobStageId"),
        col("a.PriceMatrixItemId"),
        col("a.PriceMatrixMultiplierId"),
        col("a.JobItemStatus"),
        col("a.JobItemDescription"),
        col("a.JobItemAlias"),
        col("a.Quantity"),
        col("a.Cost"),
        col("a.PlateType"),
        col("a.ItemDueDateTime"),
        col("a.ItemDueReason"),
        col("a.ItemBookedDateTime"),
        col("a.ItemKPI"),
        col("a.ItemKPIDuration"),
        col("a.ItemCompletedDateTime"),
        col("a.TargetDeadline"),
        col("a.TargetDeadlineDate"),
        col("a.CurrencyId"),
        col("a.ExchangeRateValue"),
        col("a.CurrencyCost"),
        col("a.PriceMatrixItemSalesTaxCode"),
        col("a.PriceMatrixItemSalesTaxExempt"),
        col("a.OriginalJobItemId"),
        col("a.AccountManagerLoginId"),
        col("a.AssignedTo"),
        col("a.PriceMatrixVariantItemId"),
        col("a.ItemOrder"),
        col("a.WorkflowTemplateItemId"),
        col("a.CreatedByInvoiceId"),
        col("a.POLineNumber"),
        col("a.CreatedBy"),
        col("a.UpdatedBy"),
        col("a.UpdatedDateTime"),
        col("a.ItemComplexity"),
        col("a.DimensionComments"),
        col("a.PatchConfirmationLoginId"),
        col("a.PatchConfirmationLoginDate"),
        col("a.DimensionsLocked")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobItemId = source.JobItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobItemId": col("source.JobItemId"),
        "JobStageId": col("source.JobStageId"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixMultiplierId": col("source.PriceMatrixMultiplierId"),
        "JobItemStatus": col("source.JobItemStatus"),
        "JobItemDescription": col("source.JobItemDescription"),
        "JobItemAlias": col("source.JobItemAlias"),
        "Quantity": col("source.Quantity"),
        "Cost": col("source.Cost"),
        "PlateType": col("source.PlateType"),
        "ItemDueDateTime": col("source.ItemDueDateTime"),
        "ItemDueReason": col("source.ItemDueReason"),
        "ItemBookedDateTime": col("source.ItemBookedDateTime"),
        "ItemKPI": col("source.ItemKPI"),
        "ItemKPIDuration": col("source.ItemKPIDuration"),
        "ItemCompletedDateTime": col("source.ItemCompletedDateTime"),
        "TargetDeadline": col("source.TargetDeadline"),
        "TargetDeadlineDate": col("source.TargetDeadlineDate"),
        "CurrencyId": col("source.CurrencyId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "CurrencyCost": col("source.CurrencyCost"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "PriceMatrixItemSalesTaxExempt": col("source.PriceMatrixItemSalesTaxExempt"),
        "OriginalJobItemId": col("source.OriginalJobItemId"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "AssignedTo": col("source.AssignedTo"),
        "PriceMatrixVariantItemId": col("source.PriceMatrixVariantItemId"),
        "ItemOrder": col("source.ItemOrder"),
        "WorkflowTemplateItemId": col("source.WorkflowTemplateItemId"),
        "CreatedByInvoiceId": col("source.CreatedByInvoiceId"),
        "POLineNumber": col("source.POLineNumber"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "UpdatedDateTime": col("source.UpdatedDateTime"),
        "ItemComplexity": col("source.ItemComplexity"),
        "DimensionComments": col("source.DimensionComments"),
        "PatchConfirmationLoginId": col("source.PatchConfirmationLoginId"),
        "PatchConfirmationLoginDate": col("source.PatchConfirmationLoginDate"),
        "DimensionsLocked": col("source.DimensionsLocked")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobItemId": col("source.JobItemId"),
        "JobStageId": col("source.JobStageId"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixMultiplierId": col("source.PriceMatrixMultiplierId"),
        "JobItemStatus": col("source.JobItemStatus"),
        "JobItemDescription": col("source.JobItemDescription"),
        "JobItemAlias": col("source.JobItemAlias"),
        "Quantity": col("source.Quantity"),
        "Cost": col("source.Cost"),
        "PlateType": col("source.PlateType"),
        "ItemDueDateTime": col("source.ItemDueDateTime"),
        "ItemDueReason": col("source.ItemDueReason"),
        "ItemBookedDateTime": col("source.ItemBookedDateTime"),
        "ItemKPI": col("source.ItemKPI"),
        "ItemKPIDuration": col("source.ItemKPIDuration"),
        "ItemCompletedDateTime": col("source.ItemCompletedDateTime"),
        "TargetDeadline": col("source.TargetDeadline"),
        "TargetDeadlineDate": col("source.TargetDeadlineDate"),
        "CurrencyId": col("source.CurrencyId"),
        "ExchangeRateValue": col("source.ExchangeRateValue"),
        "CurrencyCost": col("source.CurrencyCost"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "PriceMatrixItemSalesTaxExempt": col("source.PriceMatrixItemSalesTaxExempt"),
        "OriginalJobItemId": col("source.OriginalJobItemId"),
        "AccountManagerLoginId": col("source.AccountManagerLoginId"),
        "AssignedTo": col("source.AssignedTo"),
        "PriceMatrixVariantItemId": col("source.PriceMatrixVariantItemId"),
        "ItemOrder": col("source.ItemOrder"),
        "WorkflowTemplateItemId": col("source.WorkflowTemplateItemId"),
        "CreatedByInvoiceId": col("source.CreatedByInvoiceId"),
        "POLineNumber": col("source.POLineNumber"),
        "CreatedBy": col("source.CreatedBy"),
        "UpdatedBy": col("source.UpdatedBy"),
        "UpdatedDateTime": col("source.UpdatedDateTime"),
        "ItemComplexity": col("source.ItemComplexity"),
        "DimensionComments": col("source.DimensionComments"),
        "PatchConfirmationLoginId": col("source.PatchConfirmationLoginId"),
        "PatchConfirmationLoginDate": col("source.PatchConfirmationLoginDate"),
        "DimensionsLocked": col("source.DimensionsLocked")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

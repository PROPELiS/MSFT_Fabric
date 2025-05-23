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

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_JobVersionBarcodeId", StringType(), True),
    StructField("JobVersionBarcodeId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("BarcodeNumber", StringType(), True),
    StructField("BarcodeType", StringType(), True),
    StructField("BarcodeCol", StringType(), True),
    StructField("BarcodeLMI", StringType(), True),
    StructField("BarcodeMag", StringType(), True),
    StructField("BarcodeBWR", StringType(), True),
    StructField("BarcodeNotes", StringType(), True),
    StructField("NarrowBarWidth", StringType(), True),
    StructField("BarcodeHeight", StringType(), True),
    StructField("BearerBars", StringType(), True),
    StructField("AutoGenerateBarcode", StringType(), True),
    StructField("SymbolType", StringType(), True),
    StructField("CellSize", StringType(), True),
    StructField("Width", StringType(), True),
    StructField("AddCharacters", StringType(), True),
    StructField("PutTextOnTop", StringType(), True),
    StructField("AdditionalTextOffset", StringType(), True),
    StructField("InstancesInDesign", StringType(), True),
    StructField("MinimumSymbolSizeId", StringType(), True),
    StructField("ErrorCorrectionId", StringType(), True),
    StructField("PrintabilityGauges", StringType(), True),
    StructField("Ratio", StringType(), True),
    StructField("TextFormatId", StringType(), True),
    StructField("BarcodeNumberHash", StringType(), True)
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
    StructField("JobVersionBarcodeId", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("BarcodeNumber", StringType(), True),
    StructField("BarcodeType", StringType(), True),
    StructField("BarcodeCol", StringType(), True),
    StructField("BarcodeLMI", StringType(), True),
    StructField("BarcodeMag", StringType(), True),
    StructField("BarcodeBWR", StringType(), True),
    StructField("BarcodeNotes", StringType(), True),
    StructField("NarrowBarWidth", StringType(), True),
    StructField("BarcodeHeight", StringType(), True),
    StructField("BearerBars", StringType(), True),
    StructField("AutoGenerateBarcode", StringType(), True),
    StructField("SymbolType", StringType(), True),
    StructField("CellSize", StringType(), True),
    StructField("Width", StringType(), True),
    StructField("AddCharacters", StringType(), True),
    StructField("PutTextOnTop", StringType(), True),
    StructField("AdditionalTextOffset", StringType(), True),
    StructField("InstancesInDesign", StringType(), True),
    StructField("MinimumSymbolSizeId", StringType(), True),
    StructField("ErrorCorrectionId", StringType(), True),
    StructField("PrintabilityGauges", StringType(), True),
    StructField("Ratio", StringType(), True),
    StructField("TextFormatId", StringType(), True),
    StructField("BarcodeNumberHash", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_tech_spec_barcodes"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_tech_spec_barcodes")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_barcodes_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobVersionBarcodeId = source.JobVersionBarcodeId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionBarcodeId": col("source.JobVersionBarcodeId"),
        "JobVersionId": col("source.JobVersionId"),
        "BarcodeNumber": col("source.BarcodeNumber"),
        "BarcodeType": col("source.BarcodeType"),
        "BarcodeCol": col("source.BarcodeCol"),
        "BarcodeLMI": col("source.BarcodeLMI"),
        "BarcodeMag": col("source.BarcodeMag"),
        "BarcodeBWR": col("source.BarcodeBWR"),
        "BarcodeNotes": col("source.BarcodeNotes"),
        "NarrowBarWidth": col("source.NarrowBarWidth"),
        "BarcodeHeight": col("source.BarcodeHeight"),
        "BearerBars": col("source.BearerBars"),
        "AutoGenerateBarcode": col("source.AutoGenerateBarcode"),
        "SymbolType": col("source.SymbolType"),
        "CellSize": col("source.CellSize"),
        "Width": col("source.Width"),
        "AddCharacters": col("source.AddCharacters"),
        "PutTextOnTop": col("source.PutTextOnTop"),
        "AdditionalTextOffset": col("source.AdditionalTextOffset"),
        "InstancesInDesign": col("source.InstancesInDesign"),
        "MinimumSymbolSizeId": col("source.MinimumSymbolSizeId"),
        "ErrorCorrectionId": col("source.ErrorCorrectionId"),
        "PrintabilityGauges": col("source.PrintabilityGauges"),
        "Ratio": col("source.Ratio"),
        "TextFormatId": col("source.TextFormatId"),
        "BarcodeNumberHash": col("source.BarcodeNumberHash")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionBarcodeId": col("source.JobVersionBarcodeId"),
        "JobVersionId": col("source.JobVersionId"),
        "BarcodeNumber": col("source.BarcodeNumber"),
        "BarcodeType": col("source.BarcodeType"),
        "BarcodeCol": col("source.BarcodeCol"),
        "BarcodeLMI": col("source.BarcodeLMI"),
        "BarcodeMag": col("source.BarcodeMag"),
        "BarcodeBWR": col("source.BarcodeBWR"),
        "BarcodeNotes": col("source.BarcodeNotes"),
        "NarrowBarWidth": col("source.NarrowBarWidth"),
        "BarcodeHeight": col("source.BarcodeHeight"),
        "BearerBars": col("source.BearerBars"),
        "AutoGenerateBarcode": col("source.AutoGenerateBarcode"),
        "SymbolType": col("source.SymbolType"),
        "CellSize": col("source.CellSize"),
        "Width": col("source.Width"),
        "AddCharacters": col("source.AddCharacters"),
        "PutTextOnTop": col("source.PutTextOnTop"),
        "AdditionalTextOffset": col("source.AdditionalTextOffset"),
        "InstancesInDesign": col("source.InstancesInDesign"),
        "MinimumSymbolSizeId": col("source.MinimumSymbolSizeId"),
        "ErrorCorrectionId": col("source.ErrorCorrectionId"),
        "PrintabilityGauges": col("source.PrintabilityGauges"),
        "Ratio": col("source.Ratio"),
        "TextFormatId": col("source.TextFormatId"),
        "BarcodeNumberHash": col("source.BarcodeNumberHash")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_job_tech_spec_barcodes")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_barcodes_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobVersionBarcodeId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobVersionBarcodeId = S.CT_JobVersionBarcodeId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobVersionBarcodeId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobVersionBarcodeId") == col("b.JobVersionBarcodeId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobVersionBarcodeId"),
        col("a.JobVersionId"),
        col("a.BarcodeNumber"),
        col("a.BarcodeType"),
        col("a.BarcodeCol"),
        col("a.BarcodeLMI"),
        col("a.BarcodeMag"),
        col("a.BarcodeBWR"),
        col("a.BarcodeNotes"),
        col("a.NarrowBarWidth"),
        col("a.BarcodeHeight"),
        col("a.BearerBars"),
        col("a.AutoGenerateBarcode"),
        col("a.SymbolType"),
        col("a.CellSize"),
        col("a.Width"),
        col("a.AddCharacters"),
        col("a.PutTextOnTop"),
        col("a.AdditionalTextOffset"),
        col("a.InstancesInDesign"),
        col("a.MinimumSymbolSizeId"),
        col("a.ErrorCorrectionId"),
        col("a.PrintabilityGauges"),
        col("a.Ratio"),
        col("a.TextFormatId"),
        col("a.BarcodeNumberHash")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobVersionBarcodeId = source.JobVersionBarcodeId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionBarcodeId": col("source.JobVersionBarcodeId"),
        "JobVersionId": col("source.JobVersionId"),
        "BarcodeNumber": col("source.BarcodeNumber"),
        "BarcodeType": col("source.BarcodeType"),
        "BarcodeCol": col("source.BarcodeCol"),
        "BarcodeLMI": col("source.BarcodeLMI"),
        "BarcodeMag": col("source.BarcodeMag"),
        "BarcodeBWR": col("source.BarcodeBWR"),
        "BarcodeNotes": col("source.BarcodeNotes"),
        "NarrowBarWidth": col("source.NarrowBarWidth"),
        "BarcodeHeight": col("source.BarcodeHeight"),
        "BearerBars": col("source.BearerBars"),
        "AutoGenerateBarcode": col("source.AutoGenerateBarcode"),
        "SymbolType": col("source.SymbolType"),
        "CellSize": col("source.CellSize"),
        "Width": col("source.Width"),
        "AddCharacters": col("source.AddCharacters"),
        "PutTextOnTop": col("source.PutTextOnTop"),
        "AdditionalTextOffset": col("source.AdditionalTextOffset"),
        "InstancesInDesign": col("source.InstancesInDesign"),
        "MinimumSymbolSizeId": col("source.MinimumSymbolSizeId"),
        "ErrorCorrectionId": col("source.ErrorCorrectionId"),
        "PrintabilityGauges": col("source.PrintabilityGauges"),
        "Ratio": col("source.Ratio"),
        "TextFormatId": col("source.TextFormatId"),
        "BarcodeNumberHash": col("source.BarcodeNumberHash")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionBarcodeId": col("source.JobVersionBarcodeId"),
        "JobVersionId": col("source.JobVersionId"),
        "BarcodeNumber": col("source.BarcodeNumber"),
        "BarcodeType": col("source.BarcodeType"),
        "BarcodeCol": col("source.BarcodeCol"),
        "BarcodeLMI": col("source.BarcodeLMI"),
        "BarcodeMag": col("source.BarcodeMag"),
        "BarcodeBWR": col("source.BarcodeBWR"),
        "BarcodeNotes": col("source.BarcodeNotes"),
        "NarrowBarWidth": col("source.NarrowBarWidth"),
        "BarcodeHeight": col("source.BarcodeHeight"),
        "BearerBars": col("source.BearerBars"),
        "AutoGenerateBarcode": col("source.AutoGenerateBarcode"),
        "SymbolType": col("source.SymbolType"),
        "CellSize": col("source.CellSize"),
        "Width": col("source.Width"),
        "AddCharacters": col("source.AddCharacters"),
        "PutTextOnTop": col("source.PutTextOnTop"),
        "AdditionalTextOffset": col("source.AdditionalTextOffset"),
        "InstancesInDesign": col("source.InstancesInDesign"),
        "MinimumSymbolSizeId": col("source.MinimumSymbolSizeId"),
        "ErrorCorrectionId": col("source.ErrorCorrectionId"),
        "PrintabilityGauges": col("source.PrintabilityGauges"),
        "Ratio": col("source.Ratio"),
        "TextFormatId": col("source.TextFormatId"),
        "BarcodeNumberHash": col("source.BarcodeNumberHash")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM SILVER.dbo.tbl_job_tech_spec_barcodes")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

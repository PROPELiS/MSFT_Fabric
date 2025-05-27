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

# MARKDOWN ********************

# 
# #### Run the cell below to install the required packages for Copilot


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
    StructField("CT_JobVersionId", StringType(), True),
    StructField("JobVersionId", LongType(), True),
    StructField("JobId", StringType(), True),
    StructField("JobVersion", StringType(), True),
    StructField("JobStatus", StringType(), True),
    StructField("PackagingReference", StringType(), True),
    StructField("BookedDateTime", StringType(), True),
    StructField("JobRangeId", StringType(), True),
    StructField("Retailer", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("Variety", StringType(), True),
    StructField("Promotion", StringType(), True),
    StructField("Weight", StringType(), True),
    StructField("LanguageDesc", StringType(), True),
    StructField("PlateSizeUnitId", StringType(), True),
    StructField("TransferMethod", StringType(), True),
    StructField("TransferNotes", StringType(), True),
    StructField("SessionId", StringType(), True),
    StructField("LegacyDesignNo", StringType(), True),
    StructField("DespatchNotes", StringType(), True),
    StructField("ProjectId", StringType(), True),
    StructField("MAPSDesignRef", StringType(), True),
    StructField("MAPSBriefId", StringType(), True),
    StructField("MAPSLinked", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("FormattedJobNumber", StringType(), True),
    StructField("ClonedJobVersionId", StringType(), True),
    StructField("ArtReceived", StringType(), True),
    StructField("EndUserInfoReceived", StringType(), True),
    StructField("PrinterInfoReceived", StringType(), True),
    StructField("LastInfoReceived", StringType(), True),
    StructField("OrderTypeId", StringType(), True),
    StructField("PackTypeId", StringType(), True),
    StructField("JobRelationshipId", StringType(), True),
    StructField("ImportedContentSource", StringType(), True),
    StructField("OverallJobDueDate", StringType(), True),
    StructField("JobRelationship", StringType(), True),
    StructField("ISOLanguageCodes", StringType(), True),
    StructField("WordCount", StringType(), True),
    StructField("LanguageCount", StringType(), True),
    StructField("InvoicingStatusId", StringType(), True)
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
   StructField("JobVersionId", LongType(), True),
   StructField("JobId", StringType(), True),
   StructField("JobVersion", StringType(), True),
   StructField("JobStatus", StringType(), True),
   StructField("PackagingReference", StringType(), True),
   StructField("BookedDateTime", StringType(), True),
   StructField("JobRangeId", StringType(), True),
   StructField("Retailer", StringType(), True),
   StructField("Brand", StringType(), True),
   StructField("Variety", StringType(), True),
   StructField("Promotion", StringType(), True),
   StructField("Weight", StringType(), True),
   StructField("LanguageDesc", StringType(), True),
   StructField("PlateSizeUnitId", StringType(), True),
   StructField("TransferMethod", StringType(), True),
   StructField("TransferNotes", StringType(), True),
   StructField("SessionId", StringType(), True),
   StructField("LegacyDesignNo", StringType(), True),
   StructField("DespatchNotes", StringType(), True),
   StructField("ProjectId", StringType(), True),
   StructField("MAPSDesignRef", StringType(), True),
   StructField("MAPSBriefId", StringType(), True),
   StructField("MAPSLinked", StringType(), True),
   StructField("Priority", StringType(), True),
   StructField("FormattedJobNumber", StringType(), True),
   StructField("ClonedJobVersionId", StringType(), True),
   StructField("ArtReceived", StringType(), True),
   StructField("EndUserInfoReceived", StringType(), True),
   StructField("PrinterInfoReceived", StringType(), True),
   StructField("LastInfoReceived", StringType(), True),
   StructField("OrderTypeId", StringType(), True),
   StructField("PackTypeId", StringType(), True),
   StructField("JobRelationshipId", StringType(), True),
   StructField("ImportedContentSource", StringType(), True),
   StructField("OverallJobDueDate", StringType(), True),
   StructField("JobRelationship", StringType(), True),
   StructField("ISOLanguageCodes", StringType(), True),
   StructField("WordCount", StringType(), True),
   StructField("LanguageCount", StringType(), True),
   StructField("InvoicingStatusId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([],schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_versions"

# Initialize Delta Table reference
silver_table = DeltaTable.forPath(spark, silver_path)
param = ""  # Replace with the actual PARAM value



if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_versions")
   
    bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_versions_FULL"
   
    bronze_df = spark.read.format("delta").load(bronze_path) 

    

    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.JobVersionId = source.JobVersionId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionId": col("source.JobVersionId"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "JobStatus": col("source.JobStatus"),
        "PackagingReference": col("source.PackagingReference"),
        "BookedDateTime": col("source.BookedDateTime"),
        "JobRangeId": col("source.JobRangeId"),
        "Retailer": col("source.Retailer"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "Promotion": col("source.Promotion"),
        "Weight": col("source.Weight"),
        "LanguageDesc": col("source.LanguageDesc"),
        "PlateSizeUnitId": col("source.PlateSizeUnitId"),
        "TransferMethod": col("source.TransferMethod"),
        "TransferNotes": col("source.TransferNotes"),
        "SessionId": col("source.SessionId"),
        "LegacyDesignNo": col("source.LegacyDesignNo"),
        "DespatchNotes": col("source.DespatchNotes"),
        "ProjectId": col("source.ProjectId"),
        "MAPSDesignRef": col("source.MAPSDesignRef"),
        "MAPSBriefId": col("source.MAPSBriefId"),
        "MAPSLinked": col("source.MAPSLinked"),
        "Priority": col("source.Priority"),
        "FormattedJobNumber": col("source.FormattedJobNumber"),
        "ClonedJobVersionId": col("source.ClonedJobVersionId"),
        "ArtReceived": col("source.ArtReceived"),
        "EndUserInfoReceived": col("source.EndUserInfoReceived"),
        "PrinterInfoReceived": col("source.PrinterInfoReceived"),
        "LastInfoReceived": col("source.LastInfoReceived"),
        "OrderTypeId": col("source.OrderTypeId"),
        "PackTypeId": col("source.PackTypeId"),
        "JobRelationshipId": col("source.JobRelationshipId"),
        "ImportedContentSource": col("source.ImportedContentSource"),
        "OverallJobDueDate": col("source.OverallJobDueDate"),
        "JobRelationship": col("source.JobRelationship"),
        "ISOLanguageCodes": col("source.ISOLanguageCodes"),
        "WordCount": col("source.WordCount"),
        "LanguageCount": col("source.LanguageCount"),
        "InvoicingStatusId": col("source.InvoicingStatusId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionId": col("source.JobVersionId"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "JobStatus": col("source.JobStatus"),
        "PackagingReference": col("source.PackagingReference"),
        "BookedDateTime": col("source.BookedDateTime"),
        "JobRangeId": col("source.JobRangeId"),
        "Retailer": col("source.Retailer"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "Promotion": col("source.Promotion"),
        "Weight": col("source.Weight"),
        "LanguageDesc": col("source.LanguageDesc"),
        "PlateSizeUnitId": col("source.PlateSizeUnitId"),
        "TransferMethod": col("source.TransferMethod"),
        "TransferNotes": col("source.TransferNotes"),
        "SessionId": col("source.SessionId"),
        "LegacyDesignNo": col("source.LegacyDesignNo"),
        "DespatchNotes": col("source.DespatchNotes"),
        "ProjectId": col("source.ProjectId"),
        "MAPSDesignRef": col("source.MAPSDesignRef"),
        "MAPSBriefId": col("source.MAPSBriefId"),
        "MAPSLinked": col("source.MAPSLinked"),
        "Priority": col("source.Priority"),
        "FormattedJobNumber": col("source.FormattedJobNumber"),
        "ClonedJobVersionId": col("source.ClonedJobVersionId"),
        "ArtReceived": col("source.ArtReceived"),
        "EndUserInfoReceived": col("source.EndUserInfoReceived"),
        "PrinterInfoReceived": col("source.PrinterInfoReceived"),
        "LastInfoReceived": col("source.LastInfoReceived"),
        "OrderTypeId": col("source.OrderTypeId"),
        "PackTypeId": col("source.PackTypeId"),
        "JobRelationshipId": col("source.JobRelationshipId"),
        "ImportedContentSource": col("source.ImportedContentSource"),
        "OverallJobDueDate": col("source.OverallJobDueDate"),
        "JobRelationship": col("source.JobRelationship"),
        "ISOLanguageCodes": col("source.ISOLanguageCodes"),
        "WordCount": col("source.WordCount"),
        "LanguageCount": col("source.LanguageCount"),
        "InvoicingStatusId": col("source.InvoicingStatusId")
    }).execute()
   
else:

    df.write.format("delta").mode("append").saveAsTable("tbl_job_versions")
   
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_versions_DELTA"
   # Read source and target as Delta Tables
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobVersionId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobVersionId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobVersionId = S.CT_JobVersionId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobVersionId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobVersionId") == col("b.JobVersionId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobVersionId"),
        col("a.JobId"),
        col("a.JobVersion"),
        col("a.JobStatus"),
        col("a.PackagingReference"),
        col("a.BookedDateTime"),
        col("a.JobRangeId"),
        col("a.Retailer"),
        col("a.Brand"),
        col("a.Variety"),
        col("a.Promotion"),
        col("a.Weight"),
        col("a.LanguageDesc"),
        col("a.PlateSizeUnitId"),
        col("a.TransferMethod"),
        col("a.TransferNotes"),
        col("a.SessionId"),
        col("a.LegacyDesignNo"),
        col("a.DespatchNotes"),
        col("a.ProjectId"),
        col("a.MAPSDesignRef"),
        col("a.MAPSBriefId"),
        col("a.MAPSLinked"),
        col("a.Priority"),
        col("a.FormattedJobNumber"),
        col("a.ClonedJobVersionId"),
        col("a.ArtReceived"),
        col("a.EndUserInfoReceived"),
        col("a.PrinterInfoReceived"),
        col("a.LastInfoReceived"),
        col("a.OrderTypeId"),
        col("a.PackTypeId"),
        col("a.JobRelationshipId"),
        col("a.ImportedContentSource"),
        col("a.OverallJobDueDate"),
        col("a.JobRelationship"),
        col("a.ISOLanguageCodes"),
        col("a.WordCount"),
        col("a.LanguageCount"),
        col("a.InvoicingStatusId")
    ).distinct()
       
    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobVersionId = source.JobVersionId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionId": col("source.JobVersionId"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "JobStatus": col("source.JobStatus"),
        "PackagingReference": col("source.PackagingReference"),
        "BookedDateTime": col("source.BookedDateTime"),
        "JobRangeId": col("source.JobRangeId"),
        "Retailer": col("source.Retailer"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "Promotion": col("source.Promotion"),
        "Weight": col("source.Weight"),
        "LanguageDesc": col("source.LanguageDesc"),
        "PlateSizeUnitId": col("source.PlateSizeUnitId"),
        "TransferMethod": col("source.TransferMethod"),
        "TransferNotes": col("source.TransferNotes"),
        "SessionId": col("source.SessionId"),
        "LegacyDesignNo": col("source.LegacyDesignNo"),
        "DespatchNotes": col("source.DespatchNotes"),
        "ProjectId": col("source.ProjectId"),
        "MAPSDesignRef": col("source.MAPSDesignRef"),
        "MAPSBriefId": col("source.MAPSBriefId"),
        "MAPSLinked": col("source.MAPSLinked"),
        "Priority": col("source.Priority"),
        "FormattedJobNumber": col("source.FormattedJobNumber"),
        "ClonedJobVersionId": col("source.ClonedJobVersionId"),
        "ArtReceived": col("source.ArtReceived"),
        "EndUserInfoReceived": col("source.EndUserInfoReceived"),
        "PrinterInfoReceived": col("source.PrinterInfoReceived"),
        "LastInfoReceived": col("source.LastInfoReceived"),
        "OrderTypeId": col("source.OrderTypeId"),
        "PackTypeId": col("source.PackTypeId"),
        "JobRelationshipId": col("source.JobRelationshipId"),
        "ImportedContentSource": col("source.ImportedContentSource"),
        "OverallJobDueDate": col("source.OverallJobDueDate"),
        "JobRelationship": col("source.JobRelationship"),
        "ISOLanguageCodes": col("source.ISOLanguageCodes"),
        "WordCount": col("source.WordCount"),
        "LanguageCount": col("source.LanguageCount"),
        "InvoicingStatusId": col("source.InvoicingStatusId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobVersionId": col("source.JobVersionId"),
        "JobId": col("source.JobId"),
        "JobVersion": col("source.JobVersion"),
        "JobStatus": col("source.JobStatus"),
        "PackagingReference": col("source.PackagingReference"),
        "BookedDateTime": col("source.BookedDateTime"),
        "JobRangeId": col("source.JobRangeId"),
        "Retailer": col("source.Retailer"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "Promotion": col("source.Promotion"),
        "Weight": col("source.Weight"),
        "LanguageDesc": col("source.LanguageDesc"),
        "PlateSizeUnitId": col("source.PlateSizeUnitId"),
        "TransferMethod": col("source.TransferMethod"),
        "TransferNotes": col("source.TransferNotes"),
        "SessionId": col("source.SessionId"),
        "LegacyDesignNo": col("source.LegacyDesignNo"),
        "DespatchNotes": col("source.DespatchNotes"),
        "ProjectId": col("source.ProjectId"),
        "MAPSDesignRef": col("source.MAPSDesignRef"),
        "MAPSBriefId": col("source.MAPSBriefId"),
        "MAPSLinked": col("source.MAPSLinked"),
        "Priority": col("source.Priority"),
        "FormattedJobNumber": col("source.FormattedJobNumber"),
        "ClonedJobVersionId": col("source.ClonedJobVersionId"),
        "ArtReceived": col("source.ArtReceived"),
        "EndUserInfoReceived": col("source.EndUserInfoReceived"),
        "PrinterInfoReceived": col("source.PrinterInfoReceived"),
        "LastInfoReceived": col("source.LastInfoReceived"),
        "OrderTypeId": col("source.OrderTypeId"),
        "PackTypeId": col("source.PackTypeId"),
        "JobRelationshipId": col("source.JobRelationshipId"),
        "ImportedContentSource": col("source.ImportedContentSource"),
        "OverallJobDueDate": col("source.OverallJobDueDate"),
        "JobRelationship": col("source.JobRelationship"),
        "ISOLanguageCodes": col("source.ISOLanguageCodes"),
        "WordCount": col("source.WordCount"),
        "LanguageCount": col("source.LanguageCount"),
        "InvoicingStatusId": col("source.InvoicingStatusId")
    }).execute()
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

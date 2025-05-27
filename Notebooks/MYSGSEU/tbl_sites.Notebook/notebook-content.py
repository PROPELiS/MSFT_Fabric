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
# META       "default_lakehouse_workspace_id": "de3e35d4-28a5-4df0-a8d1-00feff73469d"
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
    StructField("CT_SiteId", StringType(), True),
	StructField("SiteId", StringType(), True),
    StructField("SiteName", StringType(), True),
	StructField("SiteJobFolderRoot", StringType(), True),
	StructField("SiteAdminUID", StringType(), True),
	StructField("SiteAdminPWD", StringType(), True),
    StructField("SiteAdminDomain", StringType(), True),
    StructField("SiteCode", StringType(), True),
    StructField("SiteSubCode", StringType(), True),
    StructField("CompanyLegalName", StringType(), True),
    StructField("LegalAddress", StringType(), True),
    StructField("Logo", StringType(), True),
    StructField("RemitAddress", StringType(), True),
    StructField("CompanyNumberId", StringType(), True),
    StructField("LocationBusinessCode", StringType(), True),
	StructField("FunctionalCurrencyId", StringType(), True),
	StructField("RegionId", StringType(), True),
	StructField("BankInformation", StringType(), True),
	StructField("TimeZoneId", StringType(), True),
	StructField("CountryId", StringType(), True),
	StructField("IsGPNSite", StringType(), True),
	StructField("VLMEnabled", StringType(), True),
    StructField("ShiftReAssignment", StringType(), True),
	StructField("ShiftReAssignmentSplitJobs", StringType(), True),
	StructField("DateFormatID", StringType(), True)
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
   StructField("SiteId", StringType(), True),
   StructField("SiteName", StringType(), True),
   StructField("SiteJobFolderRoot", StringType(), True),
   StructField("SiteAdminUID", StringType(), True),
   StructField("SiteAdminPWD", StringType(), True),
   StructField("SiteAdminDomain", StringType(), True),
   StructField("SiteCode", StringType(), True),
   StructField("SiteSubCode", StringType(), True),
   StructField("CompanyLegalName", StringType(), True),
   StructField("LegalAddress", StringType(), True),
   StructField("Logo", StringType(), True),
   StructField("RemitAddress", StringType(), True),
   StructField("CompanyNumberId", StringType(), True),
   StructField("LocationBusinessCode", StringType(), True),
   StructField("FunctionalCurrencyId", StringType(), True),
   StructField("RegionId", StringType(), True),
   StructField("BankInformation", StringType(), True),
   StructField("TimeZoneId", StringType(), True),
   StructField("CountryId", StringType(), True),
   StructField("IsGPNSite", StringType(), True),
   StructField("VLMEnabled", StringType(), True),
   StructField("ShiftReAssignment", StringType(), True),
   StructField("ShiftReAssignmentSplitJobs", StringType(), True),
   StructField("DateFormatID", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_sites"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_sites")

    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_sites_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.SiteId = source.SiteId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "SiteId": col("source.SiteId"),
        "SiteName": col("source.SiteName"),
        "SiteJobFolderRoot": col("source.SiteJobFolderRoot"),
        "SiteAdminUID": col("source.SiteAdminUID"),
		"SiteAdminPWD": col("source.SiteAdminPWD"),
        "SiteAdminDomain": col("source.SiteAdminDomain"),
        "SiteCode": col("source.SiteCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "CompanyLegalName": col("source.CompanyLegalName"),
        "LegalAddress": col("source.LegalAddress"),
        "Logo": col("source.Logo"),
        "RemitAddress": col("source.RemitAddress"),
        "CompanyNumberId": col("source.CompanyNumberId"),
        "LocationBusinessCode": col("source.LocationBusinessCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "RegionId": col("source.RegionId"),
        "BankInformation": col("source.BankInformation"),
        "TimeZoneId": col("source.TimeZoneId"),
        "CountryId": col("source.CountryId"),
        "IsGPNSite": col("source.IsGPNSite"),
        "VLMEnabled": col("source.VLMEnabled"),
        "ShiftReAssignment": col("source.ShiftReAssignment"),
        "ShiftReAssignmentSplitJobs": col("source.ShiftReAssignmentSplitJobs"),
        "DateFormatID": col("source.DateFormatID")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "SiteId": col("source.SiteId"),
        "SiteName": col("source.SiteName"),
        "SiteJobFolderRoot": col("source.SiteJobFolderRoot"),
        "SiteAdminUID": col("source.SiteAdminUID"),
		"SiteAdminPWD": col("source.SiteAdminPWD"),
        "SiteAdminDomain": col("source.SiteAdminDomain"),
        "SiteCode": col("source.SiteCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "CompanyLegalName": col("source.CompanyLegalName"),
        "LegalAddress": col("source.LegalAddress"),
        "Logo": col("source.Logo"),
        "RemitAddress": col("source.RemitAddress"),
        "CompanyNumberId": col("source.CompanyNumberId"),
        "LocationBusinessCode": col("source.LocationBusinessCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "RegionId": col("source.RegionId"),
        "BankInformation": col("source.BankInformation"),
        "TimeZoneId": col("source.TimeZoneId"),
        "CountryId": col("source.CountryId"),
        "IsGPNSite": col("source.IsGPNSite"),
        "VLMEnabled": col("source.VLMEnabled"),
        "ShiftReAssignment": col("source.ShiftReAssignment"),
        "ShiftReAssignmentSplitJobs": col("source.ShiftReAssignmentSplitJobs"),
        "DateFormatID": col("source.DateFormatID")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_sites")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_sites_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_SiteId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_SiteId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.SiteId = S.CT_SiteId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("SiteId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.SiteId") == col("b.SiteId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.SiteId"),
        col("a.SiteName"),
        col("a.SiteJobFolderRoot"),
        col("a.SiteAdminUID"),
		col("a.SiteAdminPWD"),
        col("a.SiteAdminDomain"),
        col("a.SiteCode"),
        col("a.SiteSubCode"),
        col("a.CompanyLegalName"),
        col("a.LegalAddress"),
        col("a.Logo"),
        col("a.RemitAddress"),
        col("a.CompanyNumberId"),
        col("a.LocationBusinessCode"),
        col("a.FunctionalCurrencyId"),
        col("a.RegionId"),
        col("a.BankInformation"),
        col("a.TimeZoneId"),
        col("a.CountryId"),
        col("a.IsGPNSite"),
        col("a.VLMEnabled"),
        col("a.ShiftReAssignment"),
        col("a.ShiftReAssignmentSplitJobs"),
        col("a.DateFormatID")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.SiteId = source.SiteId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "SiteId": col("source.SiteId"),
        "SiteName": col("source.SiteName"),
        "SiteJobFolderRoot": col("source.SiteJobFolderRoot"),
        "SiteAdminUID": col("source.SiteAdminUID"),
		"SiteAdminPWD": col("source.SiteAdminPWD"),
        "SiteAdminDomain": col("source.SiteAdminDomain"),
        "SiteCode": col("source.SiteCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "CompanyLegalName": col("source.CompanyLegalName"),
        "LegalAddress": col("source.LegalAddress"),
        "Logo": col("source.Logo"),
        "RemitAddress": col("source.RemitAddress"),
        "CompanyNumberId": col("source.CompanyNumberId"),
        "LocationBusinessCode": col("source.LocationBusinessCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "RegionId": col("source.RegionId"),
        "BankInformation": col("source.BankInformation"),
        "TimeZoneId": col("source.TimeZoneId"),
        "CountryId": col("source.CountryId"),
        "IsGPNSite": col("source.IsGPNSite"),
        "VLMEnabled": col("source.VLMEnabled"),
        "ShiftReAssignment": col("source.ShiftReAssignment"),
        "ShiftReAssignmentSplitJobs": col("source.ShiftReAssignmentSplitJobs"),
        "DateFormatID": col("source.DateFormatID")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "SiteId": col("source.SiteId"),
        "SiteName": col("source.SiteName"),
        "SiteJobFolderRoot": col("source.SiteJobFolderRoot"),
        "SiteAdminUID": col("source.SiteAdminUID"),
		"SiteAdminPWD": col("source.SiteAdminPWD"),
        "SiteAdminDomain": col("source.SiteAdminDomain"),
        "SiteCode": col("source.SiteCode"),
        "SiteSubCode": col("source.SiteSubCode"),
        "CompanyLegalName": col("source.CompanyLegalName"),
        "LegalAddress": col("source.LegalAddress"),
        "Logo": col("source.Logo"),
        "RemitAddress": col("source.RemitAddress"),
        "CompanyNumberId": col("source.CompanyNumberId"),
        "LocationBusinessCode": col("source.LocationBusinessCode"),
        "FunctionalCurrencyId": col("source.FunctionalCurrencyId"),
        "RegionId": col("source.RegionId"),
        "BankInformation": col("source.BankInformation"),
        "TimeZoneId": col("source.TimeZoneId"),
        "CountryId": col("source.CountryId"),
        "IsGPNSite": col("source.IsGPNSite"),
        "VLMEnabled": col("source.VLMEnabled"),
        "ShiftReAssignment": col("source.ShiftReAssignment"),
        "ShiftReAssignmentSplitJobs": col("source.ShiftReAssignmentSplitJobs"),
        "DateFormatID": col("source.DateFormatID")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(*) FROM SILVER.dbo.tbl_sites")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

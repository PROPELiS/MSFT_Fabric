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
    StructField("CT_ProfileId", StringType(), True),
    StructField("ProfileId", StringType(), True),
    StructField("LoginId", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("ParentId", StringType(), True),
    StructField("EmailUID", StringType(), True),
    StructField("EmailPWD", StringType(), True),
    StructField("HeaderPreference", StringType(), True),
    StructField("AddressBookPreference", StringType(), True),
    StructField("HelpPreference", StringType(), True),
    StructField("LoggingLevel", StringType(), True),
    StructField("DefaultRefresh", StringType(), True),
    StructField("DefaultRecordsetSize", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("UseThumbs", StringType(), True),
    StructField("LoginURL", StringType(), True),
    StructField("O365Integration", StringType(), True),
    StructField("TimeZoneId", StringType(), True),
    StructField("DateFormatId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("ActiveDirectoryId", StringType(), True),
    StructField("ePADExUserName", StringType(), True),
    StructField("UseDefaultEmail", StringType(), True),
    StructField("GreenlightSkin", StringType(), True),
    StructField("GreenlightMeasurementUnits", StringType(), True),
    StructField("CollapseModules", StringType(), True),
    StructField("HistorySize", StringType(), True),
    StructField("UltiproId", StringType(), True),
    StructField("DefaultRecordsetSizeSGS5", StringType(), True),
    StructField("EmployeeTypeId", StringType(), True),
    StructField("VLMExclusion", StringType(), True),
    StructField("ChecklistLinkPreference", StringType(), True),
    StructField("DefaultIntegrationInstance", StringType(), True),
    StructField("ThemeName", StringType(), True),
    StructField("HomepagePreference", StringType(), True),
    StructField("Layout", StringType(), True)
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
    StructField("ProfileId", StringType(), True),
    StructField("LoginId", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("ParentId", StringType(), True),
    StructField("EmailUID", StringType(), True),
    StructField("EmailPWD", StringType(), True),
    StructField("HeaderPreference", StringType(), True),
    StructField("AddressBookPreference", StringType(), True),
    StructField("HelpPreference", StringType(), True),
    StructField("LoggingLevel", StringType(), True),
    StructField("DefaultRefresh", StringType(), True),
    StructField("DefaultRecordsetSize", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("UseThumbs", StringType(), True),
    StructField("LoginURL", StringType(), True),
    StructField("O365Integration", StringType(), True),
    StructField("TimeZoneId", StringType(), True),
    StructField("DateFormatId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("ActiveDirectoryId", StringType(), True),
    StructField("ePADExUserName", StringType(), True),
    StructField("UseDefaultEmail", StringType(), True),
    StructField("GreenlightSkin", StringType(), True),
    StructField("GreenlightMeasurementUnits", StringType(), True),
    StructField("CollapseModules", StringType(), True),
    StructField("HistorySize", StringType(), True),
    StructField("UltiproId", StringType(), True),
    StructField("DefaultRecordsetSizeSGS5", StringType(), True),
    StructField("EmployeeTypeId", StringType(), True),
    StructField("VLMExclusion", StringType(), True),
    StructField("ChecklistLinkPreference", StringType(), True),
    StructField("DefaultIntegrationInstance", StringType(), True),
    StructField("ThemeName", StringType(), True),
    StructField("HomepagePreference", StringType(), True),
    StructField("Layout", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_login_profiles_FULL"
silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_login_profiles"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
#param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_login_profiles")
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
   
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
       bronze_df.alias("source"),
       "target.ProfileId = source.ProfileId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProfileId": "source.ProfileId",
        "LoginId": "source.LoginId",
        "FirstName": "source.FirstName",
        "LastName": "source.LastName",
        "ParentId": "source.ParentId",
        "EmailUID": "source.EmailUID",
        "EmailPWD": "source.EmailPWD",
        "HeaderPreference": "source.HeaderPreference",
        "AddressBookPreference": "source.AddressBookPreference",
        "HelpPreference": "source.HelpPreference",
        "LoggingLevel": "source.LoggingLevel",
        "DefaultRefresh": "source.DefaultRefresh",
        "DefaultRecordsetSize": "source.DefaultRecordsetSize",
        "SiteId": "source.SiteId",
        "UseThumbs": "source.UseThumbs",
        "LoginURL": "source.LoginURL",
        "O365Integration": "source.O365Integration",
        "TimeZoneId": "source.TimeZoneId",
        "DateFormatId": "source.DateFormatId",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "ActiveDirectoryId": "source.ActiveDirectoryId",
        "ePADExUserName": "source.ePADExUserName",
        "UseDefaultEmail": "source.UseDefaultEmail",
        "GreenlightSkin": "source.GreenlightSkin",
        "GreenlightMeasurementUnits": "source.GreenlightMeasurementUnits",
        "CollapseModules": "source.CollapseModules",
        "HistorySize": "source.HistorySize",
        "UltiproId": "source.UltiproId",
        "DefaultRecordsetSizeSGS5": "source.DefaultRecordsetSizeSGS5",
        "EmployeeTypeId": "source.EmployeeTypeId",
        "VLMExclusion": "source.VLMExclusion",
        "ChecklistLinkPreference": "source.ChecklistLinkPreference",
        "DefaultIntegrationInstance": "source.DefaultIntegrationInstance",
        "ThemeName": "source.ThemeName",
        "HomepagePreference": "source.HomepagePreference",
        "Layout": "source.Layout"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProfileId": "source.ProfileId",
        "LoginId": "source.LoginId",
        "FirstName": "source.FirstName",
        "LastName": "source.LastName",
        "ParentId": "source.ParentId",
        "EmailUID": "source.EmailUID",
        "EmailPWD": "source.EmailPWD",
        "HeaderPreference": "source.HeaderPreference",
        "AddressBookPreference": "source.AddressBookPreference",
        "HelpPreference": "source.HelpPreference",
        "LoggingLevel": "source.LoggingLevel",
        "DefaultRefresh": "source.DefaultRefresh",
        "DefaultRecordsetSize": "source.DefaultRecordsetSize",
        "SiteId": "source.SiteId",
        "UseThumbs": "source.UseThumbs",
        "LoginURL": "source.LoginURL",
        "O365Integration": "source.O365Integration",
        "TimeZoneId": "source.TimeZoneId",
        "DateFormatId": "source.DateFormatId",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "ActiveDirectoryId": "source.ActiveDirectoryId",
        "ePADExUserName": "source.ePADExUserName",
        "UseDefaultEmail": "source.UseDefaultEmail",
        "GreenlightSkin": "source.GreenlightSkin",
        "GreenlightMeasurementUnits": "source.GreenlightMeasurementUnits",
        "CollapseModules": "source.CollapseModules",
        "HistorySize": "source.HistorySize",
        "UltiproId": "source.UltiproId",
        "DefaultRecordsetSizeSGS5": "source.DefaultRecordsetSizeSGS5",
        "EmployeeTypeId": "source.EmployeeTypeId",
        "VLMExclusion": "source.VLMExclusion",
        "ChecklistLinkPreference": "source.ChecklistLinkPreference",
        "DefaultIntegrationInstance": "source.DefaultIntegrationInstance",
        "ThemeName": "source.ThemeName",
        "HomepagePreference": "source.HomepagePreference",
        "Layout": "source.Layout"
    }).execute()

else:
    # df = spark.createDataFrame([], schema)
    df.write.format("delta").mode("append").saveAsTable("tbl_login_profiles")
    
   # Define paths
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_login_profiles_DELTA"
   # Read source and target as Delta Tables
    silver_df_delta = DeltaTable.forPath(spark, silver_path)
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobFolderId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_ProfileId") \
    .distinct()


   # Perform the MERGE operation
    silver_df_delta.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.ProfileId = S.CT_ProfileId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("ProfileId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.ProfileId") == col("b.ProfileId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.ProfileId").alias("ProfileId"),
        col("a.LoginId").alias("LoginId"),
        col("a.FirstName").alias("FirstName"),
        col("a.LastName").alias("LastName"),
        col("a.ParentId").alias("ParentId"),
        col("a.EmailUID").alias("EmailUID"),
        col("a.EmailPWD").alias("EmailPWD"),
        col("a.HeaderPreference").alias("HeaderPreference"),
        col("a.AddressBookPreference").alias("AddressBookPreference"),
        col("a.HelpPreference").alias("HelpPreference"),
        col("a.LoggingLevel").alias("LoggingLevel"),
        col("a.DefaultRefresh").alias("DefaultRefresh"),
        col("a.DefaultRecordsetSize").alias("DefaultRecordsetSize"),
        col("a.SiteId").alias("SiteId"),
        col("a.UseThumbs").alias("UseThumbs"),
        col("a.LoginURL").alias("LoginURL"),
        col("a.O365Integration").alias("O365Integration"),
        col("a.TimeZoneId").alias("TimeZoneId"),
        col("a.DateFormatId").alias("DateFormatId"),
        col("a.CreatedDate").alias("CreatedDate"),
        col("a.UpdatedDate").alias("UpdatedDate"),
        col("a.ActiveDirectoryId").alias("ActiveDirectoryId"),
        col("a.ePADExUserName").alias("ePADExUserName"),
        col("a.UseDefaultEmail").alias("UseDefaultEmail"),
        col("a.GreenlightSkin").alias("GreenlightSkin"),
        col("a.GreenlightMeasurementUnits").alias("GreenlightMeasurementUnits"),
        col("a.CollapseModules").alias("CollapseModules"),
        col("a.HistorySize").alias("HistorySize"),
        col("a.UltiproId").alias("UltiproId"),
        col("a.DefaultRecordsetSizeSGS5").alias("DefaultRecordsetSizeSGS5"),
        col("a.EmployeeTypeId").alias("EmployeeTypeId"),
        col("a.VLMExclusion").alias("VLMExclusion"),
        col("a.ChecklistLinkPreference").alias("ChecklistLinkPreference"),
        col("a.DefaultIntegrationInstance").alias("DefaultIntegrationInstance"),
        col("a.ThemeName").alias("ThemeName"),
        col("a.HomepagePreference").alias("HomepagePreference"),
        col("a.Layout").alias("Layout")
    ).distinct()

    # Perform the MERGE operation
    silver_df_delta.alias("target").merge(
        transformed_df.alias("source"),
        "target.ProfileId = source.ProfileId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProfileId": "source.ProfileId",
        "LoginId": "source.LoginId",
        "FirstName": "source.FirstName",
        "LastName": "source.LastName",
        "ParentId": "source.ParentId",
        "EmailUID": "source.EmailUID",
        "EmailPWD": "source.EmailPWD",
        "HeaderPreference": "source.HeaderPreference",
        "AddressBookPreference": "source.AddressBookPreference",
        "HelpPreference": "source.HelpPreference",
        "LoggingLevel": "source.LoggingLevel",
        "DefaultRefresh": "source.DefaultRefresh",
        "DefaultRecordsetSize": "source.DefaultRecordsetSize",
        "SiteId": "source.SiteId",
        "UseThumbs": "source.UseThumbs",
        "LoginURL": "source.LoginURL",
        "O365Integration": "source.O365Integration",
        "TimeZoneId": "source.TimeZoneId",
        "DateFormatId": "source.DateFormatId",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "ActiveDirectoryId": "source.ActiveDirectoryId",
        "ePADExUserName": "source.ePADExUserName",
        "UseDefaultEmail": "source.UseDefaultEmail",
        "GreenlightSkin": "source.GreenlightSkin",
        "GreenlightMeasurementUnits": "source.GreenlightMeasurementUnits",
        "CollapseModules": "source.CollapseModules",
        "HistorySize": "source.HistorySize",
        "UltiproId": "source.UltiproId",
        "DefaultRecordsetSizeSGS5": "source.DefaultRecordsetSizeSGS5",
        "EmployeeTypeId": "source.EmployeeTypeId",
        "VLMExclusion": "source.VLMExclusion",
        "ChecklistLinkPreference": "source.ChecklistLinkPreference",
        "DefaultIntegrationInstance": "source.DefaultIntegrationInstance",
        "ThemeName": "source.ThemeName",
        "HomepagePreference": "source.HomepagePreference",
        "Layout": "source.Layout"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProfileId": "source.ProfileId",
        "LoginId": "source.LoginId",
        "FirstName": "source.FirstName",
        "LastName": "source.LastName",
        "ParentId": "source.ParentId",
        "EmailUID": "source.EmailUID",
        "EmailPWD": "source.EmailPWD",
        "HeaderPreference": "source.HeaderPreference",
        "AddressBookPreference": "source.AddressBookPreference",
        "HelpPreference": "source.HelpPreference",
        "LoggingLevel": "source.LoggingLevel",
        "DefaultRefresh": "source.DefaultRefresh",
        "DefaultRecordsetSize": "source.DefaultRecordsetSize",
        "SiteId": "source.SiteId",
        "UseThumbs": "source.UseThumbs",
        "LoginURL": "source.LoginURL",
        "O365Integration": "source.O365Integration",
        "TimeZoneId": "source.TimeZoneId",
        "DateFormatId": "source.DateFormatId",
        "CreatedDate": "source.CreatedDate",
        "UpdatedDate": "source.UpdatedDate",
        "ActiveDirectoryId": "source.ActiveDirectoryId",
        "ePADExUserName": "source.ePADExUserName",
        "UseDefaultEmail": "source.UseDefaultEmail",
        "GreenlightSkin": "source.GreenlightSkin",
        "GreenlightMeasurementUnits": "source.GreenlightMeasurementUnits",
        "CollapseModules": "source.CollapseModules",
        "HistorySize": "source.HistorySize",
        "UltiproId": "source.UltiproId",
        "DefaultRecordsetSizeSGS5": "source.DefaultRecordsetSizeSGS5",
        "EmployeeTypeId": "source.EmployeeTypeId",
        "VLMExclusion": "source.VLMExclusion",
        "ChecklistLinkPreference": "source.ChecklistLinkPreference",
        "DefaultIntegrationInstance": "source.DefaultIntegrationInstance",
        "ThemeName": "source.ThemeName",
        "HomepagePreference": "source.HomepagePreference",
        "Layout": "source.Layout"
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

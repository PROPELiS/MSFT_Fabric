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
orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_ContactId", StringType(), True),
    StructField("ContactId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("ContactFirstName", StringType(), True),
    StructField("ContactLastName", StringType(), True),
    StructField("ContactSalutation", StringType(), True),
    StructField("ContactPosition", StringType(), True),
    StructField("LoginId", StringType(), True),
    StructField("ContactDisabled", StringType(), True),
    StructField("ScoroId", StringType(), True)
])
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("ContactId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("ContactFirstName", StringType(), True),
    StructField("ContactLastName", StringType(), True),
    StructField("ContactSalutation", StringType(), True),
    StructField("ContactPosition", StringType(), True),
    StructField("LoginId", StringType(), True),
    StructField("ContactDisabled", StringType(), True),
    StructField("ScoroId", StringType(), True)
])

# Create an empty DataFrame
df = spark.createDataFrame([], schema)

# Define the Silver Delta Table Path
silver_table_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_customer_contacts"
silver_table = DeltaTable.forPath(spark, silver_table_path)

if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_customer_contacts")
    bronze_table_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_contacts_FULL"
    bronze_df = spark.read.format("delta").load(bronze_table_path)
    
   
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.ContactId = source.ContactId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "ContactFirstName": col("source.ContactFirstName"),
        "ContactLastName": col("source.ContactLastName"),
        "ContactSalutation": col("source.ContactSalutation"),
        "ContactPosition": col("source.ContactPosition"),
        "LoginId": col("source.LoginId"),
        "ContactDisabled": col("source.ContactDisabled"),
        "ScoroId": col("source.ScoroId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "ContactFirstName": col("source.ContactFirstName"),
        "ContactLastName": col("source.ContactLastName"),
        "ContactSalutation": col("source.ContactSalutation"),
        "ContactPosition": col("source.ContactPosition"),
        "LoginId": col("source.LoginId"),
        "ContactDisabled": col("source.ContactDisabled"),
        "ScoroId": col("source.ScoroId")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_customer_contacts")
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_contacts_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D").select("CT_ContactId").distinct()
    
    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.ContactId = S.CT_ContactId") \
        .whenMatchedDelete() \
        .execute()
    
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.alias("b"),
        (col("a.ContactId") == col("b.ContactId")) &
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.ContactId"),
        col("a.CustomerId"),
        col("a.ContactFirstName"),
        col("a.ContactLastName"),
        col("a.ContactSalutation"),
        col("a.ContactPosition"),
        col("a.LoginId"),
        col("a.ContactDisabled"),
        col("a.ScoroId")
    ).distinct()

    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ContactId = source.ContactId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "ContactFirstName": col("source.ContactFirstName"),
        "ContactLastName": col("source.ContactLastName"),
        "ContactSalutation": col("source.ContactSalutation"),
        "ContactPosition": col("source.ContactPosition"),
        "LoginId": col("source.LoginId"),
        "ContactDisabled": col("source.ContactDisabled"),
        "ScoroId": col("source.ScoroId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ContactId": col("source.ContactId"),
        "CustomerId": col("source.CustomerId"),
        "ContactFirstName": col("source.ContactFirstName"),
        "ContactLastName": col("source.ContactLastName"),
        "ContactSalutation": col("source.ContactSalutation"),
        "ContactPosition": col("source.ContactPosition"),
        "LoginId": col("source.LoginId"),
        "ContactDisabled": col("source.ContactDisabled"),
        "ScoroId": col("source.ScoroId")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

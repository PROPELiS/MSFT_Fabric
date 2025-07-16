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
    StructField("CT_DAXContactId", StringType(), True),
    StructField("DAXContactId", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("PartyId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("Id", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("InActive", StringType(), True),
    StructField("RemovedFromDAX", StringType(), True),
    StructField("Role", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the new schema with the required columns
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("DAXContactId", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("PartyId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("Id", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("InActive", StringType(), True),
    StructField("RemovedFromDAX", StringType(), True),
    StructField("Role", StringType(), True)
])

# Create an empty DataFrame with the new schema
df = spark.createDataFrame([], schema)

# Overwrite the existing Delta table with the new schema
silver_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_dax_customer_contacts"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
 # Replace with the actual IN_MODE value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_DAX_customer_contacts")
    
    bronze_Path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_DAX_customer_contacts_FULL"
    
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
  
    
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.DAXContactId = source.DAXContactId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXContactId": col("source.DAXContactId"),
        "DAXCustomerId": col("source.DAXCustomerId"),
        "PartyId": col("source.PartyId"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "Id": col("source.Id"),
        "Type": col("source.Type"),
        "Email": col("source.Email"),
        "InActive": col("source.InActive"),
        "RemovedFromDAX": col("source.RemovedFromDAX"),
        "Role": col("source.Role")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXContactId": col("source.DAXContactId"),
        "DAXCustomerId": col("source.DAXCustomerId"),
        "PartyId": col("source.PartyId"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "Id": col("source.Id"),
        "Type": col("source.Type"),
        "Email": col("source.Email"),
        "InActive": col("source.InActive"),
        "RemovedFromDAX": col("source.RemovedFromDAX"),
        "Role": col("source.Role")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_DAX_customer_contacts")
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_DAX_customer_contacts_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_DAXContactId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
        .select("CT_DAXContactId") \
        .distinct()

    # Perform the MERGE operation to delete records
    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.DAXContactId = S.CT_DAXContactId") \
        .whenMatchedDelete() \
        .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("DAXContactId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.DAXContactId") == col("b.DAXContactId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.DAXContactId"),
        col("a.DAXCustomerId"),
        col("a.PartyId"),
        col("a.CreatedDate"),
        col("a.UpdatedDate"),
        col("a.Id"),
        col("a.Type"),
        col("a.Email"),
        col("a.InActive"),
        col("a.RemovedFromDAX"),
        col("a.Role")
    ).distinct()

    # Perform the MERGE operation for insert/update
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.DAXContactId = source.DAXContactId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXContactId": col("source.DAXContactId"),
        "DAXCustomerId": col("source.DAXCustomerId"),
        "PartyId": col("source.PartyId"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "Id": col("source.Id"),
        "Type": col("source.Type"),
        "Email": col("source.Email"),
        "InActive": col("source.InActive"),
        "RemovedFromDAX": col("source.RemovedFromDAX"),
        "Role": col("source.Role")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "DAXContactId": col("source.DAXContactId"),
        "DAXCustomerId": col("source.DAXCustomerId"),
        "PartyId": col("source.PartyId"),
        "CreatedDate": col("source.CreatedDate"),
        "UpdatedDate": col("source.UpdatedDate"),
        "Id": col("source.Id"),
        "Type": col("source.Type"),
        "Email": col("source.Email"),
        "InActive": col("source.InActive"),
        "RemovedFromDAX": col("source.RemovedFromDAX"),
        "Role": col("source.Role")
    }).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

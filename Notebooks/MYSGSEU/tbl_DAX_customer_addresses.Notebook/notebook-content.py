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

from pyspark.sql.functions import concat_ws, expr, sha2, size, lit, col, row_number, array, struct, udf, current_timestamp, max as spark_max
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_DAXAddressId", StringType(), True),
    StructField("DAXAddressId", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("PartyId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("Id", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("IsPrimary", StringType(), True),
    StructField("AddressView", StringType(), True),
    StructField("Effective", StringType(), True),
    StructField("Expiration", StringType(), True),
    StructField("PurposeType", StringType(), True),
    StructField("RemovedFromDAX", StringType(), True)
])
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("DAXAddressId", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("PartyId", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("Id", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("IsPrimary", StringType(), True),
    StructField("AddressView", StringType(), True),
    StructField("Effective", StringType(), True),
    StructField("Expiration", StringType(), True),
    StructField("PurposeType", StringType(), True),
    StructField("RemovedFromDAX", StringType(), True)
])

    # Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_dax_customer_addresses"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_DAX_customer_addresses")
    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_DAX_customer_addresses_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
     
   # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.DAXAddressId = source.DAXAddressId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "DAXAddressId": col("source.DAXAddressId"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "PartyId": col("source.PartyId"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "Id": col("source.Id"),
    "Street": col("source.Street"),
    "City": col("source.City"),
    "State": col("source.State"),
    "PostalCode": col("source.PostalCode"),
    "Country": col("source.Country"),
    "IsPrimary": col("source.IsPrimary"),
    "AddressView": col("source.AddressView"),
    "Effective": col("source.Effective"),
    "Expiration": col("source.Expiration"),
    "PurposeType": col("source.PurposeType"),
    "RemovedFromDAX": col("source.RemovedFromDAX")
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "DAXAddressId": col("source.DAXAddressId"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "PartyId": col("source.PartyId"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "Id": col("source.Id"),
    "Street": col("source.Street"),
    "City": col("source.City"),
    "State": col("source.State"),
    "PostalCode": col("source.PostalCode"),
    "Country": col("source.Country"),
    "IsPrimary": col("source.IsPrimary"),
    "AddressView": col("source.AddressView"),
    "Effective": col("source.Effective"),
    "Expiration": col("source.Expiration"),
    "PurposeType": col("source.PurposeType"),
    "RemovedFromDAX": col("source.RemovedFromDAX")
}).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_DAX_customer_addresses")
source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_DAX_customer_addresses_DELTA"
source_df_delta = spark.read.format("delta").load(source_path)

# Filtering Deleted Records
filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
    .select("CT_DAXAddressId").distinct()
# Merging into Silver Table
silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.DAXAddressId = S.CT_DAXAddressId") \
    .whenMatchedDelete()

# Finding Latest SYS_CHANGE_VERSION per DAXAddressId
transformed_df = source_df_delta.alias("a").join(
    source_df_delta.groupBy("DAXAddressId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
    (col("a.DAXAddressId") == col("b.DAXAddressId")) & 
    (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
).where(col("a.SYS_CHANGE_OPERATION") != 'D')\
.select(
    col("a.SYS_CHANGE_VERSION"),
    col("a.SYS_CHANGE_OPERATION"),
    col("a.DAXAddressId"),         
    col("a.DAXCustomerId"),       
    col("a.PartyId"),       
    col("a.CreatedDate"),         
    col("a.UpdatedDate"),          
    col("a.Id"),          
    col("a.Street"),              
    col("a.City"),
    col("a.State"),
    col("a.PostalCode"),          
    col("a.Country"),     
    col("a.IsPrimary"),                 
    col("a.AddressView"),
    col("a.Effective"), 
    col("a.Expiration"),
    col("a.PurposeType"),     
    col("a.RemovedFromDAX")
).distinct()

silver_table.alias("target").merge(
    transformed_df.alias("source"),
    "target.DAXAddressId = source.DAXAddressId"
  ).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "DAXAddressId": col("source.DAXAddressId"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "PartyId": col("source.PartyId"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "Id": col("source.Id"),
    "Street": col("source.Street"),
    "City": col("source.City"),
    "State": col("source.State"),
    "PostalCode": col("source.PostalCode"),
    "Country": col("source.Country"),
    "IsPrimary": col("source.IsPrimary"),
    "AddressView": col("source.AddressView"),
    "Effective": col("source.Effective"),
    "Expiration": col("source.Expiration"),
    "PurposeType": col("source.PurposeType"),
    "RemovedFromDAX": col("source.RemovedFromDAX")
   }).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "DAXAddressId": col("source.DAXAddressId"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "PartyId": col("source.PartyId"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "Id": col("source.Id"),
    "Street": col("source.Street"),
    "City": col("source.City"),
    "State": col("source.State"),
    "PostalCode": col("source.PostalCode"),
    "Country": col("source.Country"),
    "IsPrimary": col("source.IsPrimary"),
    "AddressView": col("source.AddressView"),
    "Effective": col("source.Effective"),
    "Expiration": col("source.Expiration"),
    "PurposeType": col("source.PurposeType"),
    "RemovedFromDAX": col("source.RemovedFromDAX")
    }).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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


orderSchema = StructType([
    StructField("LocationCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("inactive", StringType(), True),
    StructField("ETL_BatchId", StringType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)
])


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


Schema = StructType([
    StructField("LocationCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("inactive", StringType(), True),
    StructField("ETL_BatchId", StringType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], Schema)
silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/Ultipro/locationlist"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("Ultipro.LocationList")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro_/_LocationList"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

    # Merge data from Bronze Full to Silver
    silver_table.alias("target") \
    .merge(
        bronze_df.alias("source"),
        (col("target.LocationCode") == col("source.LocationCode")) &
        (col("target.ETL_BatchId") == col("source.ETL_BatchId")) 
    ).whenMatchedUpdate(set={
     "LocationCode": col("source.LocationCode"),
     "Description": col("source.Description"),
     "Country": col("source.Country"),
     "inactive": col("source.inactive"),
     "ETL_BatchId": col("source.ETL_BatchId"),
     "ETL_ChangeTrackingOperation": col("source.ETL_ChangeTrackingOperation")
}).whenNotMatchedInsert(values={
     "LocationCode": col("source.LocationCode"),
     "Description": col("source.Description"),
     "Country": col("source.Country"),
     "inactive": col("source.inactive"),
     "ETL_BatchId": col("source.ETL_BatchId"),
     "ETL_ChangeTrackingOperation": col("source.ETL_ChangeTrackingOperation")
}).execute()
else :
 df.write.format("delta").mode("append").saveAsTable("Ultipro.LocationList")


source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro_/_LocationList"
source_df_delta = spark.read.format("delta").load(source_path).filter(col("ETL_ChangeTrackingOperation") != 'D')


# Perform the MERGE operation with only the new columns
silver_table.alias("target") \
    .merge(
        source_df_delta.alias("source"),
        (col("target.LocationCode") == col("source.LocationCode")) &
        (col("target.ETL_BatchId") == col("source.ETL_BatchId")) 
    ).whenMatchedUpdate(set={
     "LocationCode": col("source.LocationCode"),
     "Description": col("source.Description"),
     "Country": col("source.Country"),
     "inactive": col("source.inactive"),
     "ETL_BatchId": col("source.ETL_BatchId"),
     "ETL_ChangeTrackingOperation": col("source.ETL_ChangeTrackingOperation")
}).whenNotMatchedInsert(values={
     "LocationCode": col("source.LocationCode"),
     "Description": col("source.Description"),
     "Country": col("source.Country"),
     "inactive": col("source.inactive"),
     "ETL_BatchId": col("source.ETL_BatchId"),
     "ETL_ChangeTrackingOperation": col("source.ETL_ChangeTrackingOperation")
}).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(*) FROM SILVER.Ultipro.locationlist")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

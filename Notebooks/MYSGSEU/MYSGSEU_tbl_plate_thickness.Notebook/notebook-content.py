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
    StructField("CT_ThicknessId", StringType(), True),
    StructField("ThicknessId", StringType(), True),
    StructField("ThicknessDesc", StringType(), True),
    StructField("ThicknessDisproFactor", StringType(), True),
    StructField("ThicknessDisproFactorAlcan", StringType(), True),
    StructField("ThicknessDisproFactorTetra", StringType(), True),
    StructField("ThicknessDisproFactorWebtech", StringType(), True),
    StructField("ThicknessDisproFactorKobuschEgypt", StringType(), True),
    StructField("ThicknessDisproFactorASPLA", StringType(), True),
    StructField("ReliefDepth", StringType(), True),
    StructField("Tolerance", StringType(), True),
    StructField("ThicknessDisproFactorAmcorArgentan", StringType(), True),
    StructField("ThicknessDisproFactorAmcorHorsensWH", StringType(), True),
    StructField("ThicknessDisproFactorAmcorHorsensFK", StringType(), True)
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
    StructField("ThicknessId", StringType(), True),
    StructField("ThicknessDesc", StringType(), True),
    StructField("ThicknessDisproFactor", StringType(), True),
    StructField("ThicknessDisproFactorAlcan", StringType(), True),
    StructField("ThicknessDisproFactorTetra", StringType(), True),
    StructField("ThicknessDisproFactorWebtech", StringType(), True),
    StructField("ThicknessDisproFactorKobuschEgypt", StringType(), True),
    StructField("ThicknessDisproFactorASPLA", StringType(), True),
    StructField("ReliefDepth", StringType(), True),
    StructField("Tolerance", StringType(), True),
    StructField("ThicknessDisproFactorAmcorArgentan", StringType(), True),
    StructField("ThicknessDisproFactorAmcorHorsensWH", StringType(), True),
    StructField("ThicknessDisproFactorAmcorHorsensFK", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_plate_thickness"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_plate_thickness")
    # Write the DataFrame as a Delta table
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_plate_thickness_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
     
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.ThicknessId = source.ThicknessId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "ThicknessId": "source.ThicknessId",
    "ThicknessDesc": "source.ThicknessDesc",
    "ThicknessDisproFactor": "source.ThicknessDisproFactor",
    "ThicknessDisproFactorAlcan": "source.ThicknessDisproFactorAlcan",
    "ThicknessDisproFactorTetra": "source.ThicknessDisproFactorTetra",
    "ThicknessDisproFactorWebtech": "source.ThicknessDisproFactorWebtech",
    "ThicknessDisproFactorKobuschEgypt": "source.ThicknessDisproFactorKobuschEgypt",
    "ThicknessDisproFactorASPLA": "source.ThicknessDisproFactorASPLA",
    "ReliefDepth": "source.ReliefDepth",
    "Tolerance": "source.Tolerance",
    "ThicknessDisproFactorAmcorArgentan": "source.ThicknessDisproFactorAmcorArgentan",
    "ThicknessDisproFactorAmcorHorsensWH": "source.ThicknessDisproFactorAmcorHorsensWH",
    "ThicknessDisproFactorAmcorHorsensFK": "source.ThicknessDisproFactorAmcorHorsensFK"
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "ThicknessId": "source.ThicknessId",
    "ThicknessDesc": "source.ThicknessDesc",
    "ThicknessDisproFactor": "source.ThicknessDisproFactor",
    "ThicknessDisproFactorAlcan": "source.ThicknessDisproFactorAlcan",
    "ThicknessDisproFactorTetra": "source.ThicknessDisproFactorTetra",
    "ThicknessDisproFactorWebtech": "source.ThicknessDisproFactorWebtech",
    "ThicknessDisproFactorKobuschEgypt": "source.ThicknessDisproFactorKobuschEgypt",
    "ThicknessDisproFactorASPLA": "source.ThicknessDisproFactorASPLA",
    "ReliefDepth": "source.ReliefDepth",
    "Tolerance": "source.Tolerance",
    "ThicknessDisproFactorAmcorArgentan": "source.ThicknessDisproFactorAmcorArgentan",
    "ThicknessDisproFactorAmcorHorsensWH": "source.ThicknessDisproFactorAmcorHorsensWH",
    "ThicknessDisproFactorAmcorHorsensFK": "source.ThicknessDisproFactorAmcorHorsensFK"
}).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_plate_thickness")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_plate_thickness_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_ThicknessId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.ThicknessId = S.CT_ThicknessId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("ThicknessId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.ThicknessId") == col("b.ThicknessId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.ThicknessId").alias("ThicknessId"),
        col("a.ThicknessDesc").alias("ThicknessDesc"),
        col("a.ThicknessDisproFactor").alias("ThicknessDisproFactor"),
        col("a.ThicknessDisproFactorAlcan").alias("ThicknessDisproFactorAlcan"),
        col("a.ThicknessDisproFactorTetra").alias("ThicknessDisproFactorTetra"),
        col("a.ThicknessDisproFactorWebtech").alias("ThicknessDisproFactorWebtech"),
        col("a.ThicknessDisproFactorKobuschEgypt").alias("ThicknessDisproFactorKobuschEgypt"),
        col("a.ThicknessDisproFactorASPLA").alias("ThicknessDisproFactorASPLA"),
        col("a.ReliefDepth").alias("ReliefDepth"),
        col("a.Tolerance").alias("Tolerance"),
        col("a.ThicknessDisproFactorAmcorArgentan").alias("ThicknessDisproFactorAmcorArgentan"),
        col("a.ThicknessDisproFactorAmcorHorsensWH").alias("ThicknessDisproFactorAmcorHorsensWH"),
        col("a.ThicknessDisproFactorAmcorHorsensFK").alias("ThicknessDisproFactorAmcorHorsensFK")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ThicknessId = source.ThicknessId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ThicknessId": col("source.ThicknessId"),
        "ThicknessDesc": col("source.ThicknessDesc"),
        "ThicknessDisproFactor": col("source.ThicknessDisproFactor"),
        "ThicknessDisproFactorAlcan": col("source.ThicknessDisproFactorAlcan"),
        "ThicknessDisproFactorTetra": col("source.ThicknessDisproFactorTetra"),
        "ThicknessDisproFactorWebtech": col("source.ThicknessDisproFactorWebtech"),
        "ThicknessDisproFactorKobuschEgypt": col("source.ThicknessDisproFactorKobuschEgypt"),
        "ThicknessDisproFactorASPLA": col("source.ThicknessDisproFactorASPLA"),
        "ReliefDepth": col("source.ReliefDepth"),
        "Tolerance": col("source.Tolerance"),
        "ThicknessDisproFactorAmcorArgentan": col("source.ThicknessDisproFactorAmcorArgentan"),
        "ThicknessDisproFactorAmcorHorsensWH": col("source.ThicknessDisproFactorAmcorHorsensWH"),
        "ThicknessDisproFactorAmcorHorsensFK": col("source.ThicknessDisproFactorAmcorHorsensFK")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ThicknessId": col("source.ThicknessId"),
        "ThicknessDesc": col("source.ThicknessDesc"),
        "ThicknessDisproFactor": col("source.ThicknessDisproFactor"),
        "ThicknessDisproFactorAlcan": col("source.ThicknessDisproFactorAlcan"),
        "ThicknessDisproFactorTetra": col("source.ThicknessDisproFactorTetra"),
        "ThicknessDisproFactorWebtech": col("source.ThicknessDisproFactorWebtech"),
        "ThicknessDisproFactorKobuschEgypt": col("source.ThicknessDisproFactorKobuschEgypt"),
        "ThicknessDisproFactorASPLA": col("source.ThicknessDisproFactorASPLA"),
        "ReliefDepth": col("source.ReliefDepth"),
        "Tolerance": col("source.Tolerance"),
        "ThicknessDisproFactorAmcorArgentan": col("source.ThicknessDisproFactorAmcorArgentan"),
        "ThicknessDisproFactorAmcorHorsensWH": col("source.ThicknessDisproFactorAmcorHorsensWH"),
        "ThicknessDisproFactorAmcorHorsensFK": col("source.ThicknessDisproFactorAmcorHorsensFK")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

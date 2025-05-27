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
    StructField("CT_PDRId", StringType(), True),
    StructField("PDRId", StringType(), True),
    StructField("PDRNumber", StringType(), True),
    StructField("ProjectName", StringType(), True),
    StructField("GBU", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Scale", StringType(), True),
    StructField("MDO", StringType(), True),
    StructField("LeadCountry", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("LastUpdate", StringType(), True),
    StructField("FirstShipment", StringType(), True),
    StructField("XMLVersionNumber", StringType(), True),
    StructField("ProjectDescription", StringType(), True),
    StructField("EstimatedStartDate", StringType(), True),
    StructField("ActualStartDate", StringType(), True),
    StructField("ActualFinishDate", StringType(), True),
    StructField("ArtworkPackage", StringType(), True),
    StructField("Obsolete", StringType(), True)
    
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
    StructField("PDRId", StringType(), True),
    StructField("PDRNumber", StringType(), True),
    StructField("ProjectName", StringType(), True),
    StructField("GBU", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Scale", StringType(), True),
    StructField("MDO", StringType(), True),
    StructField("LeadCountry", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("LastUpdate", StringType(), True),
    StructField("FirstShipment", StringType(), True),
    StructField("XMLVersionNumber", StringType(), True),
    StructField("ProjectDescription", StringType(), True),
    StructField("EstimatedStartDate", StringType(), True),
    StructField("ActualStartDate", StringType(), True),
    StructField("ActualFinishDate", StringType(), True),
    StructField("ArtworkPackage", StringType(), True),
    StructField("Obsolete", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_pg_pdr"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_pg_pdr")
    # Write the DataFrame as a Delta table
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_pg_pdr_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.PDRId = source.PDRId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "PDRId": "source.PDRId",
    "PDRNumber": "source.PDRNumber",
    "ProjectName": "source.ProjectName",
    "GBU": "source.GBU",
    "Category": "source.Category",
    "Scale": "source.Scale",
    "MDO": "source.MDO",
    "LeadCountry": "source.LeadCountry",
    "Status": "source.Status",
    "LastUpdate": "source.LastUpdate",
    "FirstShipment": "source.FirstShipment",
    "XMLVersionNumber": "source.XMLVersionNumber",
    "ProjectDescription": "source.ProjectDescription",
    "EstimatedStartDate": "source.EstimatedStartDate",
    "ActualStartDate": "source.ActualStartDate",
    "ActualFinishDate": "source.ActualFinishDate",
    "ArtworkPackage": "source.ArtworkPackage",
    "Obsolete": "source.Obsolete"
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
    "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
    "PDRId": "source.PDRId",
    "PDRNumber": "source.PDRNumber",
    "ProjectName": "source.ProjectName",
    "GBU": "source.GBU",
    "Category": "source.Category",
    "Scale": "source.Scale",
    "MDO": "source.MDO",
    "LeadCountry": "source.LeadCountry",
    "Status": "source.Status",
    "LastUpdate": "source.LastUpdate",
    "FirstShipment": "source.FirstShipment",
    "XMLVersionNumber": "source.XMLVersionNumber",
    "ProjectDescription": "source.ProjectDescription",
    "EstimatedStartDate": "source.EstimatedStartDate",
    "ActualStartDate": "source.ActualStartDate",
    "ActualFinishDate": "source.ActualFinishDate",
    "ArtworkPackage": "source.ArtworkPackage",
    "Obsolete": "source.Obsolete"
}).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_pg_pdr")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_pg_pdr_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_PDRId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.PDRId = S.CT_PDRId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("PDRId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.PDRId") == col("b.PDRId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.PDRId").alias("PDRId"),
        col("a.PDRNumber").alias("PDRNumber"),
        col("a.ProjectName").alias("ProjectName"),
        col("a.GBU").alias("GBU"),
        col("a.Category").alias("Category"),
        col("a.Scale").alias("Scale"),
        col("a.MDO").alias("MDO"),
        col("a.LeadCountry").alias("LeadCountry"),
        col("a.Status").alias("Status"),
        col("a.LastUpdate").alias("LastUpdate"),
        col("a.FirstShipment").alias("FirstShipment"),
        col("a.XMLVersionNumber").alias("XMLVersionNumber"),
        col("a.ProjectDescription").alias("ProjectDescription"),
        col("a.EstimatedStartDate").alias("EstimatedStartDate"),
        col("a.ActualStartDate").alias("ActualStartDate"),
        col("a.ActualFinishDate").alias("ActualFinishDate"),
        col("a.ArtworkPackage").alias("ArtworkPackage"),
        col("a.Obsolete").alias("Obsolete")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.PDRId = source.PDRId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PDRId": col("source.PDRId"),
        "PDRNumber": col("source.PDRNumber"),
        "ProjectName": col("source.ProjectName"),
        "GBU": col("source.GBU"),
        "Category": col("source.Category"),
        "Scale": col("source.Scale"),
        "MDO": col("source.MDO"),
        "LeadCountry": col("source.LeadCountry"),
        "Status": col("source.Status"),
        "LastUpdate": col("source.LastUpdate"),
        "FirstShipment": col("source.FirstShipment"),
        "XMLVersionNumber": col("source.XMLVersionNumber"),
        "ProjectDescription": col("source.ProjectDescription"),
        "EstimatedStartDate": col("source.EstimatedStartDate"),
        "ActualStartDate": col("source.ActualStartDate"),
        "ActualFinishDate": col("source.ActualFinishDate"),
        "ArtworkPackage": col("source.ArtworkPackage"),
        "Obsolete": col("source.Obsolete"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PDRId": col("source.PDRId"),
        "PDRNumber": col("source.PDRNumber"),
        "ProjectName": col("source.ProjectName"),
        "GBU": col("source.GBU"),
        "Category": col("source.Category"),
        "Scale": col("source.Scale"),
        "MDO": col("source.MDO"),
        "LeadCountry": col("source.LeadCountry"),
        "Status": col("source.Status"),
        "LastUpdate": col("source.LastUpdate"),
        "FirstShipment": col("source.FirstShipment"),
        "XMLVersionNumber": col("source.XMLVersionNumber"),
        "ProjectDescription": col("source.ProjectDescription"),
        "EstimatedStartDate": col("source.EstimatedStartDate"),
        "ActualStartDate": col("source.ActualStartDate"),
        "ActualFinishDate": col("source.ActualFinishDate"),
        "ArtworkPackage": col("source.ArtworkPackage"),
        "Obsolete": col("source.Obsolete"),
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SILVER.dbo.tbl_pg_pdr LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_TechSpecColourCylinderId", StringType(), True),
	StructField("TechSpecColourCylinderId", StringType(), True),
    StructField("TechSpecColourId", StringType(), True),
	StructField("EngravingType", StringType(), True),
	StructField("EngravingProcess", StringType(), True),
	StructField("CylinderType", StringType(), True),
    StructField("SteppedDia", StringType(), True),
    StructField("BaseDiaReceived", StringType(), True),
    StructField("BaseDiaRequired", StringType(), True),
    StructField("BuildUpCutDown", StringType(), True),
    StructField("CylinderCircumference", StringType(), True),
    StructField("CylinderFaceLength", StringType(), True),
    StructField("CylinderreceivedState", StringType(), True),
    StructField("ChromeThickness", StringType(), True),
    StructField("MultiDesign", StringType(), True),
	StructField("NoOfCuts", StringType(), True),
	StructField("CylinderLocation", StringType(), True),
	StructField("RackBoxNo", StringType(), True),
	StructField("TestCutStart", StringType(), True),
	StructField("TestCutDrop", StringType(), True),
	StructField("CylinderNotes", StringType(), True),
	StructField("WebCompensation", StringType(), True),
    StructField("KeywayLaydown", StringType(), True)
	
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
   StructField("TechSpecColourCylinderId", StringType(), True),
   StructField("TechSpecColourId", StringType(), True),
   StructField("EngravingType", StringType(), True),
   StructField("EngravingProcess", StringType(), True),
   StructField("CylinderType", StringType(), True),
   StructField("SteppedDia", StringType(), True),
   StructField("BaseDiaReceived", StringType(), True),
   StructField("BaseDiaRequired", StringType(), True),
   StructField("BuildUpCutDown", StringType(), True),
   StructField("CylinderCircumference", StringType(), True),
   StructField("CylinderFaceLength", StringType(), True),
   StructField("CylinderreceivedState", StringType(), True),
   StructField("ChromeThickness", StringType(), True),
   StructField("MultiDesign", StringType(), True),
   StructField("NoOfCuts", StringType(), True),
   StructField("CylinderLocation", StringType(), True),
   StructField("RackBoxNo", StringType(), True),
   StructField("TestCutStart", StringType(), True),
   StructField("TestCutDrop", StringType(), True),
   StructField("CylinderNotes", StringType(), True),
   StructField("WebCompensation", StringType(), True),
   StructField("KeywayLaydown", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinders"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_job_tech_spec_colour_cylinders")

    bronze_Path ="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinders_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.TechSpecColourCylinderId = source.TechSpecColourCylinderId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "TechSpecColourId": col("source.TechSpecColourId"),
        "EngravingType": col("source.EngravingType"),
        "EngravingProcess": col("source.EngravingProcess"),
		"CylinderType": col("source.CylinderType"),
        "SteppedDia": col("source.SteppedDia"),
        "BaseDiaReceived": col("source.BaseDiaReceived"),
        "BaseDiaRequired": col("source.BaseDiaRequired"),
        "BuildUpCutDown": col("source.BuildUpCutDown"),
        "CylinderCircumference": col("source.CylinderCircumference"),
        "CylinderFaceLength": col("source.CylinderFaceLength"),
        "CylinderreceivedState": col("source.CylinderreceivedState"),
        "ChromeThickness": col("source.ChromeThickness"),
        "MultiDesign": col("source.MultiDesign"),
        "NoOfCuts": col("source.NoOfCuts"),
        "CylinderLocation": col("source.CylinderLocation"),
        "RackBoxNo": col("source.RackBoxNo"),
        "TestCutStart": col("source.TestCutStart"),
        "TestCutDrop": col("source.TestCutDrop"),
        "CylinderNotes": col("source.CylinderNotes"),
        "WebCompensation": col("source.WebCompensation"),
        "KeywayLaydown": col("source.KeywayLaydown")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "TechSpecColourId": col("source.TechSpecColourId"),
        "EngravingType": col("source.EngravingType"),
        "EngravingProcess": col("source.EngravingProcess"),
		"CylinderType": col("source.CylinderType"),
        "SteppedDia": col("source.SteppedDia"),
        "BaseDiaReceived": col("source.BaseDiaReceived"),
        "BaseDiaRequired": col("source.BaseDiaRequired"),
        "BuildUpCutDown": col("source.BuildUpCutDown"),
        "CylinderCircumference": col("source.CylinderCircumference"),
        "CylinderFaceLength": col("source.CylinderFaceLength"),
        "CylinderreceivedState": col("source.CylinderreceivedState"),
        "ChromeThickness": col("source.ChromeThickness"),
        "MultiDesign": col("source.MultiDesign"),
        "NoOfCuts": col("source.NoOfCuts"),
        "CylinderLocation": col("source.CylinderLocation"),
        "RackBoxNo": col("source.RackBoxNo"),
        "TestCutStart": col("source.TestCutStart"),
        "TestCutDrop": col("source.TestCutDrop"),
        "CylinderNotes": col("source.CylinderNotes"),
        "WebCompensation": col("source.WebCompensation"),
        "KeywayLaydown": col("source.KeywayLaydown")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_job_tech_spec_colour_cylinders")
    
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinders_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_TechSpecColourCylinderId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_TechSpecColourCylinderId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.TechSpecColourCylinderId = S.CT_TechSpecColourCylinderId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("TechSpecColourCylinderId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.TechSpecColourCylinderId") == col("b.TechSpecColourCylinderId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.TechSpecColourCylinderId"),
        col("a.TechSpecColourId"),
        col("a.EngravingType"),
        col("a.EngravingProcess"),
		col("a.CylinderType"),
        col("a.SteppedDia"),
        col("a.BaseDiaReceived"),
        col("a.BaseDiaRequired"),
        col("a.BuildUpCutDown"),
        col("a.CylinderCircumference"),
        col("a.CylinderFaceLength"),
        col("a.CylinderreceivedState"),
        col("a.ChromeThickness"),
        col("a.MultiDesign"),
        col("a.NoOfCuts"),
        col("a.CylinderLocation"),
        col("a.RackBoxNo"),
        col("a.TestCutStart"),
        col("a.TestCutDrop"),
        col("a.CylinderNotes"),
        col("a.WebCompensation"),
        col("a.KeywayLaydown")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.TechSpecColourCylinderId = source.TechSpecColourCylinderId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "TechSpecColourId": col("source.TechSpecColourId"),
        "EngravingType": col("source.EngravingType"),
        "EngravingProcess": col("source.EngravingProcess"),
		"CylinderType": col("source.CylinderType"),
        "SteppedDia": col("source.SteppedDia"),
        "BaseDiaReceived": col("source.BaseDiaReceived"),
        "BaseDiaRequired": col("source.BaseDiaRequired"),
        "BuildUpCutDown": col("source.BuildUpCutDown"),
        "CylinderCircumference": col("source.CylinderCircumference"),
        "CylinderFaceLength": col("source.CylinderFaceLength"),
        "CylinderreceivedState": col("source.CylinderreceivedState"),
        "ChromeThickness": col("source.ChromeThickness"),
        "MultiDesign": col("source.MultiDesign"),
        "NoOfCuts": col("source.NoOfCuts"),
        "CylinderLocation": col("source.CylinderLocation"),
        "RackBoxNo": col("source.RackBoxNo"),
        "TestCutStart": col("source.TestCutStart"),
        "TestCutDrop": col("source.TestCutDrop"),
        "CylinderNotes": col("source.CylinderNotes"),
        "WebCompensation": col("source.WebCompensation"),
        "KeywayLaydown": col("source.KeywayLaydown")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "TechSpecColourId": col("source.TechSpecColourId"),
        "EngravingType": col("source.EngravingType"),
        "EngravingProcess": col("source.EngravingProcess"),
		"CylinderType": col("source.CylinderType"),
        "SteppedDia": col("source.SteppedDia"),
        "BaseDiaReceived": col("source.BaseDiaReceived"),
        "BaseDiaRequired": col("source.BaseDiaRequired"),
        "BuildUpCutDown": col("source.BuildUpCutDown"),
        "CylinderCircumference": col("source.CylinderCircumference"),
        "CylinderFaceLength": col("source.CylinderFaceLength"),
        "CylinderreceivedState": col("source.CylinderreceivedState"),
        "ChromeThickness": col("source.ChromeThickness"),
        "MultiDesign": col("source.MultiDesign"),
        "NoOfCuts": col("source.NoOfCuts"),
        "CylinderLocation": col("source.CylinderLocation"),
        "RackBoxNo": col("source.RackBoxNo"),
        "TestCutStart": col("source.TestCutStart"),
        "TestCutDrop": col("source.TestCutDrop"),
        "CylinderNotes": col("source.CylinderNotes"),
        "WebCompensation": col("source.WebCompensation"),
        "KeywayLaydown": col("source.KeywayLaydown")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

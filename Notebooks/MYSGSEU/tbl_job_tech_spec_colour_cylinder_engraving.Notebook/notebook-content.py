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
    StructField("CT_TechSpecColourCylinderEngravingId", StringType(), True),
	StructField("TechSpecColourCylinderEngravingId", StringType(), True),
    StructField("TechSpecColourCylinderId", StringType(), True),
	StructField("PrintScenario", StringType(), True),
	StructField("ProfileName", StringType(), True),
	StructField("GammaCurve", StringType(), True),
    StructField("ScreenLpi", StringType(), True),
    StructField("Angle", StringType(), True),
    StructField("StylusDepth", StringType(), True),
    StructField("ShadowCell", StringType(), True),
    StructField("Channel", StringType(), True),
    StructField("CellWall", StringType(), True),
    StructField("MidToneCell", StringType(), True),
    StructField("TestCutVolt", StringType(), True),
    StructField("HighlightCell", StringType(), True),
	StructField("CellShape", StringType(), True),
	StructField("Edge", StringType(), True),
	StructField("FlexoPerfection", StringType(), True),
	StructField("PostElimination", StringType(), True),
	StructField("SchepersCell", StringType(), True)
	
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
   StructField("TechSpecColourCylinderEngravingId", StringType(), True),
   StructField("TechSpecColourCylinderId", StringType(), True),
   StructField("PrintScenario", StringType(), True),
   StructField("ProfileName", StringType(), True),
   StructField("GammaCurve", StringType(), True),
   StructField("ScreenLpi", StringType(), True),
   StructField("Angle", StringType(), True),
   StructField("StylusDepth", StringType(), True),
   StructField("ShadowCell", StringType(), True),
   StructField("Channel", StringType(), True),
   StructField("CellWall", StringType(), True),
   StructField("MidToneCell", StringType(), True),
   StructField("TestCutVolt", StringType(), True),
   StructField("HighlightCell", StringType(), True),
   StructField("CellShape", StringType(), True),
   StructField("Edge", StringType(), True),
   StructField("FlexoPerfection", StringType(), True),
   StructField("PostElimination", StringType(), True),
   StructField("SchepersCell", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinder_engraving"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_job_tech_spec_colour_cylinder_engraving")

    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinder_engraving_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.TechSpecColourCylinderEngravingId = source.TechSpecColourCylinderEngravingId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderEngravingId": col("source.TechSpecColourCylinderEngravingId"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "PrintScenario": col("source.PrintScenario"),
        "ProfileName": col("source.ProfileName"),
		"GammaCurve": col("source.GammaCurve"),
        "ScreenLpi": col("source.ScreenLpi"),
        "Angle": col("source.Angle"),
        "StylusDepth": col("source.StylusDepth"),
        "ShadowCell": col("source.ShadowCell"),
        "Channel": col("source.Channel"),
        "CellWall": col("source.CellWall"),
        "MidToneCell": col("source.MidToneCell"),
        "TestCutVolt": col("source.TestCutVolt"),
        "HighlightCell": col("source.HighlightCell"),
        "CellShape": col("source.CellShape"),
        "Edge": col("source.Edge"),
        "FlexoPerfection": col("source.FlexoPerfection"),
        "PostElimination": col("source.PostElimination"),
        "SchepersCell": col("source.SchepersCell")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderEngravingId": col("source.TechSpecColourCylinderEngravingId"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "PrintScenario": col("source.PrintScenario"),
        "ProfileName": col("source.ProfileName"),
		"GammaCurve": col("source.GammaCurve"),
        "ScreenLpi": col("source.ScreenLpi"),
        "Angle": col("source.Angle"),
        "StylusDepth": col("source.StylusDepth"),
        "ShadowCell": col("source.ShadowCell"),
        "Channel": col("source.Channel"),
        "CellWall": col("source.CellWall"),
        "MidToneCell": col("source.MidToneCell"),
        "TestCutVolt": col("source.TestCutVolt"),
        "HighlightCell": col("source.HighlightCell"),
        "CellShape": col("source.CellShape"),
        "Edge": col("source.Edge"),
        "FlexoPerfection": col("source.FlexoPerfection"),
        "PostElimination": col("source.PostElimination"),
        "SchepersCell": col("source.SchepersCell")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_job_tech_spec_colour_cylinder_engraving")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colour_cylinder_engraving_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_TechSpecColourCylinderEngravingId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_TechSpecColourCylinderEngravingId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.TechSpecColourCylinderEngravingId = S.CT_TechSpecColourCylinderEngravingId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("TechSpecColourCylinderEngravingId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.TechSpecColourCylinderEngravingId") == col("b.TechSpecColourCylinderEngravingId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.TechSpecColourCylinderEngravingId"),
        col("a.TechSpecColourCylinderId"),
        col("a.PrintScenario"),
        col("a.ProfileName"),
		col("a.GammaCurve"),
        col("a.ScreenLpi"),
        col("a.Angle"),
        col("a.StylusDepth"),
        col("a.ShadowCell"),
        col("a.Channel"),
        col("a.CellWall"),
        col("a.MidToneCell"),
        col("a.TestCutVolt"),
        col("a.HighlightCell"),
        col("a.CellShape"),
        col("a.Edge"),
        col("a.FlexoPerfection"),
        col("a.PostElimination"),
        col("a.SchepersCell")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.TechSpecColourCylinderEngravingId = source.TechSpecColourCylinderEngravingId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderEngravingId": col("source.TechSpecColourCylinderEngravingId"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "PrintScenario": col("source.PrintScenario"),
        "ProfileName": col("source.ProfileName"),
		"GammaCurve": col("source.GammaCurve"),
        "ScreenLpi": col("source.ScreenLpi"),
        "Angle": col("source.Angle"),
        "StylusDepth": col("source.StylusDepth"),
        "ShadowCell": col("source.ShadowCell"),
        "Channel": col("source.Channel"),
        "CellWall": col("source.CellWall"),
        "MidToneCell": col("source.MidToneCell"),
        "TestCutVolt": col("source.TestCutVolt"),
        "HighlightCell": col("source.HighlightCell"),
        "CellShape": col("source.CellShape"),
        "Edge": col("source.Edge"),
        "FlexoPerfection": col("source.FlexoPerfection"),
        "PostElimination": col("source.PostElimination"),
        "SchepersCell": col("source.SchepersCell")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "TechSpecColourCylinderEngravingId": col("source.TechSpecColourCylinderEngravingId"),
        "TechSpecColourCylinderId": col("source.TechSpecColourCylinderId"),
        "PrintScenario": col("source.PrintScenario"),
        "ProfileName": col("source.ProfileName"),
		"GammaCurve": col("source.GammaCurve"),
        "ScreenLpi": col("source.ScreenLpi"),
        "Angle": col("source.Angle"),
        "StylusDepth": col("source.StylusDepth"),
        "ShadowCell": col("source.ShadowCell"),
        "Channel": col("source.Channel"),
        "CellWall": col("source.CellWall"),
        "MidToneCell": col("source.MidToneCell"),
        "TestCutVolt": col("source.TestCutVolt"),
        "HighlightCell": col("source.HighlightCell"),
        "CellShape": col("source.CellShape"),
        "Edge": col("source.Edge"),
        "FlexoPerfection": col("source.FlexoPerfection"),
        "PostElimination": col("source.PostElimination"),
        "SchepersCell": col("source.SchepersCell")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

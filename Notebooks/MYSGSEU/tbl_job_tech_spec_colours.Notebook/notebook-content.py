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
# META         },
# META         {
# META           "id": "59693f16-ceb1-40c6-b096-d37b5fbbbd26"
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
			StructField("CT_JobTechSpecColourId", StringType(), True),
			StructField("JobTechSpecColourId", StringType(), True),
			StructField("JobTechSpecId", StringType(), True),
			StructField("ClientPlateColourRef", StringType(), True),
			StructField("MCGColourId", StringType(), True),
			StructField("ColourType", StringType(), True),
			StructField("DotShape", StringType(), True),
			StructField("ScreenRuling", StringType(), True),
			StructField("ScreenAngle", StringType(), True),
			StructField("DGCCurveId", StringType(), True),
			StructField("ICPCurveId", StringType(), True),
			StructField("DGCCurveId_Proof", StringType(), True),
			StructField("ICPCurveId_Proof", StringType(), True),
			StructField("ColourOrder", StringType(), True),
			StructField("NewColour", StringType(), True),
			StructField("CommonColourRef", StringType(), True),
			StructField("TapeSpec", StringType(), True),
			StructField("ColourNote", StringType(), True),
			StructField("CustImageIdNo", StringType(), True),
			StructField("CustCarrierIdNo", StringType(), True),
			StructField("PrimaryColour", StringType(), True),
			StructField("EyemarkColour", StringType(), True),

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
			StructField("JobTechSpecColourId", StringType(), True),
			StructField("JobTechSpecId", StringType(), True),
			StructField("ClientPlateColourRef", StringType(), True),
			StructField("MCGColourId", StringType(), True),
			StructField("ColourType", StringType(), True),
			StructField("DotShape", StringType(), True),
			StructField("ScreenRuling", StringType(), True),
			StructField("ScreenAngle", StringType(), True),
			StructField("DGCCurveId", StringType(), True),
			StructField("ICPCurveId", StringType(), True),
			StructField("DGCCurveId_Proof", StringType(), True),
			StructField("ICPCurveId_Proof", StringType(), True),
			StructField("ColourOrder", StringType(), True),
			StructField("NewColour", StringType(), True),
			StructField("CommonColourRef", StringType(), True),
			StructField("TapeSpec", StringType(), True),
			StructField("ColourNote", StringType(), True),
			StructField("CustImageIdNo", StringType(), True),
			StructField("CustCarrierIdNo", StringType(), True),
			StructField("PrimaryColour", StringType(), True),
			StructField("EyemarkColour", StringType(), True),
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_job_tech_spec_colours"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_job_tech_spec_colours")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colours_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
     
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.JobTechSpecColourId = source.JobTechSpecColourId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechSpecColourId": col("source.JobTechSpecColourId"),
        "JobTechSpecId": col("source.JobTechSpecId"),
        "ClientPlateColourRef": col("source.ClientPlateColourRef"),
        "MCGColourId": col("source.MCGColourId"),
        "ColourType": col("source.ColourType"),
        "DotShape": col("source.DotShape"),
        "ScreenRuling": col("source.ScreenRuling"),
        "ScreenAngle": col("source.ScreenAngle"),
        "DGCCurveId": col("source.DGCCurveId"),
        "ICPCurveId": col("source.ICPCurveId"),
        "DGCCurveId_Proof": col("source.DGCCurveId_Proof"),
        "ICPCurveId_Proof": col("source.ICPCurveId_Proof"),
        "ColourOrder": col("source.ColourOrder"),
        "NewColour": col("source.NewColour"),
        "CommonColourRef": col("source.CommonColourRef"),
        "TapeSpec": col("source.TapeSpec"),
        "ColourNote": col("source.ColourNote"),
        "CustImageIdNo": col("source.CustImageIdNo"),
        "CustCarrierIdNo": col("source.CustCarrierIdNo"),
        "PrimaryColour": col("source.PrimaryColour"),
        "EyemarkColour": col("source.EyemarkColour")
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechSpecColourId": col("source.JobTechSpecColourId"),
        "JobTechSpecId": col("source.JobTechSpecId"),
        "ClientPlateColourRef": col("source.ClientPlateColourRef"),
        "MCGColourId": col("source.MCGColourId"),
        "ColourType": col("source.ColourType"),
        "DotShape": col("source.DotShape"),
        "ScreenRuling": col("source.ScreenRuling"),
        "ScreenAngle": col("source.ScreenAngle"),
        "DGCCurveId": col("source.DGCCurveId"),
        "ICPCurveId": col("source.ICPCurveId"),
        "DGCCurveId_Proof": col("source.DGCCurveId_Proof"),
        "ICPCurveId_Proof": col("source.ICPCurveId_Proof"),
        "ColourOrder": col("source.ColourOrder"),
        "NewColour": col("source.NewColour"),
        "CommonColourRef": col("source.CommonColourRef"),
        "TapeSpec": col("source.TapeSpec"),
        "ColourNote": col("source.ColourNote"),
        "CustImageIdNo": col("source.CustImageIdNo"),
        "CustCarrierIdNo": col("source.CustCarrierIdNo"),
        "PrimaryColour": col("source.PrimaryColour"),
        "EyemarkColour": col("source.EyemarkColour")
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_job_tech_spec_colours")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_job_tech_spec_colours_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_JobTechSpecColourId
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_JobTechSpecColourId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.JobTechSpecColourId = S.CT_JobTechSpecColourId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("JobTechSpecColourId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.JobTechSpecColourId") == col("b.JobTechSpecColourId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.JobTechSpecColourId"),
		col("a.JobTechSpecId"),
		col("a.ClientPlateColourRef"),
		col("a.MCGColourId"),
		col("a.ColourType"),
		col("a.DotShape"),
		col("a.ScreenRuling"),
		col("a.ScreenAngle"),
		col("a.DGCCurveId"),
		col("a.ICPCurveId"),
		col("a.DGCCurveId_Proof"),
		col("a.ICPCurveId_Proof"),
		col("a.ColourOrder"),
		col("a.NewColour"),
		col("a.CommonColourRef"),
		col("a.TapeSpec"),
		col("a.ColourNote"),
		col("a.CustImageIdNo"),
		col("a.CustCarrierIdNo"),
		col("a.PrimaryColour"),
		col("a.EyemarkColour")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.JobTechSpecColourId = source.JobTechSpecColourId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechSpecColourId": col("source.JobTechSpecColourId"),
        "JobTechSpecId": col("source.JobTechSpecId"),
        "ClientPlateColourRef": col("source.ClientPlateColourRef"),
        "MCGColourId": col("source.MCGColourId"),
        "ColourType": col("source.ColourType"),
        "DotShape": col("source.DotShape"),
        "ScreenRuling": col("source.ScreenRuling"),
        "ScreenAngle": col("source.ScreenAngle"),
        "DGCCurveId": col("source.DGCCurveId"),
        "ICPCurveId": col("source.ICPCurveId"),
        "DGCCurveId_Proof": col("source.DGCCurveId_Proof"),
        "ICPCurveId_Proof": col("source.ICPCurveId_Proof"),
        "ColourOrder": col("source.ColourOrder"),
        "NewColour": col("source.NewColour"),
        "CommonColourRef": col("source.CommonColourRef"),
        "TapeSpec": col("source.TapeSpec"),
        "ColourNote": col("source.ColourNote"),
        "CustImageIdNo": col("source.CustImageIdNo"),
        "CustCarrierIdNo": col("source.CustCarrierIdNo"),
        "PrimaryColour": col("source.PrimaryColour"),
        "EyemarkColour": col("source.EyemarkColour")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "JobTechSpecColourId": col("source.JobTechSpecColourId"),
        "JobTechSpecId": col("source.JobTechSpecId"),
        "ClientPlateColourRef": col("source.ClientPlateColourRef"),
        "MCGColourId": col("source.MCGColourId"),
        "ColourType": col("source.ColourType"),
        "DotShape": col("source.DotShape"),
        "ScreenRuling": col("source.ScreenRuling"),
        "ScreenAngle": col("source.ScreenAngle"),
        "DGCCurveId": col("source.DGCCurveId"),
        "ICPCurveId": col("source.ICPCurveId"),
        "DGCCurveId_Proof": col("source.DGCCurveId_Proof"),
        "ICPCurveId_Proof": col("source.ICPCurveId_Proof"),
        "ColourOrder": col("source.ColourOrder"),
        "NewColour": col("source.NewColour"),
        "CommonColourRef": col("source.CommonColourRef"),
        "TapeSpec": col("source.TapeSpec"),
        "ColourNote": col("source.ColourNote"),
        "CustImageIdNo": col("source.CustImageIdNo"),
        "CustCarrierIdNo": col("source.CustCarrierIdNo"),
        "PrimaryColour": col("source.PrimaryColour"),
        "EyemarkColour": col("source.EyemarkColour")
    }).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_job_tech_spec_colours_FULL")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*)  FROM SILVER.dbo.tbl_job_tech_spec_colours")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_job_tech_spec_colours_DELTA")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

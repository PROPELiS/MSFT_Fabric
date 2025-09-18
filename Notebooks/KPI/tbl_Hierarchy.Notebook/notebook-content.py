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

in_mode ="FULL"

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
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("EmployeeJobCode", StringType(), True),
    StructField("EmployeeJobTitle", StringType(), True),
    StructField("LocationCode", StringType(), True),
    StructField("LocationName", StringType(), True),
    StructField("SUP_Level1_Name", StringType(), True),
    StructField("SUP_Level1_EmployeeNumber", StringType(), True),
    StructField("SUP_Level1_EmailAddress", StringType(), True),
    StructField("SUP_Level1_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level1_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level1_LocationCode", StringType(), True),
    StructField("SUP_Level1_LocationName", StringType(), True),
    StructField("SUP_Level2_Name", StringType(), True),
    StructField("SUP_Level2_EmployeeNumber", StringType(), True),
    StructField("SUP_Level2_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level2_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level2_emailaddress", StringType(), True),
    StructField("SUP_Level3_Name", StringType(), True),
    StructField("SUP_Level3_EmployeeNumber", StringType(), True),
    StructField("SUP_Level3_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level3_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level3_emailaddress", StringType(), True),
    StructField("SUP_Level4_Name", StringType(), True),
    StructField("SUP_Level4_EmployeeNumber", StringType(), True),
    StructField("SUP_Level4_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level4_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level4_emailaddress", StringType(), True),
    StructField("SUP_Level5_Name", StringType(), True),
    StructField("SUP_Level5_EmployeeNumber", StringType(), True),
    StructField("SUP_Level5_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level5_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level5_emailaddress", StringType(), True),
    StructField("SUP_Level6_Name", StringType(), True),
    StructField("SUP_Level6_EmployeeNumber", StringType(), True),
    StructField("SUP_Level6_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level6_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level6_emailaddress", StringType(), True),
    StructField("SUP_Level7_Name", StringType(), True),
    StructField("SUP_Level7_EmployeeNumber", StringType(), True),
    StructField("SUP_Level7_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level7_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level7_emailaddress", StringType(), True),
    StructField("SUP_Level8_Name", StringType(), True),
    StructField("SUP_Level8_EmployeeNumber", StringType(), True),
    StructField("SUP_Level8_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level8_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level8_emailaddress", StringType(), True),
    StructField("rep_login", StringType(), True),
    StructField("SUP_Level1_login", StringType(), True),
    StructField("SUP_Level2_login", StringType(), True),
    StructField("SUP_Level3_login", StringType(), True),
    StructField("SUP_Level4_login", StringType(), True),
    StructField("SUP_Level5_login", StringType(), True),
    StructField("SUP_Level6_login", StringType(), True),
    StructField("SUP_Level7_login", StringType(), True),
    StructField("SUP_Level8_login", StringType(), True)
])


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("EmployeeJobCode", StringType(), True),
    StructField("EmployeeJobTitle", StringType(), True),
    StructField("LocationCode", StringType(), True),
    StructField("LocationName", StringType(), True),
    StructField("SUP_Level1_Name", StringType(), True),
    StructField("SUP_Level1_EmployeeNumber", StringType(), True),
    StructField("SUP_Level1_EmailAddress", StringType(), True),
    StructField("SUP_Level1_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level1_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level1_LocationCode", StringType(), True),
    StructField("SUP_Level1_LocationName", StringType(), True),
    StructField("SUP_Level2_Name", StringType(), True),
    StructField("SUP_Level2_EmployeeNumber", StringType(), True),
    StructField("SUP_Level2_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level2_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level2_emailaddress", StringType(), True),
    StructField("SUP_Level3_Name", StringType(), True),
    StructField("SUP_Level3_EmployeeNumber", StringType(), True),
    StructField("SUP_Level3_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level3_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level3_emailaddress", StringType(), True),
    StructField("SUP_Level4_Name", StringType(), True),
    StructField("SUP_Level4_EmployeeNumber", StringType(), True),
    StructField("SUP_Level4_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level4_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level4_emailaddress", StringType(), True),
    StructField("SUP_Level5_Name", StringType(), True),
    StructField("SUP_Level5_EmployeeNumber", StringType(), True),
    StructField("SUP_Level5_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level5_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level5_emailaddress", StringType(), True),
    StructField("SUP_Level6_Name", StringType(), True),
    StructField("SUP_Level6_EmployeeNumber", StringType(), True),
    StructField("SUP_Level6_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level6_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level6_emailaddress", StringType(), True),
    StructField("SUP_Level7_Name", StringType(), True),
    StructField("SUP_Level7_EmployeeNumber", StringType(), True),
    StructField("SUP_Level7_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level7_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level7_emailaddress", StringType(), True),
    StructField("SUP_Level8_Name", StringType(), True),
    StructField("SUP_Level8_EmployeeNumber", StringType(), True),
    StructField("SUP_Level8_EmployeeJobCode", StringType(), True),
    StructField("SUP_Level8_EmployeeJodeTitle", StringType(), True),
    StructField("SUP_Level8_emailaddress", StringType(), True),
    StructField("rep_login", StringType(), True),
    StructField("SUP_Level1_login", StringType(), True),
    StructField("SUP_Level2_login", StringType(), True),
    StructField("SUP_Level3_login", StringType(), True),
    StructField("SUP_Level4_login", StringType(), True),
    StructField("SUP_Level5_login", StringType(), True),
    StructField("SUP_Level6_login", StringType(), True),
    StructField("SUP_Level7_login", StringType(), True),
    StructField("SUP_Level8_login", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
df.write.format("delta").mode("overwrite").saveAsTable("KPI.tbl_Hierarchy")
silver_path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/KPI/tbl_hierarchy"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    # df.write.format("delta").mode("overwrite").saveAsTable("KPI.tbl_Hierarchy")
    bronze_Path="abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/KPI/Hierarchy"

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

    # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.EmployeeNumber = source.EmployeeNumber"
       ).whenMatchedUpdate(set={
           "EmployeeNumber": col("source.EmployeeNumber"),
           "EmployeeName": col("source.EmployeeName"),
           "EmailAddress": col("source.EmailAddress"),
           "EmployeeJobCode": col("source.EmployeeJobCode"),
           "EmployeeJobTitle": col("source.EmployeeJobTitle"),
           "LocationCode": col("source.LocationCode"),
           "LocationName": col("source.LocationName"),       
           "SUP_Level1_Name": col("source.SUP_Level1_Name"),
           "SUP_Level1_EmployeeNumber": col("source.SUP_Level1_EmployeeNumber"),
           "SUP_Level1_EmailAddress": col("source.SUP_Level1_EmailAddress"),
           "SUP_Level1_EmployeeJobCode": col("source.SUP_Level1_EmployeeJobCode"),
           "SUP_Level1_EmployeeJodeTitle": col("source.SUP_Level1_EmployeeJodeTitle"),
           "SUP_Level1_LocationCode": col("source.SUP_Level1_LocationCode"),
           "SUP_Level1_LocationName": col("source.SUP_Level1_LocationName"),       
           "SUP_Level2_Name": col("source.SUP_Level2_Name"),
           "SUP_Level2_EmployeeNumber": col("source.SUP_Level2_EmployeeNumber"),
           "SUP_Level2_EmployeeJobCode": col("source.SUP_Level2_EmployeeJobCode"),
           "SUP_Level2_EmployeeJodeTitle": col("source.SUP_Level2_EmployeeJodeTitle"),
           "SUP_Level2_emailaddress": col("source.SUP_Level2_emailaddress"),      
           "SUP_Level3_Name": col("source.SUP_Level3_Name"),
           "SUP_Level3_EmployeeNumber": col("source.SUP_Level3_EmployeeNumber"),
           "SUP_Level3_EmployeeJobCode": col("source.SUP_Level3_EmployeeJobCode"),
           "SUP_Level3_EmployeeJodeTitle": col("source.SUP_Level3_EmployeeJodeTitle"),
           "SUP_Level3_emailaddress": col("source.SUP_Level3_emailaddress"),     
           "SUP_Level4_Name": col("source.SUP_Level4_Name"),
           "SUP_Level4_EmployeeNumber": col("source.SUP_Level4_EmployeeNumber"),
           "SUP_Level4_EmployeeJobCode": col("source.SUP_Level4_EmployeeJobCode"),
           "SUP_Level4_EmployeeJodeTitle": col("source.SUP_Level4_EmployeeJodeTitle"),
           "SUP_Level4_emailaddress": col("source.SUP_Level4_emailaddress"),      
           "SUP_Level5_Name": col("source.SUP_Level5_Name"),
           "SUP_Level5_EmployeeNumber": col("source.SUP_Level5_EmployeeNumber"),
           "SUP_Level5_EmployeeJobCode": col("source.SUP_Level5_EmployeeJobCode"),
           "SUP_Level5_EmployeeJodeTitle": col("source.SUP_Level5_EmployeeJodeTitle"),
           "SUP_Level5_emailaddress": col("source.SUP_Level5_emailaddress"),
           "SUP_Level6_Name": col("source.SUP_Level6_Name"),
           "SUP_Level6_EmployeeNumber": col("source.SUP_Level6_EmployeeNumber"),
           "SUP_Level6_EmployeeJobCode": col("source.SUP_Level6_EmployeeJobCode"),
           "SUP_Level6_EmployeeJodeTitle": col("source.SUP_Level6_EmployeeJodeTitle"),
           "SUP_Level6_emailaddress": col("source.SUP_Level6_emailaddress"),
           "SUP_Level7_Name": col("source.SUP_Level7_Name"),
           "SUP_Level7_EmployeeNumber": col("source.SUP_Level7_EmployeeNumber"),
           "SUP_Level7_EmployeeJobCode": col("source.SUP_Level7_EmployeeJobCode"),
           "SUP_Level7_EmployeeJodeTitle": col("source.SUP_Level7_EmployeeJodeTitle"),
           "SUP_Level7_emailaddress": col("source.SUP_Level7_emailaddress"),
           "SUP_Level8_Name": col("source.SUP_Level8_Name"),
           "SUP_Level8_EmployeeNumber": col("source.SUP_Level8_EmployeeNumber"),
           "SUP_Level8_EmployeeJobCode": col("source.SUP_Level8_EmployeeJobCode"),
           "SUP_Level8_EmployeeJodeTitle": col("source.SUP_Level8_EmployeeJodeTitle"),
           "SUP_Level8_emailaddress": col("source.SUP_Level8_emailaddress"),
           "rep_login": col("source.rep_login"),
           "SUP_Level1_login": col("source.SUP_Level1_login"),
           "SUP_Level2_login": col("source.SUP_Level2_login"),
           "SUP_Level3_login": col("source.SUP_Level3_login"),
           "SUP_Level4_login": col("source.SUP_Level4_login"),
           "SUP_Level5_login": col("source.SUP_Level5_login"),
           "SUP_Level6_login": col("source.SUP_Level6_login"),
           "SUP_Level7_login": col("source.SUP_Level7_login"),
           "SUP_Level8_login": col("source.SUP_Level8_login")
       }).whenNotMatchedInsert(values={
           "EmployeeNumber": col("source.EmployeeNumber"),
           "EmployeeName": col("source.EmployeeName"),
           "EmailAddress": col("source.EmailAddress"),
           "EmployeeJobCode": col("source.EmployeeJobCode"),
           "EmployeeJobTitle": col("source.EmployeeJobTitle"),
           "LocationCode": col("source.LocationCode"),
           "LocationName": col("source.LocationName"),
           "SUP_Level1_Name": col("source.SUP_Level1_Name"),
           "SUP_Level1_EmployeeNumber": col("source.SUP_Level1_EmployeeNumber"),
           "SUP_Level1_EmailAddress": col("source.SUP_Level1_EmailAddress"),
           "SUP_Level1_EmployeeJobCode": col("source.SUP_Level1_EmployeeJobCode"),
           "SUP_Level1_EmployeeJodeTitle": col("source.SUP_Level1_EmployeeJodeTitle"),
           "SUP_Level1_LocationCode": col("source.SUP_Level1_LocationCode"),
           "SUP_Level1_LocationName": col("source.SUP_Level1_LocationName"),
           "SUP_Level2_Name": col("source.SUP_Level2_Name"),
           "SUP_Level2_EmployeeNumber": col("source.SUP_Level2_EmployeeNumber"),
           "SUP_Level2_EmployeeJobCode": col("source.SUP_Level2_EmployeeJobCode"),
           "SUP_Level2_EmployeeJodeTitle": col("source.SUP_Level2_EmployeeJodeTitle"),
           "SUP_Level2_emailaddress": col("source.SUP_Level2_emailaddress"),
           "SUP_Level3_Name": col("source.SUP_Level3_Name"),
           "SUP_Level3_EmployeeNumber": col("source.SUP_Level3_EmployeeNumber"),
           "SUP_Level3_EmployeeJobCode": col("source.SUP_Level3_EmployeeJobCode"),
           "SUP_Level3_EmployeeJodeTitle": col("source.SUP_Level3_EmployeeJodeTitle"),
           "SUP_Level3_emailaddress": col("source.SUP_Level3_emailaddress"),
           "SUP_Level4_Name": col("source.SUP_Level4_Name"),
           "SUP_Level4_EmployeeNumber": col("source.SUP_Level4_EmployeeNumber"),
           "SUP_Level4_EmployeeJobCode": col("source.SUP_Level4_EmployeeJobCode"),
           "SUP_Level4_EmployeeJodeTitle": col("source.SUP_Level4_EmployeeJodeTitle"),
           "SUP_Level4_emailaddress": col("source.SUP_Level4_emailaddress"),
           "SUP_Level5_Name": col("source.SUP_Level5_Name"),
           "SUP_Level5_EmployeeNumber": col("source.SUP_Level5_EmployeeNumber"),
           "SUP_Level5_EmployeeJobCode": col("source.SUP_Level5_EmployeeJobCode"),
           "SUP_Level5_EmployeeJodeTitle": col("source.SUP_Level5_EmployeeJodeTitle"),
           "SUP_Level5_emailaddress": col("source.SUP_Level5_emailaddress"),
           "SUP_Level6_Name": col("source.SUP_Level6_Name"),
           "SUP_Level6_EmployeeNumber": col("source.SUP_Level6_EmployeeNumber"),
           "SUP_Level6_EmployeeJobCode": col("source.SUP_Level6_EmployeeJobCode"),
           "SUP_Level6_EmployeeJodeTitle": col("source.SUP_Level6_EmployeeJodeTitle"),
           "SUP_Level6_emailaddress": col("source.SUP_Level6_emailaddress"),
           "SUP_Level7_Name": col("source.SUP_Level7_Name"),
           "SUP_Level7_EmployeeNumber": col("source.SUP_Level7_EmployeeNumber"),
           "SUP_Level7_EmployeeJobCode": col("source.SUP_Level7_EmployeeJobCode"),
           "SUP_Level7_EmployeeJodeTitle": col("source.SUP_Level7_EmployeeJodeTitle"),
           "SUP_Level7_emailaddress": col("source.SUP_Level7_emailaddress"),
           "SUP_Level8_Name": col("source.SUP_Level8_Name"),
           "SUP_Level8_EmployeeNumber": col("source.SUP_Level8_EmployeeNumber"),
           "SUP_Level8_EmployeeJobCode": col("source.SUP_Level8_EmployeeJobCode"),
           "SUP_Level8_EmployeeJodeTitle": col("source.SUP_Level8_EmployeeJodeTitle"),
           "SUP_Level8_emailaddress": col("source.SUP_Level8_emailaddress"),
           "rep_login": col("source.rep_login"),
           "SUP_Level1_login": col("source.SUP_Level1_login"),
           "SUP_Level2_login": col("source.SUP_Level2_login"),
           "SUP_Level3_login": col("source.SUP_Level3_login"),
           "SUP_Level4_login": col("source.SUP_Level4_login"),
           "SUP_Level5_login": col("source.SUP_Level5_login"),
           "SUP_Level6_login": col("source.SUP_Level6_login"),
           "SUP_Level7_login": col("source.SUP_Level7_login"),
           "SUP_Level8_login": col("source.SUP_Level8_login")
       }).execute()
    
    
else:
    df.write.format("delta").mode("append").saveAsTable("KPI.tbl_Hierarchy")
   # Define paths
    source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/KPI/Hierarchy"
    source_df_delta = spark.read.format("delta").load(source_path)
    
# Perform the MERGE operation
    silver_table.alias("target").merge(
        source_df_delta.alias("source"),
        "target.EmployeeNumber = source.EmployeeNumber"
    ).whenMatchedUpdate(set={
        "EmployeeNumber": col("source.EmployeeNumber"),
        "EmployeeName": col("source.EmployeeName"),
        "EmailAddress": col("source.EmailAddress"),
        "EmployeeJobCode": col("source.EmployeeJobCode"),
        "EmployeeJobTitle": col("source.EmployeeJobTitle"),
        "LocationCode": col("source.LocationCode"),
        "LocationName": col("source.LocationName"),
        "SUP_Level1_Name": col("source.SUP_Level1_Name"),
        "SUP_Level1_EmployeeNumber": col("source.SUP_Level1_EmployeeNumber"),
        "SUP_Level1_EmailAddress": col("source.SUP_Level1_EmailAddress"),
        "SUP_Level1_EmployeeJobCode": col("source.SUP_Level1_EmployeeJobCode"),
        "SUP_Level1_EmployeeJodeTitle": col("source.SUP_Level1_EmployeeJodeTitle"),
        "SUP_Level1_LocationCode": col("source.SUP_Level1_LocationCode"),
        "SUP_Level1_LocationName": col("source.SUP_Level1_LocationName"),
        "SUP_Level2_Name": col("source.SUP_Level2_Name"),
        "SUP_Level2_EmployeeNumber": col("source.SUP_Level2_EmployeeNumber"),
        "SUP_Level2_EmployeeJobCode": col("source.SUP_Level2_EmployeeJobCode"),
        "SUP_Level2_EmployeeJodeTitle": col("source.SUP_Level2_EmployeeJodeTitle"),
        "SUP_Level2_emailaddress": col("source.SUP_Level2_emailaddress"),
        "SUP_Level3_Name": col("source.SUP_Level3_Name"),
        "SUP_Level3_EmployeeNumber": col("source.SUP_Level3_EmployeeNumber"),
        "SUP_Level3_EmployeeJobCode": col("source.SUP_Level3_EmployeeJobCode"),
        "SUP_Level3_EmployeeJodeTitle": col("source.SUP_Level3_EmployeeJodeTitle"),
        "SUP_Level3_emailaddress": col("source.SUP_Level3_emailaddress"),
        "SUP_Level4_Name": col("source.SUP_Level4_Name"),
        "SUP_Level4_EmployeeNumber": col("source.SUP_Level4_EmployeeNumber"),
        "SUP_Level4_EmployeeJobCode": col("source.SUP_Level4_EmployeeJobCode"),
        "SUP_Level4_EmployeeJodeTitle": col("source.SUP_Level4_EmployeeJodeTitle"),
        "SUP_Level4_emailaddress": col("source.SUP_Level4_emailaddress"),
        "SUP_Level5_Name": col("source.SUP_Level5_Name"),
        "SUP_Level5_EmployeeNumber": col("source.SUP_Level5_EmployeeNumber"),
        "SUP_Level5_EmployeeJobCode": col("source.SUP_Level5_EmployeeJobCode"),
        "SUP_Level5_EmployeeJodeTitle": col("source.SUP_Level5_EmployeeJodeTitle"),
        "SUP_Level5_emailaddress": col("source.SUP_Level5_emailaddress"),
        "SUP_Level6_Name": col("source.SUP_Level6_Name"),
        "SUP_Level6_EmployeeNumber": col("source.SUP_Level6_EmployeeNumber"),
        "SUP_Level6_EmployeeJobCode": col("source.SUP_Level6_EmployeeJobCode"),
        "SUP_Level6_EmployeeJodeTitle": col("source.SUP_Level6_EmployeeJodeTitle"),
        "SUP_Level6_emailaddress": col("source.SUP_Level6_emailaddress"),    
        "SUP_Level7_Name": col("source.SUP_Level7_Name"),
        "SUP_Level7_EmployeeNumber": col("source.SUP_Level7_EmployeeNumber"),
        "SUP_Level7_EmployeeJobCode": col("source.SUP_Level7_EmployeeJobCode"),
        "SUP_Level7_EmployeeJodeTitle": col("source.SUP_Level7_EmployeeJodeTitle"),
        "SUP_Level7_emailaddress": col("source.SUP_Level7_emailaddress"),
        "SUP_Level8_Name": col("source.SUP_Level8_Name"),
        "SUP_Level8_EmployeeNumber": col("source.SUP_Level8_EmployeeNumber"),
        "SUP_Level8_EmployeeJobCode": col("source.SUP_Level8_EmployeeJobCode"),
        "SUP_Level8_EmployeeJodeTitle": col("source.SUP_Level8_EmployeeJodeTitle"),
        "SUP_Level8_emailaddress": col("source.SUP_Level8_emailaddress"),   
        "rep_login": col("source.rep_login"),
        "SUP_Level1_login": col("source.SUP_Level1_login"),
        "SUP_Level2_login": col("source.SUP_Level2_login"),
        "SUP_Level3_login": col("source.SUP_Level3_login"),
        "SUP_Level4_login": col("source.SUP_Level4_login"),
        "SUP_Level5_login": col("source.SUP_Level5_login"),
        "SUP_Level6_login": col("source.SUP_Level6_login"),
        "SUP_Level7_login": col("source.SUP_Level7_login"),
        "SUP_Level8_login": col("source.SUP_Level8_login")
    }).whenNotMatchedInsert(values={
        "EmployeeNumber": col("source.EmployeeNumber"),
        "EmployeeName": col("source.EmployeeName"),
        "EmailAddress": col("source.EmailAddress"),
        "EmployeeJobCode": col("source.EmployeeJobCode"),
        "EmployeeJobTitle": col("source.EmployeeJobTitle"),
        "LocationCode": col("source.LocationCode"),
        "LocationName": col("source.LocationName"),
        "SUP_Level1_Name": col("source.SUP_Level1_Name"),
        "SUP_Level1_EmployeeNumber": col("source.SUP_Level1_EmployeeNumber"),
        "SUP_Level1_EmailAddress": col("source.SUP_Level1_EmailAddress"),
        "SUP_Level1_EmployeeJobCode": col("source.SUP_Level1_EmployeeJobCode"),
        "SUP_Level1_EmployeeJodeTitle": col("source.SUP_Level1_EmployeeJodeTitle"),
        "SUP_Level1_LocationCode": col("source.SUP_Level1_LocationCode"),
        "SUP_Level1_LocationName": col("source.SUP_Level1_LocationName"),
        "SUP_Level2_Name": col("source.SUP_Level2_Name"),
        "SUP_Level2_EmployeeNumber": col("source.SUP_Level2_EmployeeNumber"),
        "SUP_Level2_EmployeeJobCode": col("source.SUP_Level2_EmployeeJobCode"),
        "SUP_Level2_EmployeeJodeTitle": col("source.SUP_Level2_EmployeeJodeTitle"),
        "SUP_Level2_emailaddress": col("source.SUP_Level2_emailaddress"),
        "SUP_Level3_Name": col("source.SUP_Level3_Name"),
        "SUP_Level3_EmployeeNumber": col("source.SUP_Level3_EmployeeNumber"),
        "SUP_Level3_EmployeeJobCode": col("source.SUP_Level3_EmployeeJobCode"),
        "SUP_Level3_EmployeeJodeTitle": col("source.SUP_Level3_EmployeeJodeTitle"),
        "SUP_Level3_emailaddress": col("source.SUP_Level3_emailaddress"),
        "SUP_Level4_Name": col("source.SUP_Level4_Name"),
        "SUP_Level4_EmployeeNumber": col("source.SUP_Level4_EmployeeNumber"),
        "SUP_Level4_EmployeeJobCode": col("source.SUP_Level4_EmployeeJobCode"),
        "SUP_Level4_EmployeeJodeTitle": col("source.SUP_Level4_EmployeeJodeTitle"),
        "SUP_Level4_emailaddress": col("source.SUP_Level4_emailaddress"),
        "SUP_Level5_Name": col("source.SUP_Level5_Name"),
        "SUP_Level5_EmployeeNumber": col("source.SUP_Level5_EmployeeNumber"),
        "SUP_Level5_EmployeeJobCode": col("source.SUP_Level5_EmployeeJobCode"),
        "SUP_Level5_EmployeeJodeTitle": col("source.SUP_Level5_EmployeeJodeTitle"),
        "SUP_Level5_emailaddress": col("source.SUP_Level5_emailaddress"),
        "SUP_Level6_Name": col("source.SUP_Level6_Name"),
        "SUP_Level6_EmployeeNumber": col("source.SUP_Level6_EmployeeNumber"),
        "SUP_Level6_EmployeeJobCode": col("source.SUP_Level6_EmployeeJobCode"),
        "SUP_Level6_EmployeeJodeTitle": col("source.SUP_Level6_EmployeeJodeTitle"),
        "SUP_Level6_emailaddress": col("source.SUP_Level6_emailaddress"),
        "SUP_Level7_Name": col("source.SUP_Level7_Name"),
        "SUP_Level7_EmployeeNumber": col("source.SUP_Level7_EmployeeNumber"),
        "SUP_Level7_EmployeeJobCode": col("source.SUP_Level7_EmployeeJobCode"),
        "SUP_Level7_EmployeeJodeTitle": col("source.SUP_Level7_EmployeeJodeTitle"),
        "SUP_Level7_emailaddress": col("source.SUP_Level7_emailaddress"),
        "SUP_Level8_Name": col("source.SUP_Level8_Name"),
        "SUP_Level8_EmployeeNumber": col("source.SUP_Level8_EmployeeNumber"),
        "SUP_Level8_EmployeeJobCode": col("source.SUP_Level8_EmployeeJobCode"),
        "SUP_Level8_EmployeeJodeTitle": col("source.SUP_Level8_EmployeeJodeTitle"),
        "SUP_Level8_emailaddress": col("source.SUP_Level8_emailaddress"),
        "rep_login": col("source.rep_login"),
        "SUP_Level1_login": col("source.SUP_Level1_login"),
        "SUP_Level2_login": col("source.SUP_Level2_login"),
        "SUP_Level3_login": col("source.SUP_Level3_login"),
        "SUP_Level4_login": col("source.SUP_Level4_login"),
        "SUP_Level5_login": col("source.SUP_Level5_login"),
        "SUP_Level6_login": col("source.SUP_Level6_login"),
        "SUP_Level7_login": col("source.SUP_Level7_login"),
        "SUP_Level8_login": col("source.SUP_Level8_login")
    }).execute()
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

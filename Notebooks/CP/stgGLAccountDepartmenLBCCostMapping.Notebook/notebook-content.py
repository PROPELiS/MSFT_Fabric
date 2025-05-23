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

in_mode = "FULL"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
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

orderschema = StructType([
    StructField("GLAccountNumber", StringType(), True),
    StructField("DepartmentNumber", StringType(), True),
    StructField("FacilityLBC", StringType(), True),
    StructField("MppingType", StringType(), True),
    StructField("GLDepartmentKey", StringType(), True)
])




# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()



schema = StructType([
    StructField("GLAccountNumber", StringType(), True),
    StructField("DepartmentNumber", StringType(), True),
    StructField("FacilityLBC", StringType(), True),
    StructField("MppingType", StringType(), True),
    StructField("GLDepartmentKey", StringType(), True)
])
    # Define the schema

df = spark.createDataFrame([], schema)
df.write.format("delta").mode("overwrite").saveAsTable("CP.stgGLAccountDepartmenLBCCostMapping")

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/CP/stgglaccountdepartmenlbccostmapping"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("CP.stgGLAccountDepartmenLBCCostMapping")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/stgGLAccountDepartmenLBCCostMapping"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  


# Merge data from Bronze Full to Silver with only the specified columns
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.FacilityLBC = source.FacilityLBC"
    ).whenMatchedUpdate(set={
        "GLAccountNumber": col("source.GLAccountNumber"),
       "DepartmentNumber": col("source.DepartmentNumber"),
        "FacilityLBC": col("source.FacilityLBC"),
        "MppingType": col("source.MppingType"),
        "GLDepartmentKey": col("source.GLDepartmentKey")

    }).whenNotMatchedInsert(values={
        "GLAccountNumber": col("source.GLAccountNumber"),
       "DepartmentNumber": col("source.DepartmentNumber"),
        "FacilityLBC": col("source.FacilityLBC"),
        "MppingType": col("source.MppingType"),
        "GLDepartmentKey": col("source.GLDepartmentKey")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("CP.stgGLAccountDepartmenLBCCostMapping")
    
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/stgGLAccountDepartmenLBCCostMapping"
    source_df_delta = spark.read.format("delta").load(source_path)

   

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        source_df_delta.alias("source"),
        "target.FacilityLBC = source.FacilityLBC"
    ).whenMatchedUpdate(set={
        "GLAccountNumber": col("source.GLAccountNumber"),
       "DepartmentNumber": col("source.DepartmentNumber"),
        "FacilityLBC": col("source.FacilityLBC"),
        "MppingType": col("source.MppingType"),
        "GLDepartmentKey": col("source.GLDepartmentKey")

    }).whenNotMatchedInsert(values={
        "GLAccountNumber": col("source.GLAccountNumber"),
        "DepartmentNumber": col("source.DepartmentNumber"),
        "FacilityLBC": col("source.FacilityLBC"),
        "MppingType": col("source.MppingType"),
        "GLDepartmentKey": col("source.GLDepartmentKey")
    }).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(*) FROM SILVER.CP.stgglaccountdepartmenlbccostmapping ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

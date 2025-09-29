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

in_mode="FULL"

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
    StructField("CompanyCode", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("PlantCode", StringType(), True),
    StructField("PlantName", StringType(), True),
    StructField("DepartmentCode", StringType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("CurrentPayFrequency", StringType(), True),
    StructField("EmployeeStatusCode", StringType(), True),
    StructField("EmployeeStatus", StringType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("PeriodControlDate", StringType(), True),
    StructField("PayDate", StringType(), True),
    StructField("PeriodStartDate", StringType(), True),
    StructField("PeriodEndDate", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("EarningsCode", StringType(), True),
    StructField("Earnings", StringType(), True),
    StructField("CurrentHours", StringType(), True),
    StructField("CurrentAmount", StringType(), True)
   
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    # Define the schema
schema = StructType([
    StructField("CompanyCode", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("PlantCode", StringType(), True),
    StructField("PlantName", StringType(), True),
    StructField("DepartmentCode", StringType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("CurrentPayFrequency", StringType(), True),
    StructField("EmployeeStatusCode", StringType(), True),
    StructField("EmployeeStatus", StringType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("PeriodControlDate", StringType(), True),
    StructField("PayDate", StringType(), True),
    StructField("PeriodStartDate", StringType(), True),
    StructField("PeriodEndDate", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("EarningsCode", StringType(), True),
    StructField("Earnings", StringType(), True),
    StructField("CurrentHours", StringType(), True),
    StructField("CurrentAmount", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/ULTIPRO/tbl_earnings"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    bronze_Path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_Earnings"
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
    # Overwrite Silver with Bronze
    bronze_df.write.format("delta").mode("overwrite").save(silver_path)
    #df.write.format("delta").mode("overwrite").saveAsTable("Ultipro._Earnings")
else:
    df.write.format("delta").mode("append").saveAsTable("ULTIPRO.tbl_earnings")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_Earnings"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Perform the MERGE operation 
    silver_table.alias("target").merge(
        source_df_delta.alias("source"),
        condition=(
            (col("target.CurrentPayFrequency") == col("source.CurrentPayFrequency")) &
            (col("target.EmployeeStatusCode") == col("source.EmployeeStatusCode")) &
            (col("target.EmployeeNumber") == col("source.EmployeeNumber")) &
            (col("target.PeriodControlDate") == col("source.PeriodControlDate")) &
            (col("target.PayDate") == col("source.PayDate")) &
            (col("target.PeriodStartDate") == col("source.PeriodStartDate")) &
            (col("target.TransactionType") == col("source.TransactionType")) &
            (col("target.EarningsCode") == col("source.EarningsCode"))
        )
    ).whenMatchedUpdate(set={
        "CompanyCode": col("source.CompanyCode"),
        "CompanyName": col("source.CompanyName"),
        "PlantCode": col("source.PlantCode"),
        "PlantName": col("source.PlantName"),
        "DepartmentCode": col("source.DepartmentCode"),
        "DepartmentName": col("source.DepartmentName"),
        "CurrentPayFrequency": col("source.CurrentPayFrequency"),
        "EmployeeStatusCode": col("source.EmployeeStatusCode"),
        "EmployeeStatus": col("source.EmployeeStatus"),
        "EmployeeNumber": col("source.EmployeeNumber"),
        "PeriodControlDate": col("source.PeriodControlDate"),
        "PayDate": col("source.PayDate"),
        "PeriodStartDate": col("source.PeriodStartDate"),
        "PeriodEndDate": col("source.PeriodEndDate"),
        "TransactionType": col("source.TransactionType"),
        "EarningsCode": col("source.EarningsCode"),
        "Earnings": col("source.Earnings"),
        "CurrentHours": col("source.CurrentHours"),
        "CurrentAmount": col("source.CurrentAmount")
    }).whenNotMatchedInsert(values={
        "CompanyCode": col("source.CompanyCode"),
        "CompanyName": col("source.CompanyName"),
        "PlantCode": col("source.PlantCode"),
        "PlantName": col("source.PlantName"),
        "DepartmentCode": col("source.DepartmentCode"),
        "DepartmentName": col("source.DepartmentName"),
        "CurrentPayFrequency": col("source.CurrentPayFrequency"),
        "EmployeeStatusCode": col("source.EmployeeStatusCode"),
        "EmployeeStatus": col("source.EmployeeStatus"),
        "EmployeeNumber": col("source.EmployeeNumber"),
        "PeriodControlDate": col("source.PeriodControlDate"),
        "PayDate": col("source.PayDate"),
        "PeriodStartDate": col("source.PeriodStartDate"),
        "PeriodEndDate": col("source.PeriodEndDate"),
        "TransactionType": col("source.TransactionType"),
        "EarningsCode": col("source.EarningsCode"),
        "Earnings": col("source.Earnings"),
        "CurrentHours": col("source.CurrentHours"),
        "CurrentAmount": col("source.CurrentAmount")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

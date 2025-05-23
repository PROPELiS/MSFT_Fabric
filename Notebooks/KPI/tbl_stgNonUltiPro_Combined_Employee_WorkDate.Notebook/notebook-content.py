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
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

orderSchema = StructType([
    StructField("WorkDate", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("CountryCodeISO3", StringType(), True),
    StructField("LocalEmployeeCode", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("UltiProId", StringType(), True),
    StructField("IsSourceMissingUltiProId", StringType(), True),
    StructField("TotalDailyRegularHours", StringType(), True),
    StructField("TotalDailyOvertimeHours", StringType(), True)
])

# Initialize Spark
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("WorkDate", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("CountryCodeISO3", StringType(), True),
    StructField("LocalEmployeeCode", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("UltiProId", StringType(), True),
    StructField("IsSourceMissingUltiProId", StringType(), True),
    StructField("TotalDailyRegularHours", StringType(), True),
    StructField("TotalDailyOvertimeHours", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

param = ""

bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/KPI/stgNonUltiPro_Combined_Employee_WorkDate"
silver_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/KPI/stgnonultipro_combined_employee_workdate"
silver_table = DeltaTable.forPath(spark, silver_path)
# Read Bronze
bronze_df = spark.read.format("delta").load(bronze_path)

if in_mode == "FULL":
    # Transform IsSourceMissingUltiProId to match Snowflake logic
    bronze_df = bronze_df.withColumn(
    "IsSourceMissingUltiProId",
    expr("CASE WHEN IsSourceMissingUltiProId = 'True' THEN '1' WHEN IsSourceMissingUltiProId = 'False' THEN '0' END")
)
    # Overwrite the Silver table
    bronze_df.write.format("delta").mode("overwrite").save(silver_path)

else:
    df.write.format("delta").mode("append").saveAsTable("KPI.stgNonUltiPro_Combined_Employee_WorkDate")

silver_table.alias("target") \
    .merge(
        bronze_df.alias("source"),
        (col("target.WorkDate") == col("source.WorkDate")) &
        (col("target.SourceSystem") == col("source.SourceSystem")) &
        (col("target.LocalEmployeeCode") == col("source.LocalEmployeeCode"))
    ) \
    .whenMatchedUpdate(set={
        "WorkDate": col("source.WorkDate"),
        "SourceSystem": col("source.SourceSystem"),
        "CountryCodeISO3": col("source.CountryCodeISO3"),
        "LocalEmployeeCode": col("source.LocalEmployeeCode"),
        "EmployeeName": col("source.EmployeeName"),
        "UltiProId": col("source.UltiProId"),
        "IsSourceMissingUltiProId": col("source.IsSourceMissingUltiProId"),
        "TotalDailyRegularHours": col("source.TotalDailyRegularHours"),
        "TotalDailyOvertimeHours": col("source.TotalDailyOvertimeHours")
    }) \
    .whenNotMatchedInsert(values={
        "WorkDate": col("source.WorkDate"),
        "SourceSystem": col("source.SourceSystem"),
        "CountryCodeISO3": col("source.CountryCodeISO3"),
        "LocalEmployeeCode": col("source.LocalEmployeeCode"),
        "EmployeeName": col("source.EmployeeName"),
        "UltiProId": col("source.UltiProId"),
        "IsSourceMissingUltiProId": col("source.IsSourceMissingUltiProId"),
        "TotalDailyRegularHours": col("source.TotalDailyRegularHours"),
        "TotalDailyOvertimeHours": col("source.TotalDailyOvertimeHours")
    }) \
    .execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

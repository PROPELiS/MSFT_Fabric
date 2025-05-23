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

# Define the schema for the order data
orderSchema = StructType([
    StructField("EmployeeCode", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("WageEffectiveDate", StringType(), True),
    StructField("EarningType", StringType(), True),
    StructField("EarningAmount", StringType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("CountryCodeISO3", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("HourlyPayRate", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("EmployeeCode", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("WageEffectiveDate", StringType(), True),
    StructField("EarningType", StringType(), True),
    StructField("EarningAmount", StringType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("CountryCodeISO3", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("HourlyPayRate", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/KPI/stgnonultipro_combined_employee_wage"
silver_table = DeltaTable.forPath(spark, silver_path)
bronze_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/KPI/stgNonUltiPro_Combined_Employee_Wage"
# Parameters
param = ""  # Replace with the actual PARAM value

if in_mode == "FULL":
    # Load data from Bronze
    bronze_df = spark.read.format("delta").load(bronze_path)

    # Overwrite Silver table (truncate equivalent), including schema changes
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_path)

    print("Silver table replaced successfully.")
else:
    print("No action taken.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

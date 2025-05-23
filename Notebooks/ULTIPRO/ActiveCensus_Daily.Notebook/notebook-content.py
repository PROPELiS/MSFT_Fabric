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


orderSchema = StructType([
    StructField("FileDate", StringType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("CompanyCode", StringType(), True),
    StructField("PlantLBC", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("GLDepartment", StringType(), True),
    StructField("HRGrouping", StringType(), True),
    StructField("SupervisorName", StringType(), True),
    StructField("SupervisorJobCode", StringType(), True),
    StructField("EmployeeJobCode", StringType(), True),
    StructField("JobFamilyCode", StringType(), True),
    StructField("JobSubFamilyCode", StringType(), True),
    StructField("JobSubFamily", StringType(), True),
    StructField("SalaryGrade", StringType(), True),
    StructField("SalaryRangeCurrency", StringType(), True),
    StructField("AnnualMinimum", StringType(), True),
    StructField("AnnualMidpoint", StringType(), True),
    StructField("AnnualMaximum", StringType(), True),
    StructField("LocationCode", StringType(), True),
    StructField("SalaryDifferentialValue", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("WageType", StringType(), True),
    StructField("EmployeeStatus", StringType(), True),
    StructField("TerminationDate", StringType(), True),
    StructField("TerminationType", StringType(), True),
    StructField("ScheduledAnnualHours", StringType(), True),
    StructField("HourlyPayRate", StringType(), True),
    StructField("AnnualSalary", StringType(), True),
    StructField("TotalSalary", StringType(), True),
    StructField("SalaryCurrency", StringType(), True),
    StructField("EffectiveDate", StringType(), True),
    StructField("InternationalSystemID", StringType(), True),
    StructField("ETL_BatchId", StringType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

Schema = StructType([
    StructField("FileDate", StringType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("CompanyCode", StringType(), True),
    StructField("PlantLBC", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("GLDepartment", StringType(), True),
    StructField("HRGrouping", StringType(), True),
    StructField("SupervisorName", StringType(), True),
    StructField("SupervisorJobCode", StringType(), True),
    StructField("EmployeeJobCode", StringType(), True),
    StructField("JobFamilyCode", StringType(), True),
    StructField("JobSubFamilyCode", StringType(), True),
    StructField("JobSubFamily", StringType(), True),
    StructField("SalaryGrade", StringType(), True),
    StructField("SalaryRangeCurrency", StringType(), True),
    StructField("AnnualMinimum", StringType(), True),
    StructField("AnnualMidpoint", StringType(), True),
    StructField("AnnualMaximum", StringType(), True),
    StructField("LocationCode", StringType(), True),
    StructField("SalaryDifferentialValue", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("WageType", StringType(), True),
    StructField("EmployeeStatus", StringType(), True),
    StructField("TerminationDate", StringType(), True),
    StructField("TerminationType", StringType(), True),
    StructField("ScheduledAnnualHours", StringType(), True),
    StructField("HourlyPayRate", StringType(), True),
    StructField("AnnualSalary", StringType(), True),
    StructField("TotalSalary", StringType(), True),
    StructField("SalaryCurrency", StringType(), True),
    StructField("EffectiveDate", StringType(), True),
    StructField("InternationalSystemID", StringType(), True),
    StructField("ETL_BatchId", StringType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], Schema)



silver_path="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/Ultipro/activecensus_daily"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    #df.write.format("delta").mode("overwrite").saveAsTable("Ultipro.ActiveCensus_Daily")
    bronze_Path ="abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro_/ActiveCensus_Daily"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

    bronze_df.write.format("delta").mode("overwrite").save(silver_path)
    print("Silver table replaced successfully.")
else:
    print("No action taken.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

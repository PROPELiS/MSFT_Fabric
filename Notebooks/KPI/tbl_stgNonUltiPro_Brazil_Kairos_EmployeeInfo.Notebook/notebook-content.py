# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59693f16-ceb1-40c6-b096-d37b5fbbbd26",
# META       "default_lakehouse_name": "BRONZE",
# META       "default_lakehouse_workspace_id": "de3e35d4-28a5-4df0-a8d1-00feff73469d",
# META       "known_lakehouses": [
# META         {
# META           "id": "59693f16-ceb1-40c6-b096-d37b5fbbbd26"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define schema for stgNonUltipro_Mauritus_Publishing_Timesheet_Extract table
schema = StructType([
    StructField("Code", IntegerType(), True),  # INT, nullable
    StructField("employee_id", IntegerType(), True),  # INT, nullable
    StructField("work_leave", StringType(), True),  # VARCHAR(50), nullable
    StructField("is_public_holiday", BooleanType(), True),  # BIT, nullable
    StructField("is_sunday", BooleanType(), True),  # BIT, nullable
    StructField("work_date", StringType(), True),  # NVARCHAR(255), nullable
    StructField("time_in", StringType(), True),  # NVARCHAR(255), nullable
    StructField("time_out", StringType(), True),  # NVARCHAR(255), nullable
    StructField("lateness", DecimalType(5, 2), True),  # NUMERIC(5,2), nullable
    StructField("overtime", DecimalType(5, 2), True),  # NUMERIC(5,2), nullable
    StructField("lunch_hrs", DecimalType(5, 2), True),  # NUMERIC(5,2), nullable
    StructField("hours", DecimalType(5, 2), True),  # NUMERIC(5,2), nullable
    StructField("ETL_BatchId", IntegerType(), False)  # INT, not nullable, default 0
])

# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame([], schema)

# Save as a Delta table under KPI schema
empty_df.write.format("delta").mode("overwrite").save("Tables/KPI/stgNonUltipro_Mauritus_Publishing_Timesheet_Extract")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

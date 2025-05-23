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


from pyspark.sql.functions import concat_ws, expr, sha2, size, lit, col, array, struct, udf, current_timestamp, max as spark_max
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_PortfolioGroupId", StringType(), True),
    StructField("PortfolioGroupId", StringType(), True),
    StructField("SimplifiedGroupId", StringType(), True),
    StructField("PortfolioGroupName", StringType(), True),
    StructField("Deleted", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema with only the required columns
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("PortfolioGroupId", StringType(), True),
    StructField("SimplifiedGroupId", StringType(), True),
    StructField("PortfolioGroupName", StringType(), True),
    StructField("Deleted", StringType(), True)
])

# Create an empty DataFrame with the new schema
df = spark.createDataFrame([], schema)

silver_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_customer_portfolio_groups"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
# Parameters
param = ""  # Replace with the actual PARAM value

if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_customer_portfolio_groups")
    
    bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_portfolio_groups_FULL"
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.PortfolioGroupId = source.PortfolioGroupId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PortfolioGroupId": col("source.PortfolioGroupId"),
        "SimplifiedGroupId": col("source.SimplifiedGroupId"),
        "PortfolioGroupName": col("source.PortfolioGroupName"),
        "Deleted": col("source.Deleted")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PortfolioGroupId": col("source.PortfolioGroupId"),
        "SimplifiedGroupId": col("source.SimplifiedGroupId"),
        "PortfolioGroupName": col("source.PortfolioGroupName"),
        "Deleted": col("source.Deleted")
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("tbl_customer_portfolio_groups")
    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_portfolio_groups_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
        .select("CT_PortfolioGroupId").distinct()

    silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.PortfolioGroupId = S.CT_PortfolioGroupId") \
        .whenMatchedDelete() \
        .execute()

    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("PortfolioGroupId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.PortfolioGroupId") == col("b.PortfolioGroupId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.PortfolioGroupId"),
        col("a.SimplifiedGroupId"),
        col("a.PortfolioGroupName"),
        col("a.Deleted")
    ).distinct()

    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.PortfolioGroupId = source.PortfolioGroupId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PortfolioGroupId": col("source.PortfolioGroupId"),
        "SimplifiedGroupId": col("source.SimplifiedGroupId"),
        "PortfolioGroupName": col("source.PortfolioGroupName"),
        "Deleted": col("source.Deleted")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PortfolioGroupId": col("source.PortfolioGroupId"),
        "SimplifiedGroupId": col("source.SimplifiedGroupId"),
        "PortfolioGroupName": col("source.PortfolioGroupName"),
        "Deleted": col("source.Deleted")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_customer_portfolio_groups_FULL")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BRONZE.MYSGSEU.tbl_customer_simplified_groups_DELTA ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM SILVER.dbo.tbl_customer_portfolio_groups")
display(df)

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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema with only the required columns
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
   
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
   
    StructField("PortfolioGroupId", StringType(), True),
    StructField("SimplifiedGroupId", StringType(), True),
    StructField("PortfolioGroupName", StringType(), True),
    StructField("Deleted", StringType(), True)
])

# Create an empty DataFrame with the new schema
df = spark.createDataFrame([], schema)
df.write.format("delta").mode("append").saveAsTable("tbl_customer_portfolio_groups")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

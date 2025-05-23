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

# Define schema correctly
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_GroupId", StringType(), True),
    StructField("GroupId", StringType(), True),
    StructField("GroupName", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Correct schema for DataFrame creation
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("GroupId", StringType(), True),
    StructField("GroupName", StringType(), True)
])

# Create an empty DataFrame
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

silver_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/dbo/tbl_customer_groups"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value

if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("tbl_customer_groups")
    
    bronze_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_groups_FULL"
    bronze_df = spark.read.format("delta").load(bronze_path)

    # DELETE operation
    df_silver_distinct = silver_table.toDF().select("GroupId").distinct()
    df_bronze_distinct = bronze_df.select("GroupId").distinct()

    df_to_delete = df_silver_distinct.subtract(df_bronze_distinct)
    df_to_delete_full = silver_table.toDF().join(df_to_delete, "GroupId", "inner")

    silver_table.alias("T").merge(df_to_delete_full.alias("S"), "T.GroupId = S.GroupId") \
        .whenMatchedDelete() \
        .execute()

    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.GroupId = source.GroupId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "GroupId": col("source.GroupId"),
    "GroupName": col("source.GroupName")
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "GroupId": col("source.GroupId"),
    "GroupName": col("source.GroupName")
}).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("tbl_customer_groups")

    source_path = "abfss://SGSCo_Fabric_Development@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_groups_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # DELETE operation for "D"
    filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
        .select("CT_GroupId").distinct()

    silver_table.alias("T").merge(
        filtered_source_df.alias("S"), "T.GroupId = S.CT_GroupId"
    ).whenMatchedDelete().execute()

    # Filter latest changes
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("GroupId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.GroupId") == col("b.GroupId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION"),
        col("a.GroupId"),
        col("a.GroupName"),
      
    ).distinct()

    # MERGE latest changes
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.GroupId = source.GroupId"
).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "GroupId": col("source.GroupId"),
    "GroupName": col("source.GroupName")
}).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "GroupId": col("source.GroupId"),
    "GroupName": col("source.GroupName")
}).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_customer_groups_DELTA ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM SILVER.dbo.tbl_customer_groups ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT COUNT(*) FROM BRONZE.MYSGSEU.tbl_customer_groups_FULL")
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

# Correct schema for DataFrame creation
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("GroupId", StringType(), True),
    StructField("GroupName", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
df.write.format("delta").mode("append").saveAsTable("tbl_customer_groups")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

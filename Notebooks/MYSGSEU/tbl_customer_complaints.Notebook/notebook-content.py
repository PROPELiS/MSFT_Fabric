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
# META         },
# META         {
# META           "id": "5db3d583-e11f-4ac4-9781-65ee3ee820a0"
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

# Define the schema with updated columns (Added new ones and removed old ones)
orderschema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_ComplaintId", StringType(), True),
    StructField("ComplaintId", StringType(), True),
    StructField("ComplaintStatus", StringType(), True),
    StructField("ComplaintDateTime", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("CategoryId", StringType(), True),
    StructField("ComplaintDesc", StringType(), True),
    StructField("ProductionTaskId", StringType(), True),
    StructField("FaultTypeId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("Contact", StringType(), True),
    StructField("ContactEmail", StringType(), True),
    StructField("CustomerRef", StringType(), True),
    StructField("ComplaintValid", StringType(), True),
    StructField("ClaimDesc", StringType(), True),
    StructField("ClaimValid", StringType(), True),
    StructField("InvestigationNotes", StringType(), True),
    StructField("ResolutionDesc", StringType(), True),
    StructField("ResponseRequired", StringType(), True),
    StructField("ResponseGiven", StringType(), True),
    StructField("FinalCost", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("ClosureDateTime", StringType(), True),
    StructField("RaisedBy", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("ComplaintId", StringType(), True),
    StructField("ComplaintStatus", StringType(), True),
    StructField("ComplaintDateTime", StringType(), True),
    StructField("JobVersionId", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("CategoryId", StringType(), True),
    StructField("ComplaintDesc", StringType(), True),
    StructField("ProductionTaskId", StringType(), True),
    StructField("FaultTypeId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("Contact", StringType(), True),
    StructField("ContactEmail", StringType(), True),
    StructField("CustomerRef", StringType(), True),
    StructField("ComplaintValid", StringType(), True),
    StructField("ClaimDesc", StringType(), True),
    StructField("ClaimValid", StringType(), True),
    StructField("InvestigationNotes", StringType(), True),
    StructField("ResolutionDesc", StringType(), True),
    StructField("ResponseRequired", StringType(), True),
    StructField("ResponseGiven", StringType(), True),
    StructField("FinalCost", StringType(), True),
    StructField("CreditNoteId", StringType(), True),
    StructField("ClosureDateTime", StringType(), True),
    StructField("RaisedBy", StringType(), True)
])

# Creating an empty DataFrame with the new schema
df = spark.createDataFrame([], schema)

# Define the Silver Delta Table Path
silver_table_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_customer_complaints"

# Load the Silver Table as a Delta Table
silver_table = DeltaTable.forPath(spark, silver_table_path)

PARAM = ""
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_customer_complaints")
    # Define the Bronze Table Path
    bronze_table_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_complaints_FULL"

 # Read the Bronze Delta Table
    bronze_df = spark.read.format("delta").load(bronze_table_path)

    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.ComplaintId = source.ComplaintId"
     ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ComplaintId": col("source.ComplaintId"),
        "ComplaintStatus": col("source.ComplaintStatus"),
        "ComplaintDateTime": col("source.ComplaintDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "Priority": col("source.Priority"),
        "CategoryId": col("source.CategoryId"),
        "ComplaintDesc": col("source.ComplaintDesc"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "FaultTypeId": col("source.FaultTypeId"),
        "CustomerId": col("source.CustomerId"),
        "Contact": col("source.Contact"),
        "ContactEmail": col("source.ContactEmail"),
        "CustomerRef": col("source.CustomerRef"),
        "ComplaintValid": col("source.ComplaintValid"),
        "ClaimDesc": col("source.ClaimDesc"),
        "ClaimValid": col("source.ClaimValid"),
        "InvestigationNotes": col("source.InvestigationNotes"),
        "ResolutionDesc": col("source.ResolutionDesc"),
        "ResponseRequired": col("source.ResponseRequired"),
        "ResponseGiven": col("source.ResponseGiven"),
        "FinalCost": col("source.FinalCost"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ClosureDateTime": col("source.ClosureDateTime"),
        "RaisedBy": col("source.RaisedBy")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ComplaintId": col("source.ComplaintId"),
        "ComplaintStatus": col("source.ComplaintStatus"),
        "ComplaintDateTime": col("source.ComplaintDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "Priority": col("source.Priority"),
        "CategoryId": col("source.CategoryId"),
        "ComplaintDesc": col("source.ComplaintDesc"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "FaultTypeId": col("source.FaultTypeId"),
        "CustomerId": col("source.CustomerId"),
        "Contact": col("source.Contact"),
        "ContactEmail": col("source.ContactEmail"),
        "CustomerRef": col("source.CustomerRef"),
        "ComplaintValid": col("source.ComplaintValid"),
        "ClaimDesc": col("source.ClaimDesc"),
        "ClaimValid": col("source.ClaimValid"),
        "InvestigationNotes": col("source.InvestigationNotes"),
        "ResolutionDesc": col("source.ResolutionDesc"),
        "ResponseRequired": col("source.ResponseRequired"),
        "ResponseGiven": col("source.ResponseGiven"),
        "FinalCost": col("source.FinalCost"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ClosureDateTime": col("source.ClosureDateTime"),
        "RaisedBy": col("source.RaisedBy")
      }).execute()
else:
     df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_customer_complaints")
     source_path = "abfss://Propelis_Fabric_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customer_complaints_DELTA"
     source_df_delta = spark.read.format("delta").load(source_path)

     filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
        .select("CT_ComplaintId").distinct()

     silver_table.alias("T") \
        .merge(filtered_source_df.alias("S"), "T.ComplaintId = S.CT_ComplaintId") \
        .whenMatchedDelete() \
        .execute()
        # Perform the transformation with all required columns
     transformed_df = source_df_delta.alias("a").join(
     source_df_delta.groupBy("ComplaintId")
     .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
     .alias("b"),
     (col("a.ComplaintId") == col("b.ComplaintId")) & 
     (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
     ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
.select(
    col("a.SYS_CHANGE_VERSION"),
    col("a.SYS_CHANGE_OPERATION"),
    col("a.ComplaintId"),
    col("a.ComplaintStatus"),
    col("a.ComplaintDateTime"),
    col("a.JobVersionId"),
    col("a.Priority"),
    col("a.CategoryId"),
    col("a.ComplaintDesc"),
    col("a.ProductionTaskId"),
    col("a.FaultTypeId"),
    col("a.CustomerId"),
    col("a.Contact"),
    col("a.ContactEmail"),
    col("a.CustomerRef"),
    col("a.ComplaintValid"),
    col("a.ClaimDesc"),
    col("a.ClaimValid"),
    col("a.InvestigationNotes"),
    col("a.ResolutionDesc"),
    col("a.ResponseRequired"),
    col("a.ResponseGiven"),
    col("a.FinalCost"),
    col("a.CreditNoteId"),
    col("a.ClosureDateTime"),
    col("a.RaisedBy")
).distinct()

     silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ComplaintId = source.ComplaintId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ComplaintId": col("source.ComplaintId"),
        "ComplaintStatus": col("source.ComplaintStatus"),
        "ComplaintDateTime": col("source.ComplaintDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "Priority": col("source.Priority"),
        "CategoryId": col("source.CategoryId"),
        "ComplaintDesc": col("source.ComplaintDesc"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "FaultTypeId": col("source.FaultTypeId"),
        "CustomerId": col("source.CustomerId"),
        "Contact": col("source.Contact"),
        "ContactEmail": col("source.ContactEmail"),
        "CustomerRef": col("source.CustomerRef"),
        "ComplaintValid": col("source.ComplaintValid"),
        "ClaimDesc": col("source.ClaimDesc"),
        "ClaimValid": col("source.ClaimValid"),
        "InvestigationNotes": col("source.InvestigationNotes"),
        "ResolutionDesc": col("source.ResolutionDesc"),
        "ResponseRequired": col("source.ResponseRequired"),
        "ResponseGiven": col("source.ResponseGiven"),
        "FinalCost": col("source.FinalCost"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ClosureDateTime": col("source.ClosureDateTime"),
        "RaisedBy": col("source.RaisedBy")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ComplaintId": col("source.ComplaintId"),
        "ComplaintStatus": col("source.ComplaintStatus"),
        "ComplaintDateTime": col("source.ComplaintDateTime"),
        "JobVersionId": col("source.JobVersionId"),
        "Priority": col("source.Priority"),
        "CategoryId": col("source.CategoryId"),
        "ComplaintDesc": col("source.ComplaintDesc"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "FaultTypeId": col("source.FaultTypeId"),
        "CustomerId": col("source.CustomerId"),
        "Contact": col("source.Contact"),
        "ContactEmail": col("source.ContactEmail"),
        "CustomerRef": col("source.CustomerRef"),
        "ComplaintValid": col("source.ComplaintValid"),
        "ClaimDesc": col("source.ClaimDesc"),
        "ClaimValid": col("source.ClaimValid"),
        "InvestigationNotes": col("source.InvestigationNotes"),
        "ResolutionDesc": col("source.ResolutionDesc"),
        "ResponseRequired": col("source.ResponseRequired"),
        "ResponseGiven": col("source.ResponseGiven"),
        "FinalCost": col("source.FinalCost"),
        "CreditNoteId": col("source.CreditNoteId"),
        "ClosureDateTime": col("source.ClosureDateTime"),
        "RaisedBy": col("source.RaisedBy")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

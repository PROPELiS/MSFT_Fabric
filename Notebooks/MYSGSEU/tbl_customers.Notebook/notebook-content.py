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
    StructField("CT_CustomerId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("CustomerType", StringType(), True),
    StructField("PORequired", StringType(), True),
    StructField("AccountNo", StringType(), True),
    StructField("VATCodeNo", StringType(), True),
    StructField("Terms", StringType(), True),
    StructField("DefaultInvoiceAddress", StringType(), True),
    StructField("DefaultShippingAddress", StringType(), True),
    StructField("TaxCodeId", StringType(), True),
    StructField("ExchangeRateId", StringType(), True),
    StructField("AbbreviatedName", StringType(), True),
    StructField("Alias", StringType(), True),
    StructField("DefaultInvoicePostingAddress", StringType(), True),
    StructField("DimensionsCustomer", StringType(), True),
    StructField("DimensionsAccountNo", StringType(), True),
    StructField("ApprovalSystem", StringType(), True),
    StructField("DefaultLegalEntityAddress", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("OB10Number", StringType(), True),
    StructField("CustomerDeleted", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("BillTo", StringType(), True),
    StructField("SoldTo", StringType(), True),
    StructField("ShipTo", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("GS1EntityGLIN", StringType(), True),
    StructField("GS1CompanyName", StringType(), True),
    StructField("GS1UpdatedOn", StringType(), True),
    StructField("GS1UpdatedBy", StringType(), True),
    StructField("GS1UpdatedFrom", StringType(), True),
    StructField("ShowTaxQuote", StringType(), True),
    StructField("EDIAccountNumber", StringType(), True),
    StructField("AutoInvoice", StringType(), True),
    StructField("ChecklistException", StringType(), True),
    StructField("PortfolioGroupId", StringType(), True),
    StructField("ScoroAPICustomerName", StringType(), True),
    StructField("NSection", StringType(), True),
    StructField("BankId", StringType(), True),
    StructField("Ogone", StringType(), True),
    StructField("ServiceChargeExempt", StringType(), True)
])
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema with all required columns
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("CustomerType", StringType(), True),
    StructField("PORequired", StringType(), True),
    StructField("AccountNo", StringType(), True),
    StructField("VATCodeNo", StringType(), True),
    StructField("Terms", StringType(), True),
    StructField("DefaultInvoiceAddress", StringType(), True),
    StructField("DefaultShippingAddress", StringType(), True),
    StructField("TaxCodeId", StringType(), True),
    StructField("ExchangeRateId", StringType(), True),
    StructField("AbbreviatedName", StringType(), True),
    StructField("Alias", StringType(), True),
    StructField("DefaultInvoicePostingAddress", StringType(), True),
    StructField("DimensionsCustomer", StringType(), True),
    StructField("DimensionsAccountNo", StringType(), True),
    StructField("ApprovalSystem", StringType(), True),
    StructField("DefaultLegalEntityAddress", StringType(), True),
    StructField("CurrencyId", StringType(), True),
    StructField("SiteId", StringType(), True),
    StructField("OB10Number", StringType(), True),
    StructField("CustomerDeleted", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", StringType(), True),
    StructField("BillTo", StringType(), True),
    StructField("SoldTo", StringType(), True),
    StructField("ShipTo", StringType(), True),
    StructField("DAXCustomerId", StringType(), True),
    StructField("GS1EntityGLIN", StringType(), True),
    StructField("GS1CompanyName", StringType(), True),
    StructField("GS1UpdatedOn", StringType(), True),
    StructField("GS1UpdatedBy", StringType(), True),
    StructField("GS1UpdatedFrom", StringType(), True),
    StructField("ShowTaxQuote", StringType(), True),
    StructField("EDIAccountNumber", StringType(), True),
    StructField("AutoInvoice", StringType(), True),
    StructField("ChecklistException", StringType(), True),
    StructField("PortfolioGroupId", StringType(), True),
    StructField("ScoroAPICustomerName", StringType(), True),
    StructField("NSection", StringType(), True),
    StructField("BankId", StringType(), True),
    StructField("Ogone", StringType(), True),
    StructField("ServiceChargeExempt", StringType(), True)
])

# Create an empty DataFrame with the new schema
df = spark.createDataFrame([], schema)

silver_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_customers"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param=""

if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_customers")
    
    bronze_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customers_FULL"
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.CustomerId = source.CustomerId"
  ).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "CustomerId": col("source.CustomerId"),
    "CustomerName": col("source.CustomerName"),
    "CustomerType": col("source.CustomerType"),
    "PORequired": col("source.PORequired"),
    "AccountNo": col("source.AccountNo"),
    "VATCodeNo": col("source.VATCodeNo"),
    "Terms": col("source.Terms"),
    "DefaultInvoiceAddress": col("source.DefaultInvoiceAddress"),
    "DefaultShippingAddress": col("source.DefaultShippingAddress"),
    "TaxCodeId": col("source.TaxCodeId"),
    "ExchangeRateId": col("source.ExchangeRateId"),
    "AbbreviatedName": col("source.AbbreviatedName"),
    "Alias": col("source.Alias"),
    "DefaultInvoicePostingAddress": col("source.DefaultInvoicePostingAddress"),
    "DimensionsCustomer": col("source.DimensionsCustomer"),
    "DimensionsAccountNo": col("source.DimensionsAccountNo"),
    "ApprovalSystem": col("source.ApprovalSystem"),
    "DefaultLegalEntityAddress": col("source.DefaultLegalEntityAddress"),
    "CurrencyId": col("source.CurrencyId"),
    "SiteId": col("source.SiteId"),
    "OB10Number": col("source.OB10Number"),
    "CustomerDeleted": col("source.CustomerDeleted"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "BillTo": col("source.BillTo"),
    "SoldTo": col("source.SoldTo"),
    "ShipTo": col("source.ShipTo"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "GS1EntityGLIN": col("source.GS1EntityGLIN"),
    "GS1CompanyName": col("source.GS1CompanyName"),
    "GS1UpdatedOn": col("source.GS1UpdatedOn"),
    "GS1UpdatedBy": col("source.GS1UpdatedBy"),
    "GS1UpdatedFrom": col("source.GS1UpdatedFrom"),
    "ShowTaxQuote": col("source.ShowTaxQuote"),
    "EDIAccountNumber": col("source.EDIAccountNumber"),
    "AutoInvoice": col("source.AutoInvoice"),
    "ChecklistException": col("source.ChecklistException"),
    "PortfolioGroupId": col("source.PortfolioGroupId"),
    "ScoroAPICustomerName": col("source.ScoroAPICustomerName"),
    "NSection": col("source.NSection"),
    "BankId": col("source.BankId"),
    "Ogone": col("source.Ogone"),
    "ServiceChargeExempt": col("source.ServiceChargeExempt")
   }).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "CustomerId": col("source.CustomerId"),
    "CustomerName": col("source.CustomerName"),
    "CustomerType": col("source.CustomerType"),
    "PORequired": col("source.PORequired"),
    "AccountNo": col("source.AccountNo"),
    "VATCodeNo": col("source.VATCodeNo"),
    "Terms": col("source.Terms"),
    "DefaultInvoiceAddress": col("source.DefaultInvoiceAddress"),
    "DefaultShippingAddress": col("source.DefaultShippingAddress"),
    "TaxCodeId": col("source.TaxCodeId"),
    "ExchangeRateId": col("source.ExchangeRateId"),
    "AbbreviatedName": col("source.AbbreviatedName"),
    "Alias": col("source.Alias"),
    "DefaultInvoicePostingAddress": col("source.DefaultInvoicePostingAddress"),
    "DimensionsCustomer": col("source.DimensionsCustomer"),
    "DimensionsAccountNo": col("source.DimensionsAccountNo"),
    "ApprovalSystem": col("source.ApprovalSystem"),
    "DefaultLegalEntityAddress": col("source.DefaultLegalEntityAddress"),
    "CurrencyId": col("source.CurrencyId"),
    "SiteId": col("source.SiteId"),
    "OB10Number": col("source.OB10Number"),
    "CustomerDeleted": col("source.CustomerDeleted"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "BillTo": col("source.BillTo"),
    "SoldTo": col("source.SoldTo"),
    "ShipTo": col("source.ShipTo"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "GS1EntityGLIN": col("source.GS1EntityGLIN"),
    "GS1CompanyName": col("source.GS1CompanyName"),
    "GS1UpdatedOn": col("source.GS1UpdatedOn"),
    "GS1UpdatedBy": col("source.GS1UpdatedBy"),
    "GS1UpdatedFrom": col("source.GS1UpdatedFrom"),
    "ShowTaxQuote": col("source.ShowTaxQuote"),
    "EDIAccountNumber": col("source.EDIAccountNumber"),
    "AutoInvoice": col("source.AutoInvoice"),
    "ChecklistException": col("source.ChecklistException"),
    "PortfolioGroupId": col("source.PortfolioGroupId"),
    "ScoroAPICustomerName": col("source.ScoroAPICustomerName"),
    "NSection": col("source.NSection"),
    "BankId": col("source.BankId"),
    "Ogone": col("source.Ogone"),
    "ServiceChargeExempt": col("source.ServiceChargeExempt")
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_customers")
source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_customers_DELTA"
source_df_delta = spark.read.format("delta").load(source_path)



# Filtering Deleted Records
filtered_source_df = source_df_delta.filter(col("SYS_CHANGE_OPERATION") == "D") \
    .select("CT_CustomerId").distinct()



# Merging into Silver Table
silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.CustomerId = S.CT_CustomerId") \
    .whenMatchedDelete()

# Finding Latest SYS_CHANGE_VERSION per CustomerId
transformed_df = source_df_delta.alias("a").join(
    source_df_delta.groupBy("CustomerId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
    (col("a.CustomerId") == col("b.CustomerId")) & 
    (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
).where(col("a.SYS_CHANGE_OPERATION") != 'D')\
.select(
    col("a.SYS_CHANGE_VERSION"),
    col("a.SYS_CHANGE_OPERATION"),
    col("a.CustomerId"),         
    col("a.CustomerName"),       
    col("a.CustomerType"),       
    col("a.PORequired"),         
    col("a.AccountNo"),          
    col("a.VATCodeNo"),          
    col("a.Terms"),              
    col("a.DefaultInvoiceAddress"),
    col("a.DefaultShippingAddress"),
    col("a.TaxCodeId"),          
    col("a.ExchangeRateId"),     
    col("a.AbbreviatedName"),    
    col("a.Alias"),              
    col("a.DefaultInvoicePostingAddress"),
    col("a.DimensionsCustomer"), 
    col("a.DimensionsAccountNo"),
    col("a.ApprovalSystem"),     
    col("a.DefaultLegalEntityAddress"),
    col("a.CurrencyId"),         
    col("a.SiteId"),             
    col("a.OB10Number"),         
    col("a.CustomerDeleted"),    
    col("a.CreatedDate"),        
    col("a.UpdatedDate"),        
    col("a.BillTo"),             
    col("a.SoldTo"),             
    col("a.ShipTo"),             
    col("a.DAXCustomerId"),      
    col("a.GS1EntityGLIN"),      
    col("a.GS1CompanyName"),     
    col("a.GS1UpdatedOn"),       
    col("a.GS1UpdatedBy"),       
    col("a.GS1UpdatedFrom"),     
    col("a.ShowTaxQuote"),       
    col("a.EDIAccountNumber"),   
    col("a.AutoInvoice"),        
    col("a.ChecklistException"), 
    col("a.PortfolioGroupId"),   
    col("a.ScoroAPICustomerName"),
    col("a.NSection"),           
    col("a.BankId"),             
    col("a.Ogone"),              
    col("a.ServiceChargeExempt") 
).distinct()

silver_table.alias("target").merge(
    transformed_df.alias("source"),
    "target.CustomerId = source.CustomerId"
  ).whenMatchedUpdate(set={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "CustomerId": col("source.CustomerId"),
    "CustomerName": col("source.CustomerName"),
    "CustomerType": col("source.CustomerType"),
    "PORequired": col("source.PORequired"),
    "AccountNo": col("source.AccountNo"),
    "VATCodeNo": col("source.VATCodeNo"),
    "Terms": col("source.Terms"),
    "DefaultInvoiceAddress": col("source.DefaultInvoiceAddress"),
    "DefaultShippingAddress": col("source.DefaultShippingAddress"),
    "TaxCodeId": col("source.TaxCodeId"),
    "ExchangeRateId": col("source.ExchangeRateId"),
    "AbbreviatedName": col("source.AbbreviatedName"),
    "Alias": col("source.Alias"),
    "DefaultInvoicePostingAddress": col("source.DefaultInvoicePostingAddress"),
    "DimensionsCustomer": col("source.DimensionsCustomer"),
    "DimensionsAccountNo": col("source.DimensionsAccountNo"),
    "ApprovalSystem": col("source.ApprovalSystem"),
    "DefaultLegalEntityAddress": col("source.DefaultLegalEntityAddress"),
    "CurrencyId": col("source.CurrencyId"),
    "SiteId": col("source.SiteId"),
    "OB10Number": col("source.OB10Number"),
    "CustomerDeleted": col("source.CustomerDeleted"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "BillTo": col("source.BillTo"),
    "SoldTo": col("source.SoldTo"),
    "ShipTo": col("source.ShipTo"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "GS1EntityGLIN": col("source.GS1EntityGLIN"),
    "GS1CompanyName": col("source.GS1CompanyName"),
    "GS1UpdatedOn": col("source.GS1UpdatedOn"),
    "GS1UpdatedBy": col("source.GS1UpdatedBy"),
    "GS1UpdatedFrom": col("source.GS1UpdatedFrom"),
    "ShowTaxQuote": col("source.ShowTaxQuote"),
    "EDIAccountNumber": col("source.EDIAccountNumber"),
    "AutoInvoice": col("source.AutoInvoice"),
    "ChecklistException": col("source.ChecklistException"),
    "PortfolioGroupId": col("source.PortfolioGroupId"),
    "ScoroAPICustomerName": col("source.ScoroAPICustomerName"),
    "NSection": col("source.NSection"),
    "BankId": col("source.BankId"),
    "Ogone": col("source.Ogone"),
    "ServiceChargeExempt": col("source.ServiceChargeExempt")
   }).whenNotMatchedInsert(values={
    "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
    "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
    "CustomerId": col("source.CustomerId"),
    "CustomerName": col("source.CustomerName"),
    "CustomerType": col("source.CustomerType"),
    "PORequired": col("source.PORequired"),
    "AccountNo": col("source.AccountNo"),
    "VATCodeNo": col("source.VATCodeNo"),
    "Terms": col("source.Terms"),
    "DefaultInvoiceAddress": col("source.DefaultInvoiceAddress"),
    "DefaultShippingAddress": col("source.DefaultShippingAddress"),
    "TaxCodeId": col("source.TaxCodeId"),
    "ExchangeRateId": col("source.ExchangeRateId"),
    "AbbreviatedName": col("source.AbbreviatedName"),
    "Alias": col("source.Alias"),
    "DefaultInvoicePostingAddress": col("source.DefaultInvoicePostingAddress"),
    "DimensionsCustomer": col("source.DimensionsCustomer"),
    "DimensionsAccountNo": col("source.DimensionsAccountNo"),
    "ApprovalSystem": col("source.ApprovalSystem"),
    "DefaultLegalEntityAddress": col("source.DefaultLegalEntityAddress"),
    "CurrencyId": col("source.CurrencyId"),
    "SiteId": col("source.SiteId"),
    "OB10Number": col("source.OB10Number"),
    "CustomerDeleted": col("source.CustomerDeleted"),
    "CreatedDate": col("source.CreatedDate"),
    "UpdatedDate": col("source.UpdatedDate"),
    "BillTo": col("source.BillTo"),
    "SoldTo": col("source.SoldTo"),
    "ShipTo": col("source.ShipTo"),
    "DAXCustomerId": col("source.DAXCustomerId"),
    "GS1EntityGLIN": col("source.GS1EntityGLIN"),
    "GS1CompanyName": col("source.GS1CompanyName"),
    "GS1UpdatedOn": col("source.GS1UpdatedOn"),
    "GS1UpdatedBy": col("source.GS1UpdatedBy"),
    "GS1UpdatedFrom": col("source.GS1UpdatedFrom"),
    "ShowTaxQuote": col("source.ShowTaxQuote"),
    "EDIAccountNumber": col("source.EDIAccountNumber"),
    "AutoInvoice": col("source.AutoInvoice"),
    "ChecklistException": col("source.ChecklistException"),
    "PortfolioGroupId": col("source.PortfolioGroupId"),
    "ScoroAPICustomerName": col("source.ScoroAPICustomerName"),
    "NSection": col("source.NSection"),
    "BankId": col("source.BankId"),
    "Ogone": col("source.Ogone"),
    "ServiceChargeExempt": col("source.ServiceChargeExempt")
    }).execute()

    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

CREATE TABLE [dbo].[EDW_T_D_MST_CUSULPT_CUR_D] (

	[CUST_UNLOADING_PNT_KEY] int NOT NULL, 
	[Customer Number] varchar(max) NULL, 
	[Unloading Point] varchar(max) NULL, 
	[Sequential Number for Unloading Points] int NULL, 
	[Customer Factory Calendar] varchar(max) NULL, 
	[Tax Classification Description] varchar(max) NULL, 
	[ETL Source System Code] varchar(max) NULL, 
	[ETL Effective Begin Date] date NULL, 
	[ETL Effective End Date] date NULL, 
	[ETL Current Record Indicator] varchar(max) NULL, 
	[ETL Created Timestamp] datetime2(6) NULL, 
	[ETL Updated Timestamp] datetime2(6) NULL
);
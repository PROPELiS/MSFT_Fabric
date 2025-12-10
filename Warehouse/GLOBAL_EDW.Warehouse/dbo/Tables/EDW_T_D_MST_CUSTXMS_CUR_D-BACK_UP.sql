CREATE TABLE [dbo].[EDW_T_D_MST_CUSTXMS_CUR_D-BACK_UP] (

	[CUST_TAX_MAST_KEY] int NOT NULL, 
	[Customer Number] varchar(max) NULL, 
	[Departure Country] varchar(max) NULL, 
	[Tax Category] varchar(max) NULL, 
	[Tax Category Description] varchar(max) NULL, 
	[Tax Classification] varchar(max) NULL, 
	[Tax Classification Description] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE ] date NULL, 
	[ETL_EFFECTV_END_DATE ] date NULL, 
	[Current Record Indicator of Customer Tax Master] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS ] datetime2(0) NULL
);
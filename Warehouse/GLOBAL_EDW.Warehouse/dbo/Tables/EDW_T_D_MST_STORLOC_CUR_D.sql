CREATE TABLE [dbo].[EDW_T_D_MST_STORLOC_CUR_D] (

	[STOR_LOC_KEY] int NOT NULL, 
	[Storage Location Code] varchar(max) NULL, 
	[Store Location Plant ID] varchar(max) NULL, 
	[Store Location Warehouse Number] varchar(max) NULL, 
	[Storage Location Description] varchar(max) NULL, 
	[Store Location Shipping Receiving Point] varchar(max) NULL, 
	[Store Location Sales Organization] varchar(max) NULL, 
	[Store Location Vendor Account Number] varchar(max) NULL, 
	[Store Location Customer Account Number] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Store Location Plant Description] varchar(max) NULL, 
	[Store Location Warehouse Description] varchar(max) NULL, 
	[Store Location Shipping Receiving Point Description] varchar(max) NULL, 
	[Store Location Sales Organization Description] varchar(max) NULL
);
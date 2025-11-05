CREATE TABLE [Propelis].[EDW_T_MST_CUSTBRD_CUR_D] (

	[CUST_BRAND_HIERCHY_KEY] int NOT NULL, 
	[Customer Brand Customer ID] varchar(10) NULL, 
	[Customer Brand Category Code] int NULL, 
	[Customer Brand Category Description] varchar(60) NULL, 
	[Customer Brand ID] int NULL, 
	[Customer Brand Description] varchar(60) NULL, 
	[Customer Sub Brand ID] int NULL, 
	[Customer Sub Brand Description] varchar(60) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
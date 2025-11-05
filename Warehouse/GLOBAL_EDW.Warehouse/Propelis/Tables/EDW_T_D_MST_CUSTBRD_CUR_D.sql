CREATE TABLE [Propelis].[EDW_T_D_MST_CUSTBRD_CUR_D] (

	[CUST_BRAND_HIERCHY_KEY] int NOT NULL, 
	[CUST_ID] varchar(max) NULL, 
	[CTGRY_CD] int NULL, 
	[CTGRY_DESC] varchar(max) NULL, 
	[BRAND_ID] int NULL, 
	[BRAND_DESC] varchar(max) NULL, 
	[SUB_BRAND_ID] int NULL, 
	[SUB_BRAND_DESC] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] datetime2(0) NULL, 
	[ETL_EFFECTV_END_DATE] datetime2(0) NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
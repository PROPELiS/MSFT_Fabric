CREATE TABLE [dbo].[EDW_T_D_MST_STORLOC_CUR_D] (

	[STOR_LOC_KEY] int NOT NULL, 
	[STOR_LOC_CD] varchar(4) NULL, 
	[PLNT_ID] varchar(4) NULL, 
	[WRHSE_NUM] varchar(3) NULL, 
	[STOR_LOC_DESC] varchar(40) NULL, 
	[SHIPG_RECVNG_PNT] varchar(4) NULL, 
	[SALES_ORGZTN] varchar(4) NULL, 
	[VNDR_ID] varchar(10) NULL, 
	[CUST_ID] varchar(10) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[PLNT_DESC] varchar(30) NULL, 
	[WRHSE_DESC] varchar(25) NULL, 
	[SHIPG_RECVNG_PNT_DESC] varchar(30) NULL, 
	[SALES_ORG_DESC] varchar(20) NULL
);
CREATE TABLE [dbo].[EDW_T_D_MST_PLANT_CUR_D] (

	[PLNT_KEY] int NOT NULL, 
	[PLNT_ID] varchar(4) NULL, 
	[LGCY_PLNT_ID] varchar(10) NULL, 
	[PLNT_NAME] varchar(30) NULL, 
	[CMPNY_CD] varchar(4) NULL, 
	[SALES_ORGZTN] varchar(4) NULL, 
	[PURNG_ORGZTN] varchar(4) NULL, 
	[VALTN_AREA] varchar(4) NULL, 
	[CUST_NUM_OF_PLNT] varchar(10) NULL, 
	[VNDR_NUM_OF_PLNT] varchar(10) NULL, 
	[FACTORY_CLNDR_KEY] varchar(2) NULL, 
	[PLNT_LINE_1_DESC] varchar(30) NULL, 
	[PLNT_LINE_2_DESC] varchar(30) NULL, 
	[PLNT_CITY_NAME] varchar(25) NULL, 
	[PLNT_ZIP_CD] varchar(10) NULL, 
	[PLNT_CNTRY_CD] varchar(3) NULL, 
	[PLNT_CNTRY_NAME] varchar(40) NULL, 
	[TAX_JRSDCTN] varchar(15) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[IS_P001] varchar(1) NULL, 
	[IS_P002] varchar(1) NULL, 
	[CMPNY_DESC] varchar(25) NULL, 
	[SALES_ORG_DESC] varchar(20) NULL, 
	[FACTORY_CLNDR_DESC] varchar(60) NULL, 
	[PURNG_ORGZTN_DESC] varchar(20) NULL, 
	[PLNT_TAX_IND] varchar(1) NULL, 
	[PLNT_TAX_IND_DESC] varchar(20) NULL, 
	[SHIPG_RECVNG_PNT] varchar(4) NULL, 
	[SHIPG_RECVNG_PNT_DESC] varchar(30) NULL
);
CREATE TABLE [dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] (

	[ADDR_KEY] int NOT NULL, 
	[ADDR_NUM] varchar(10) NULL, 
	[VALID_FROM_DATE] date NULL, 
	[INTERNATIONAL_ADDR_VRSN_ID] varchar(1) NULL, 
	[NAME_1] varchar(40) NULL, 
	[STR] varchar(60) NULL, 
	[CITY] varchar(40) NULL, 
	[REGN] varchar(3) NULL, 
	[CITY_POSTAL_CD] varchar(10) NULL, 
	[CNTRY_KEY] varchar(3) NULL, 
	[LANG_KEY] varchar(1) NULL, 
	[FIRST_TEL_NO] varchar(30) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
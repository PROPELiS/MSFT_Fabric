CREATE TABLE [dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] (

	[CMPNY_KEY] int NOT NULL, 
	[CMPNY_CD] varchar(4) NULL, 
	[LGCY_CMPNY_CD] varchar(10) NULL, 
	[CMPNY_NAME] varchar(60) NULL, 
	[LINE_OF_BIZ_CD] varchar(20) NULL, 
	[LINE_OF_BIZ_CD_DESC] varchar(255) NULL, 
	[CRNCY_CD] varchar(5) NULL, 
	[LANG_TYP_CD] varchar(1) NULL, 
	[CHART_OF_ACCTS_ID] varchar(4) NULL, 
	[ENTTY_ADDR_LINE_1] varchar(60) NULL, 
	[ENTTY_ADDR_LINE_2] varchar(60) NULL, 
	[ENTTY_CITY_NAME] varchar(40) NULL, 
	[ENTTY_ZIP_CD] varchar(10) NULL, 
	[ENTTY_CNTRY_CD] varchar(3) NULL, 
	[ENTTY_TELEPHONE1] varchar(30) NULL, 
	[ENTTY_TELEPHONE2] varchar(30) NULL, 
	[FAX_NUM] varchar(30) NULL, 
	[TAX_JRSDCTN] varchar(15) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[FISCAL_YR_VARIANT] varchar(2) NULL, 
	[CREDIT_CTRL_AREA] varchar(4) NULL, 
	[INPUT_TAX_CD] varchar(2) NULL, 
	[OTPUT_TAX_CD] varchar(2) NULL, 
	[CRNCY_DESC] varchar(15) NULL, 
	[CHART_OF_ACCTS_DESC] varchar(50) NULL, 
	[CNTRY_DESC] varchar(15) NULL, 
	[FISCAL_YR_VARIANT_DESC] varchar(30) NULL, 
	[CREDIT_CTRL_AREA_DESC] varchar(35) NULL
);
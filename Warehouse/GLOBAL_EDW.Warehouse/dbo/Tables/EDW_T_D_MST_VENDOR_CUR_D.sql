CREATE TABLE [dbo].[EDW_T_D_MST_VENDOR_CUR_D] (

	[VNDR_KEY] int NOT NULL, 
	[VNDR_ID] varchar(10) NULL, 
	[VNDR_NAME] varchar(60) NULL, 
	[VNDR_DESC] varchar(60) NULL, 
	[VNDR_ACCT_GROUP] varchar(4) NULL, 
	[TITLE] varchar(15) NULL, 
	[SEARCH_TERM] varchar(10) NULL, 
	[STR] varchar(35) NULL, 
	[CITY_NAME] varchar(35) NULL, 
	[ZIP_CD] varchar(10) NULL, 
	[CNTRY_CD] varchar(3) NULL, 
	[ST] varchar(3) NULL, 
	[TEL_NUM_1_CD] varchar(16) NULL, 
	[FAX_NUM_CD] varchar(31) NULL, 
	[TAX_NUM_1_CD] varchar(16) NULL, 
	[TAX_NUM_2_CD] varchar(11) NULL, 
	[VAT_NUM_CD] varchar(20) NULL, 
	[EXPRESS_STATION] varchar(25) NULL, 
	[TRANSTN_ZONE] varchar(10) NULL, 
	[INDUS] varchar(4) NULL, 
	[CREATED_DATE] date NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[ADDR_NUM] varchar(10) NULL, 
	[VNDR_ACCT_GROUP_DESC] varchar(30) NULL, 
	[CNTRY_DESC] varchar(15) NULL, 
	[REGN_DESC] varchar(20) NULL, 
	[TRANSTN_ZONE_DESC] varchar(20) NULL, 
	[INDUS_DESC] varchar(20) NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[CENTRAL_DELETION_IND] varchar(1) NULL, 
	[CENTRAL_PSTNG_BLK] varchar(1) NULL, 
	[CENTRAL_PURNG_BLK] varchar(1) NULL, 
	[LANG_KEY] varchar(1) NULL, 
	[CUST_ID] varchar(10) NULL, 
	[ONE_TM_ACCT_IND] varchar(1) NULL, 
	[ONE_TM_ACCT_REF_ACCT__GROUP] varchar(4) NULL, 
	[ONE_TM_ACC_REF_ACCT_GROUP_DESC] varchar(30) NULL, 
	[TRADING_PRTNR] varchar(6) NULL, 
	[TRADING_PRTNR_DESC] varchar(30) NULL, 
	[VAT_IND] varchar(1) NULL, 
	[INDUS_TYP] varchar(30) NULL, 
	[BIZ_TYP] varchar(30) NULL, 
	[PYMT_BLK] varchar(1) NULL, 
	[TAX_JRSDCTN] varchar(15) NULL, 
	[TAX_NUM_3_CD] varchar(18) NULL, 
	[TAX_NUM_TYP] varchar(2) NULL, 
	[TAX_TYP] varchar(2) NULL, 
	[LVL_1] varchar(60) NULL, 
	[LVL_2] varchar(60) NULL, 
	[LVL_3] varchar(60) NULL, 
	[LVL_4] varchar(60) NULL, 
	[ADDRESSABLE] varchar(10) NULL, 
	[TAIL_VNDR] varchar(10) NULL, 
	[NORMALIZED_VNDR] varchar(60) NULL, 
	[CORPORATE_GROUP] varchar(10) NULL, 
	[ACCT_NO_ALT_PYE] varchar(10) NULL, 
	[E_MAIL_ADDR] varchar(241) NULL
);
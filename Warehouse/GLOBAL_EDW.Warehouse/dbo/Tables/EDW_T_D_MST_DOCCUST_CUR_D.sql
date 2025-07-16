CREATE TABLE [dbo].[EDW_T_D_MST_DOCCUST_CUR_D] (

	[DOCMT_CUST_KEY] int NOT NULL, 
	[DOCMT_TYP] varchar(3) NULL, 
	[DOCMT_NUM] varchar(25) NULL, 
	[DOCMT_VRSN] varchar(2) NULL, 
	[CUST_NUM] varchar(50) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[DOCMT_TYP_DESC] varchar(20) NULL
);
CREATE TABLE [dbo].[EDW_T_D_MST_GLCMPCD_CUR_D] (

	[GL_CMPNY_KEY] int NOT NULL, 
	[GL_CD] varchar(10) NULL, 
	[CMPNY_CD] varchar(4) NULL, 
	[CONTROLLING_AREA] varchar(4) NULL, 
	[CONTROLLING_AREA_DESC] varchar(25) NULL, 
	[CNTRY_KEY] varchar(3) NULL, 
	[CREATED_DATE] date NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[MRKD_FOR_DELETION_IND] varchar(1) NULL, 
	[ACCT_CRNCY] varchar(5) NULL, 
	[ACCT_CRNCY_DESC] varchar(15) NULL, 
	[FLD_STATUS_GROUP] varchar(4) NULL, 
	[FLD_STATUS_GROUP_DESC] varchar(40) NULL, 
	[LINE_ITMS_DISPLAY_IND] varchar(1) NULL, 
	[BALANCES_IN_LCL_CRNCY_IND] varchar(1) NULL, 
	[OPEN_ITM_MGMNT_IND] varchar(1) NULL, 
	[NO_TAX_CD_REQUIRED_IND] varchar(1) NULL, 
	[CASH_RCPT_DISBURSEMENT_IND] varchar(1) NULL, 
	[ACCT_TAX_CTGRY] varchar(2) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
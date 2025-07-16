CREATE TABLE [dbo].[EDW_T_D_MST_CRNCY_CUR_D] (

	[CRNCY_KEY] int NOT NULL, 
	[CRNCY_CD] varchar(5) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[SHORT_DESC] varchar(15) NULL, 
	[LONG_DESC] varchar(40) NULL
);
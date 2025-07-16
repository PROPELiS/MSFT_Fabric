CREATE TABLE [dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] (

	[SEG_KEY] int NOT NULL, 
	[NAME] varchar(50) NULL, 
	[SEG] varchar(10) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
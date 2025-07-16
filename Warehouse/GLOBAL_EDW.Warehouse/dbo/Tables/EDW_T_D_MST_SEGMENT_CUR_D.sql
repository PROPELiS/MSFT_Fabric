CREATE TABLE [dbo].[EDW_T_D_MST_SEGMENT_CUR_D] (

	[SEG_KEY] int NOT NULL, 
	[SEG_ID] varchar(10) NULL, 
	[SEG_CD_DESC] varchar(40) NULL, 
	[BIZ_UNIT_CD] varchar(10) NULL, 
	[BIZ_UNIT_CD_DESC] varchar(40) NULL, 
	[CREATED_DATE] date NULL, 
	[CREATED_USR_ID] varchar(12) NULL, 
	[UPDTD_DATE] date NULL, 
	[UPDTD_USR_ID] varchar(12) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
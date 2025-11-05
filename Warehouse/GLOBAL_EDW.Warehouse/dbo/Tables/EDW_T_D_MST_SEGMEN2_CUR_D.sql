CREATE TABLE [dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] (

	[SEG_KEY] int NOT NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[Segment] varchar(max) NULL, 
	[Segment Name] varchar(max) NULL
);
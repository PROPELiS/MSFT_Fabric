CREATE TABLE [dbo].[TEC_T_D_ETL_JBSTATS2_CUR_D_20240308_BK] (

	[BODS_JOB_ID] varchar(200) NOT NULL, 
	[JOB_NAME] varchar(200) NOT NULL, 
	[START_DATETIME] datetime2(0) NOT NULL, 
	[END_DATETIME] datetime2(0) NULL, 
	[JOB_GROUP_NAME] varchar(200) NULL, 
	[TOTAL_TM_DUR] int NULL, 
	[BATCH_DATE] date NULL, 
	[JOB_STATUS] varchar(20) NULL, 
	[JOB_STATUS_CD] varchar(20) NULL
);
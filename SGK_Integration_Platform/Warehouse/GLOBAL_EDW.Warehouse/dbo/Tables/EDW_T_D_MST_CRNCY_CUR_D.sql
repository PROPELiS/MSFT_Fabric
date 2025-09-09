CREATE TABLE [dbo].[EDW_T_D_MST_CRNCY_CUR_D] (

	[CRNCY_KEY] int NOT NULL, 
	[Currency Code] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Currency Short Description] varchar(max) NULL, 
	[Currency Long Description] varchar(max) NULL
);
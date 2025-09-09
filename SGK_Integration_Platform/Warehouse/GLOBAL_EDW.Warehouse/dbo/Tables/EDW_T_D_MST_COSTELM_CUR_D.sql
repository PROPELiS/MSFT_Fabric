CREATE TABLE [dbo].[EDW_T_D_MST_COSTELM_CUR_D] (

	[COST_ELEMNT_KEY] int NOT NULL, 
	[Cost Element ID] varchar(max) NULL, 
	[Cost Element Chart Of Accounts] varchar(max) NULL, 
	[Cost Element Name] varchar(max) NULL, 
	[Cost Element Description] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[Current Record Indicator of Cost Element] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Cost Element Chart Of Accounts Description] varchar(max) NULL
);
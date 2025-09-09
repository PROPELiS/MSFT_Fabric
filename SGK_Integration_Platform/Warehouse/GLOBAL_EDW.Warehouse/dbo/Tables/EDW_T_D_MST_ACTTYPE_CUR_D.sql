CREATE TABLE [dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] (

	[ACTIVITY_TYP_KEY] int NOT NULL, 
	[Activity Type ID] varchar(max) NULL, 
	[Activity Type Description] varchar(max) NULL, 
	[Activity Type Description 2] varchar(max) NULL, 
	[Activity Type Controlling Area] varchar(max) NULL, 
	[Activity Type Unit] varchar(max) NULL, 
	[Activity Type Category] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Activity Type Controlling Area Description] varchar(max) NULL, 
	[Activity Type Category Description] varchar(max) NULL
);
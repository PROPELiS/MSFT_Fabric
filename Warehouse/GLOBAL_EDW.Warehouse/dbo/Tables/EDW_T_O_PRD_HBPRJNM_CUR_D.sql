CREATE TABLE [dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] (

	[SERVC_ORDER_ID] varchar(12) NOT NULL, 
	[SALES_ORDER_ID] varchar(10) NULL, 
	[JOB_ID] varchar(20) NOT NULL, 
	[PROJ_NAME] varchar(255) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[ZLP_BB_COL_PROFILE] varchar(255) NULL, 
	[MILESTONE9_IN_DATE] datetime2(0) NULL
);
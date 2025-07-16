CREATE TABLE [dbo].[EDW_T_D_FI_BPCGLGR_CUR_D] (

	[GL_CD] varchar(10) NOT NULL, 
	[CHART_OF_ACCTS] varchar(4) NOT NULL, 
	[GL_DESC] varchar(60) NULL, 
	[REVNU] int NULL, 
	[NET_INCOM] int NULL, 
	[EBITDA] int NULL, 
	[GROSS_MRGN] int NULL, 
	[LABOR_COST] int NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
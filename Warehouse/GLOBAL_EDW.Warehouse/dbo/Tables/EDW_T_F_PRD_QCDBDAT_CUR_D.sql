CREATE TABLE [dbo].[EDW_T_F_PRD_QCDBDAT_CUR_D] (

	[SERVC_ORDER_ID] varchar(12) NOT NULL, 
	[JOB_CTGRY] varchar(50) NOT NULL, 
	[REVISION_ID] varchar(10) NOT NULL, 
	[REVISION_NO] varchar(10) NOT NULL, 
	[QC_JOB_ID] varchar(20) NOT NULL, 
	[UNQ_IDENTIFIER] varchar(20) NOT NULL, 
	[TASK] varchar(100) NOT NULL, 
	[SERVC_ORDER_KEY] int NULL, 
	[SOLDTO_PARTY_KEY] int NULL, 
	[CHECKLIST_ACTIVITY_TYP] varchar(100) NULL, 
	[PA_USR] varchar(100) NULL, 
	[QC_USR_ID] varchar(100) NULL, 
	[DOES_STAGE_HAVE_ERR] varchar(10) NULL, 
	[ERR_COMMENT] varchar(100) NULL, 
	[CUST_BRAND_HIERCHY_KEY] int NULL, 
	[SCHAWK_PLNT_KEY] int NULL, 
	[PA_PLNT_KEY] int NULL, 
	[QC_PLNT_KEY] int NULL, 
	[DATE_COMPLETION_PA_KEY] int NULL, 
	[DATE_COMPLETION_QC_KEY] int NULL, 
	[PARNT_KEY] int NULL, 
	[CREATED_ON_TS] datetime2(0) NULL, 
	[CREATED_ON_DATE_KEY] int NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[INSTRUCTIONS] varchar(255) NULL
);
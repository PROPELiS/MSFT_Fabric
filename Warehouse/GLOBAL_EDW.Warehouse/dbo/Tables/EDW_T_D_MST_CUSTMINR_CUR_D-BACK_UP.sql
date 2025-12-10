CREATE TABLE [dbo].[EDW_T_D_MST_CUSTMINR_CUR_D-BACK_UP] (

	[CUST_MATRL_INFO_RCD_KEY] int NOT NULL, 
	[CMIR Sales Organization] varchar(max) NULL, 
	[CMIR Distribution Channel] varchar(max) NULL, 
	[CMIR Customer Number] varchar(max) NULL, 
	[CMIR Material ID] varchar(max) NULL, 
	[CMIR Customer Material ID] varchar(max) NULL, 
	[CMIR Base UoM] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[Current Record Indicator of Customer Material Info Record] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[CMIR Customer Description] varchar(max) NULL
);
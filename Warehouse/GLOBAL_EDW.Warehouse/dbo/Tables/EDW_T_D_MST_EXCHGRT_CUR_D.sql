CREATE TABLE [dbo].[EDW_T_D_MST_EXCHGRT_CUR_D] (

	[EXCHG_RT_TYP] varchar(max) NOT NULL, 
	[Exchange Rate Type Description] varchar(max) NULL, 
	[From Currency] varchar(max) NOT NULL, 
	[To Currency] varchar(max) NOT NULL, 
	[Valid From Date] date NOT NULL, 
	[Exchange Rate] decimal(20,10) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
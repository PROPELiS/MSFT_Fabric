CREATE TABLE [dbo].[EDW_T_D_CO_PEGRATE_CUR_D] (

	[PEG_RT_KEY] int NOT NULL, 
	[FROM_CRNCY] varchar(5) NULL, 
	[TO_CRNCY] varchar(5) NULL, 
	[EXCHG_RT] decimal(9,5) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
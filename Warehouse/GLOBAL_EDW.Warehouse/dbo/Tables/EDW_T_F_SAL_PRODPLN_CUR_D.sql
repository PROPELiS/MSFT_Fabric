CREATE TABLE [dbo].[EDW_T_F_SAL_PRODPLN_CUR_D] (

	[CMPNY_KEY] int NOT NULL, 
	[PLNT_KEY] int NOT NULL, 
	[COST_CNTR_KEY] int NOT NULL, 
	[DATE_KEY] int NOT NULL, 
	[KPI] varchar(18) NOT NULL, 
	[TRGT_VAL] decimal(18,3) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
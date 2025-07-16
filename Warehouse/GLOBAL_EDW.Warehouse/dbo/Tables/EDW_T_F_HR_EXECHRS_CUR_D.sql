CREATE TABLE [dbo].[EDW_T_F_HR_EXECHRS_CUR_D] (

	[CMPNY_KEY] int NOT NULL, 
	[COST_CNTR_KEY] int NOT NULL, 
	[PLNT_KEY] int NOT NULL, 
	[PSTNG_DATE_KEY] int NOT NULL, 
	[HRS_PLND] decimal(18,2) NULL, 
	[HRS_EXTD] decimal(18,2) NULL, 
	[HRS_BAL] decimal(18,2) NULL, 
	[ABSNE_TYP] varchar(5) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
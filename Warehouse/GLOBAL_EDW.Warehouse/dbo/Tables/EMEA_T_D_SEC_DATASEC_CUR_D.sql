CREATE TABLE [dbo].[EMEA_T_D_SEC_DATASEC_CUR_D] (

	[CMPNY_CD] varchar(4) NOT NULL, 
	[PLNT_ID] varchar(4) NOT NULL, 
	[COST_CNTR_ID] varchar(10) NOT NULL, 
	[SEG_ID] varchar(10) NOT NULL, 
	[USR_ID] varchar(80) NOT NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
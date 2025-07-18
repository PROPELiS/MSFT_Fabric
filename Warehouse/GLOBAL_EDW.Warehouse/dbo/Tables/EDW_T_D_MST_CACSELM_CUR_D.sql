CREATE TABLE [dbo].[EDW_T_D_MST_CACSELM_CUR_D] (

	[CONTROLLING_AREA_COST_ELEMNT_KE] int NOT NULL, 
	[CONTROLLING_AREA] varchar(4) NULL, 
	[COST_ELEMNT] varchar(10) NULL, 
	[VALID_TO_DATE] date NULL, 
	[VALID_FROM_DATE] date NULL, 
	[COST_ELEMNT_CTGRY] varchar(2) NULL, 
	[COST_ELEMNT_CTGRY_DESC] varchar(60) NULL, 
	[CREATED_ON] date NULL, 
	[ENTERED_BY] varchar(12) NULL, 
	[COST_ELEMNT_ATTRIBUTES] varchar(8) NULL, 
	[PLNG_ACCESS_IND] varchar(1) NULL, 
	[PLNG_LOC_IND] varchar(1) NULL, 
	[PLNG_USR_INDICATORS] varchar(2) NULL, 
	[COST_CNTR] varchar(10) NULL, 
	[ORDER_NUM] varchar(12) NULL, 
	[RCDG_CONSPTN_QUANTITIES_IND] varchar(1) NULL, 
	[UNIT_OF_MSRMNT] varchar(3) NULL, 
	[COST_ELEMNT_DEACTIVATED_IND] varchar(1) NULL, 
	[COST_ELEMNT_IS_FLAGGED_FOR_DELE] varchar(1) NULL, 
	[RECOVERY_IND] varchar(2) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
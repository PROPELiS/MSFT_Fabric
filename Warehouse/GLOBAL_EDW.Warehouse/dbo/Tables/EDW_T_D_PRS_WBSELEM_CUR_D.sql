CREATE TABLE [dbo].[EDW_T_D_PRS_WBSELEM_CUR_D] (

	[WBS_ELEMNT_KEY] int NOT NULL, 
	[PROJ_DEFINITION_INTERNL] bigint NULL, 
	[PROJ_DEFINITION] varchar(24) NULL, 
	[PROJ_OBJ_NUM] varchar(12) NULL, 
	[WBS_ELEMNT_INTERNL] bigint NULL, 
	[WBS_ELEMNT] varchar(24) NULL, 
	[WBS_ELEMNT_OBJ_NUM] varchar(22) NULL, 
	[WBS_ELEMNT_DESC] varchar(40) NULL, 
	[WBS_PROJ_TYP] varchar(2) NULL, 
	[WBS_PROJ_PRIORITY] varchar(1) NULL, 
	[WBS_PROJ_SHORT_DESC] varchar(16) NULL, 
	[WBS_PROJ_PERSONEL_RESPNSBLE] varchar(12) NULL, 
	[WBS_PROJ_SHORT_ID] varchar(16) NULL, 
	[WBS_APPL_NUM] int NULL, 
	[WBS_RESPNSBLE_COST_CNTR] varchar(10) NULL, 
	[WBS_REQUEST_COST_CNTR] varchar(10) NULL, 
	[WBS_REQUEST_CMPNY_CD] varchar(4) NULL, 
	[WBS_BASIC_START_DATE] date NULL, 
	[WBS_BASIC_FINISH_DATE] date NULL, 
	[WBS_FORECAST_START_DATE] date NULL, 
	[WBS_FORECAST_FINISH_DATE] date NULL, 
	[WBS_ACTUAL_START_DATE] date NULL, 
	[WBS_ACTUAL_FINISH_DATE] date NULL, 
	[WBS_BASIC_DUR] int NULL, 
	[WBS_BASIC_UOM] varchar(3) NULL, 
	[WBS_FORECAST_DUR] int NULL, 
	[WBS_FORECAST_UOM] varchar(3) NULL, 
	[WBS_ACTUAL_DUR] int NULL, 
	[WBS_ACTUAL_UOM] varchar(3) NULL, 
	[WBS_BASIC_EARLIST_START_DATE] date NULL, 
	[WBS_BASIC_EARLIST_FINISH_DATE] date NULL, 
	[WBS_BASIC_LATEST_START_DATE] date NULL, 
	[WBS_BASIC_LATEST_FINISH_DATE] date NULL, 
	[WBS_FORECAST_EARLIST_START_DATE] date NULL, 
	[WBS_FORECAST_EARLIST_FINISH_DAT] date NULL, 
	[WBS_FORECAST_LATEST_START_DATE] date NULL, 
	[WBS_FORECAST_LATEST_FINISH_DATE] date NULL, 
	[WBS_TENTATIVE_ACTUAL_START_DATE] date NULL, 
	[WBS_TENTATIVE_ACTUAL_FINISH_DAT] date NULL, 
	[WBS_CLNDR] varchar(2) NULL, 
	[WBS_CONTROLLING_AREA] varchar(4) NULL, 
	[WBS_CMPNY_CD] varchar(4) NULL, 
	[WBS_BIZ_AREA] varchar(4) NULL, 
	[WBS_FUNCTNL_AREA] varchar(16) NULL, 
	[WBS_PROFT_CNTR] varchar(10) NULL, 
	[WBS_OBJ_CLASS] varchar(2) NULL, 
	[WBS_CRNCY] varchar(3) NULL, 
	[WBS_TAX_JRSDCTN] varchar(15) NULL, 
	[WBS_SUB_PROJ] varchar(12) NULL, 
	[WBS_PLNT] varchar(4) NULL, 
	[WBS_LOC] varchar(10) NULL, 
	[WBS_EQUIP] varchar(18) NULL, 
	[WBS_FUNCTNL_LOC] varchar(30) NULL, 
	[WBS_CHG_NUM] varchar(12) NULL, 
	[WBS_REF_ELEMNT] varchar(40) NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[CREATED_ON] date NULL, 
	[CHANGED_BY] varchar(12) NULL, 
	[CHANGED_ON] date NULL, 
	[WBS_LVL_IN_PROJ_HIERCHY] int NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[WBS_RESPNSBLE_PERSON_NAME] varchar(25) NULL, 
	[WBS_APPROPRIATE_PROJ_NUM] int NULL, 
	[WBS_LANG_KEY] varchar(1) NULL, 
	[WBS_PROJ_TYP_DESC] varchar(40) NULL, 
	[WBS_PROJ_PRIORITY_DESC] varchar(40) NULL, 
	[WBS_FACTORY_CLNDR_DESC] varchar(60) NULL, 
	[WBS_CONTROLLING_AREA_DESC] varchar(25) NULL, 
	[WBS_CMPNY_DESC] varchar(25) NULL, 
	[WBS_BIZ_AREA_DESC] varchar(30) NULL, 
	[WBS_OBJ_CLASS_DESC] varchar(60) NULL, 
	[WBS_CRNCY_DESC] varchar(15) NULL, 
	[WBS_PLNT_DESC] varchar(40) NULL, 
	[WBS_POC_WGHT] decimal(8,0) NULL, 
	[WBS_PROGRESS_VRSN] varchar(3) NULL, 
	[WBS_PLAN_MSRMNT_MTHD] varchar(10) NULL, 
	[WBS_PLAN_REF_OBJ] varchar(22) NULL, 
	[WBS_ACTUAL_MSRMNT_MTHD] varchar(10) NULL, 
	[WBS_ACTUAL_REF_OBJ] varchar(22) NULL, 
	[WBS_SYS_STATUS] varchar(100) NULL, 
	[WBS_USR_STATUS] varchar(100) NULL, 
	[PROJ_DESC] varchar(40) NULL, 
	[PROJ_RESPNSBLE_PERSON_NUM] varchar(8) NULL, 
	[PROJ_RESPNSBLE_PERSON_NAME] varchar(25) NULL, 
	[PROJ_CMPNY_CD] varchar(4) NULL, 
	[PROJ_CMPNY_DESC] varchar(25) NULL, 
	[PROJ_PROFT_CNTR] varchar(10) NULL, 
	[PROJ_PLNT_ID] varchar(4) NULL, 
	[PROJ_PLNT_DESC] varchar(40) NULL, 
	[PROJ_FUNCTNL_AREA] varchar(16) NULL, 
	[PROJ_CUST_NUM] varchar(10) NULL, 
	[PROJ_CUST_NAME] varchar(35) NULL, 
	[PROJ_CONTROLLING_AREA] varchar(4) NULL, 
	[PROJ_CONTROLLING_AREA_DESC] varchar(25) NULL, 
	[PROJ_LOC] varchar(10) NULL, 
	[PROJ_CRNCY] varchar(3) NULL, 
	[PROJ_BIZ_AREA] varchar(4) NULL, 
	[PROJ_BIZ_AREA_DESC] varchar(30) NULL, 
	[PROJ_FACTORY_CLNDR] varchar(2) NULL, 
	[PROJ_FACTORY_CLNDR_DESC] varchar(60) NULL, 
	[PROJ_TM_UNIT] varchar(3) NULL, 
	[PROJ_FORECAST_START_DATE] date NULL, 
	[PROJ_FORECAST_FINISH_DATE] date NULL, 
	[PROJ_CREATED_BY] varchar(12) NULL, 
	[PROJ_CREATED_DATE] date NULL, 
	[PROJ_CHANGED_BY] varchar(12) NULL, 
	[PROJ_CHANGED_ON] date NULL, 
	[PROJ_SYS_STATUS] varchar(100) NULL, 
	[PROJ_USR_STATUS] varchar(100) NULL, 
	[PROJ_START_DATE] date NULL, 
	[PROJ_FINISH_DATE] date NULL, 
	[PROJ_LAST_BASIC_SCHLG_DATE] date NULL, 
	[PROJ_LAST_FORECAST_SCHLG_DATE] date NULL
);
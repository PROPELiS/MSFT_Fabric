CREATE TABLE [dbo].[EDW_T_F_PM_WRKORDO_CUR_D] (

	[WRK_ORDER_KEY] int NULL, 
	[WRK_ORDER_ID] varchar(12) NOT NULL, 
	[OPERATION_TASK_LIST_ID] int NOT NULL, 
	[OPERATION_COUNTER_ID] int NOT NULL, 
	[OPERATION_CONFIRM_ID] int NOT NULL, 
	[OPERATION_ACTIVITY_ID] varchar(4) NULL, 
	[OPERATION_SEQ_ID] varchar(6) NOT NULL, 
	[OPERATION_COUNTER] int NOT NULL, 
	[WRK_CNTR_KEY] int NULL, 
	[ORDER_TYP] varchar(4) NULL, 
	[ORDER_CTGRY] int NULL, 
	[CMPNY_CD_KEY] int NOT NULL, 
	[DOCMT_CRNCY_KEY] int NOT NULL, 
	[MAINTENANCE_WRK_CNTR_KEY] int NOT NULL, 
	[RELS_DATE_KEY] int NOT NULL, 
	[TCHNCL_COMPLETION_DATE_KEY] int NOT NULL, 
	[CLS_DATE_KEY] int NOT NULL, 
	[PROFT_CNTR_KEY] int NOT NULL, 
	[BASIC_START_DATE_KEY] int NOT NULL, 
	[BASIC_FINISH_DATE_KEY] int NOT NULL, 
	[SCHLD_RELS_DATE_KEY] int NOT NULL, 
	[SCHLD_FINISH_DATE_KEY] int NOT NULL, 
	[SCHED_START_DATE_KEY] int NOT NULL, 
	[ACTUAL_START_DATE_KEY] int NOT NULL, 
	[ACTUAL_FINISH_DATE_KEY] int NOT NULL, 
	[ACTUAL_RELS_DATE_KEY] int NOT NULL, 
	[PLND_RELS_DATE_KEY] int NOT NULL, 
	[RESRVTN_KEY] int NOT NULL, 
	[TOTAL_SCRAP_AMT] decimal(13,3) NULL, 
	[TRGT_QTY] decimal(13,3) NULL, 
	[CAPACITY_REQMNTS_ID] int NULL, 
	[EQUIP_KEY] int NOT NULL, 
	[NOTIFICATION_KEY] int NOT NULL, 
	[FUNCTNL_LOC_KEY] int NOT NULL, 
	[START_OF_MALFUNCTION_DATE_KEY] int NOT NULL, 
	[START_OF_MALFUNCTION_TS] datetime2(0) NULL, 
	[END_OF_MALFUNCTION_DATE_KEY] int NOT NULL, 
	[END_OF_MALFUNCTION_TS] datetime2(0) NULL, 
	[BREAKDOWN_DUR_UOM] varchar(3) NULL, 
	[BREAKDOWN_DUR] decimal(16,2) NULL, 
	[CTRL_KEY] varchar(4) NULL, 
	[CTRL_KEY_DESC] varchar(40) NULL, 
	[OBJ_ID] int NULL, 
	[ACTIVITY_TYP_KEY] int NOT NULL, 
	[OBJ_NUM] varchar(22) NULL, 
	[OPERATION_SHORT_TEXT] varchar(40) NULL, 
	[WRK_PRCNTG] int NULL, 
	[PRICE] decimal(12,2) NULL, 
	[WRK_INVOLVED_IN_THE_ACTIVITY] decimal(7,1) NULL, 
	[WRK_UOM] varchar(3) NULL, 
	[NORMAL_DUR_OF_THE_ACTIVITY] decimal(6,1) NULL, 
	[NORMAL_DUR_UOM] varchar(3) NULL, 
	[ACTUAL_WRK] decimal(16,3) NULL, 
	[FORECASTED_WRK_ACTUAL_REMG] decimal(8,1) NULL, 
	[ERLST_SCHLD_START_EXCT_DATE_KEY] int NOT NULL, 
	[ERLST_SCHLD_START_EXCT_TS] datetime2(0) NULL, 
	[LATEST_SCHLD_START_EXCT_DATE_KE] int NOT NULL, 
	[LATEST_SCHLD_START_EXCT_TS] datetime2(0) NULL, 
	[ACTUAL_START_EXCT_DATE_KEY] int NOT NULL, 
	[ACTUAL_START_EXCT_SETUP_TS] datetime2(0) NULL, 
	[ERLST_SCHLD_FINISH_EXCT_DATE_KE] int NOT NULL, 
	[ERLST_SCHLD_FINISH_EXCT_TS] datetime2(0) NULL, 
	[LATEST_SCHLD_FINISH_EXCT_DATE_K] int NOT NULL, 
	[LATEST_SCHLD_FINISH_EXCT_TS] datetime2(0) NULL, 
	[ACTUAL_FINISH_EXCT_DATE_KEY] int NOT NULL, 
	[ACTUAL_FINISH_EXCT_TS] datetime2(0) NULL, 
	[IS_START_CNSTRNT] varchar(1) NULL, 
	[START_CNSTRNT_DATE_KEY] int NOT NULL, 
	[START_CNSTRNT_TS] datetime2(0) NULL, 
	[IS_FINISH_CNSTRNT] varchar(1) NULL, 
	[FINISH_CNSTRNT_DATE_KEY] int NOT NULL, 
	[FINISH_CNSTRNT_TS] datetime2(0) NULL, 
	[SRT_TERM] varchar(10) NULL, 
	[PERSONEL_NUM_KEY] int NULL, 
	[EMPL_RESPNSBLE_KEY] int NULL, 
	[PLNT_KEY] int NULL, 
	[LOC_PLNT_KEY] int NULL, 
	[MAINTENANCE_PLNT_KEY] int NULL, 
	[PLNG_PLNT_KEY] int NULL, 
	[RESPNSBLE_COST_CNTR_KEY] int NULL, 
	[COST_CNTR_KEY] int NULL, 
	[FUNCTNL_AREA_KEY] int NOT NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[IS_RELSD] varchar(1) NULL, 
	[IS_PARTIALLY_CONFIRMED] varchar(1) NULL, 
	[IS_CONFIRMED] varchar(1) NULL, 
	[IS_TECHNICALLY_COMPLETE] varchar(1) NULL, 
	[IS_CREATED] varchar(1) NULL, 
	[IS_DELETION_SET] varchar(1) NULL
);
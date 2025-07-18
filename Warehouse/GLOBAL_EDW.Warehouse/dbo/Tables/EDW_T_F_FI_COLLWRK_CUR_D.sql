CREATE TABLE [dbo].[EDW_T_F_FI_COLLWRK_CUR_D] (

	[KEY_OF_WORKLIST_ITM] varchar(200) NOT NULL, 
	[COLLECTION_SPECIALIST_KEY] int NOT NULL, 
	[COLLECTION_GROUP_KEY] int NOT NULL, 
	[COLLECTION_SEG_KEY] int NOT NULL, 
	[DATE_OF_WORKLIST_ITM_KEY] int NOT NULL, 
	[RUN_ID] varchar(12) NULL, 
	[WORKLIST_ITM_IS_COMPLETED_IND] varchar(1) NULL, 
	[COLLECTION_STRATEGY] varchar(10) NULL, 
	[VRSN_OF_COLLECTION_STRATEGY] int NULL, 
	[VALTN] int NULL, 
	[PRCNTG_VALTN] int NULL, 
	[SEQAL_NUM_OF_STRATEGY] int NULL, 
	[BIZ_PRTNR_KEY] int NULL, 
	[SHORT_NAME_OF_BIZ_PRTNR] varchar(50) NULL, 
	[AR_CUST_KEY] int NULL, 
	[AUTHZN_GROUP] varchar(4) NULL, 
	[DESC_OF_MAIN_CONTACT_PERSON] varchar(50) NULL, 
	[TEL_NUM_OF_CONTACT_PERSON_AT_BI] varchar(30) NULL, 
	[RSLT_OF_CUST_CONTACT] varchar(3) NULL, 
	[LAST_CUST_CONTACT] decimal(15,0) NULL, 
	[START_OF_CALL_TM] decimal(15,0) NULL, 
	[END_OF_CALL_TM] decimal(15,0) NULL, 
	[START_OF_VISIT_TM] decimal(15,0) NULL, 
	[END_OF_VISIT_TM] decimal(15,0) NULL, 
	[OUTSTANDING_AMOUNTS_OF_BIZ_PRTN] decimal(15,2) NULL, 
	[TOTAL_OF_ALL_RECEIVABLES_OF_BIZ] decimal(15,2) NULL, 
	[AMT_TO_BE_COLLECTED] decimal(15,2) NULL, 
	[TOTAL_OF_ALL_ITMS_DUE_FOR_KEY_D] decimal(15,2) NULL, 
	[TOTAL_OF_ALL_OVERDUE_ITMS] decimal(15,2) NULL, 
	[OUTSTANDING_AMT_PROMISED] decimal(15,2) NULL, 
	[TOTAL_OF_ALL_ITMS_INCLUDED_IN_D] decimal(15,2) NULL, 
	[AMT_DUNNED] decimal(15,2) NULL, 
	[AMT_ARRANGED] decimal(15,2) NULL, 
	[AMT_PROMISED_BUT_NOT_PAID] decimal(15,2) NULL, 
	[STRATEGY_CRNCY_KEY] int NULL, 
	[TOTAL_DUE_DATE_AMT_IN_PER_1] decimal(15,2) NULL, 
	[TOTAL_DUE_DATE_AMT_IN_PER_2] decimal(15,2) NULL, 
	[TOTAL_DUE_DATE_AMT_IN_PER_3] decimal(15,2) NULL, 
	[TOTAL_DUE_DATE_AMT_IN_PER_4] decimal(15,2) NULL, 
	[TOTAL_DUE_DATE_AMT_IN_PER_5] decimal(15,2) NULL, 
	[TOTAL_OVEDUE_AMT_IN_PER_1] decimal(15,2) NULL, 
	[TOTAL_OVEDUE_AMT_IN_PER_2] decimal(15,2) NULL, 
	[TOTAL_OVEDUE_AMT_IN_PER_3] decimal(15,2) NULL, 
	[TOTAL_OVEDUE_AMT_IN_PER_4] decimal(15,2) NULL, 
	[TOTAL_OVEDUE_AMT_IN_PER_5] decimal(15,2) NULL, 
	[DATE_OF_LAST_PYMT] date NULL, 
	[RESUBMISSION_DATE] date NULL, 
	[RESUBMISSION_TM] decimal(15,0) NULL, 
	[RSN_FOR_RESUBMISSION] varchar(4) NULL, 
	[HIGHEST_DUNNING_LVL] int NULL, 
	[DATE_OF_LAST_DUNNING_NOTICE] date NULL, 
	[CREDIT_SEG] varchar(10) NULL, 
	[CREDIT_LMT] decimal(15,2) NULL, 
	[SCORE] varchar(10) NULL, 
	[CREDIT_LMT_UTILIZATION_IN_PCT] decimal(9,0) NULL, 
	[RISK_CLASS] varchar(3) NULL, 
	[EXTNL_VALTN] varchar(10) NULL, 
	[EXTNL_VALTN_PROC] varchar(10) NULL, 
	[EXTNL_VALTN_VALID_TO] date NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[CREATED_ON] decimal(15,0) NULL, 
	[LAST_CHANGED_BY] varchar(12) NULL, 
	[LAST_CHANGED_ON] decimal(15,0) NULL, 
	[HEAD_OFFC_ON_THE_WORKLIST] varchar(10) NULL, 
	[OTHR_HEAD_OFFICES_EXIST_IN_WORK] varchar(1) NULL, 
	[GLBL_VALTN] varchar(1) NULL, 
	[AVG_DAYS_LATE] decimal(20,0) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
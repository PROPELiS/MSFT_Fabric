CREATE TABLE [dbo].[EDW_T_F_SAL_INVPRCN_CUR_D] (

	[SALES_ORDER_KEY] int NOT NULL, 
	[INVC_DATE_KEY] int NOT NULL, 
	[CMPNY_KEY] int NOT NULL, 
	[PLNT_KEY] int NOT NULL, 
	[SOLDTO_CUST_KEY] int NOT NULL, 
	[BILLTO_CUST_KEY] int NOT NULL, 
	[MATRL_KEY] int NOT NULL, 
	[PROFT_CNTR_KEY] int NOT NULL, 
	[CUST_CMPNY_KEY] int NOT NULL, 
	[CUST_SALES_AREA_KEY] int NOT NULL, 
	[SEG_KEY] int NOT NULL, 
	[GL_KEY] int NOT NULL, 
	[PROVISION_GL_KEY] int NOT NULL, 
	[ACCT_KEY] varchar(3) NOT NULL, 
	[ACCRUALS_ACCT_KEY] varchar(3) NOT NULL, 
	[DOCMT_COND_HDR_NUM] varchar(10) NOT NULL, 
	[DOCMT_COND_ITM_NUM] int NOT NULL, 
	[STEP_NUM] int NOT NULL, 
	[COUNTER] int NOT NULL, 
	[COND_TYP] varchar(4) NULL, 
	[CALCULATION_TYP] varchar(1) NULL, 
	[COND_CLASS] varchar(1) NULL, 
	[COND_CTGRY] varchar(1) NULL, 
	[COND_RT] decimal(18,3) NULL, 
	[DOCMT_CRNCY_COND_VAL_AMT] decimal(18,3) NULL, 
	[CMPNY_CRNCY_COND_VAL_AMT] decimal(18,3) NULL, 
	[GROUP_CRNCY_COND_VAL_AMT] decimal(18,3) NULL, 
	[COND_EXCHG_RT] decimal(18,3) NULL, 
	[PRICE_UNIT] int NULL, 
	[COND_UOM] varchar(3) NULL, 
	[PARNT_CUST_KEY] int NOT NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[DOCMT_CRNCY_KEY] int NOT NULL, 
	[CMPNY_CRNCY_KEY] int NOT NULL, 
	[GROUP_CRNCY_KEY] int NOT NULL, 
	[INVC_KEY] int NOT NULL, 
	[INVC_ITM_ID] int NOT NULL, 
	[ACCT_KEY_DESC] varchar(20) NULL, 
	[ACCRUALS_ACCT_KEY_DESC] varchar(20) NULL, 
	[CALCULATION_DESC] varchar(60) NULL, 
	[COND_CLASS_DESC] varchar(60) NULL, 
	[COND_CTGRY_DESC] varchar(60) NULL, 
	[INACTV_COND] varchar(1) NULL, 
	[INACTV_COND_DESC] varchar(60) NULL, 
	[ORIGIN_OF_THE_COND] varchar(1) NULL, 
	[ORIGIN_OF_THE_COND_DESC] varchar(60) NULL, 
	[COND_CTRL] varchar(1) NULL, 
	[COND_CTRL_DESC] varchar(60) NULL, 
	[COND_CHANGED_MANUALLY] varchar(1) NULL, 
	[COND_TYP_DESC] varchar(20) NULL, 
	[STATISTICAL_COND_IND] varchar(1) NULL, 
	[ACCESS_NUM] int NULL, 
	[COND_PRICING_DATE_KEY] int NULL
);
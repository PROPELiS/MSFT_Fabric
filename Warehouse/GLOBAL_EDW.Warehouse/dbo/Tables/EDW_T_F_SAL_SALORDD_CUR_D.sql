CREATE TABLE [dbo].[EDW_T_F_SAL_SALORDD_CUR_D] (

	[SALES_ORDER_KEY] int NOT NULL, 
	[SALES_ORDER_ID] varchar(10) NOT NULL, 
	[SALES_ORDER_ITM_ID] varchar(10) NOT NULL, 
	[DOCMT_DATE_KEY] int NOT NULL, 
	[REQD_DLVRY_DATE_KEY] int NOT NULL, 
	[CMPNY_KEY] int NOT NULL, 
	[PLNT_KEY] int NOT NULL, 
	[SOLDTO_CUST_KEY] int NOT NULL, 
	[BILLTO_CUST_KEY] int NOT NULL, 
	[PAYER_CUST_KEY] int NOT NULL, 
	[SHIPTO_CUST_KEY] int NOT NULL, 
	[BRAND_OWNER_CUST_KEY] int NOT NULL, 
	[CUST_CONTACT_KEY] int NOT NULL, 
	[SALES_PERSON_PERSONEL_KEY] int NOT NULL, 
	[PERSON_RESPNSBLE_PERSONEL_KEY] int NOT NULL, 
	[MATRL_KEY] int NOT NULL, 
	[PROFT_CNTR_KEY] int NOT NULL, 
	[CUST_CMPNY_KEY] int NOT NULL, 
	[CUST_SALES_AREA_KEY] int NOT NULL, 
	[SEG_KEY] int NOT NULL, 
	[SALES_AREA_KEY] int NOT NULL, 
	[DOCMT_CRNCY_KEY] int NOT NULL, 
	[CMPNY_CRNCY_KEY] int NOT NULL, 
	[GROUP_CRNCY_KEY] int NOT NULL, 
	[SALES_ORDER_QTY] decimal(18,3) NULL, 
	[SALES_ORDER_UOM] varchar(3) NULL, 
	[DOCMT_CRNCY_NET_PRICE] decimal(18,3) NULL, 
	[DOCMT_CRNCY_NET_VAL_AMT] decimal(18,3) NULL, 
	[DOCMT_CRNCY_TAX_AMT] decimal(18,3) NULL, 
	[CMPNY_CRNCY_NET_VAL_AMT] decimal(18,3) NULL, 
	[GROUP_CRNCY_NET_VAL_AMT] decimal(18,3) NULL, 
	[SHIPG_PNT] varchar(4) NULL, 
	[RSN_FOR_REJN] varchar(2) NULL, 
	[OVERLL_STATUS] varchar(1) NULL, 
	[DLVRY_STATUS] varchar(1) NULL, 
	[ORDER_RELD_BILL_STATUS] varchar(1) NULL, 
	[ENCOWAY_ID] varchar(50) NULL, 
	[ENCOWAY_VRSN] varchar(4) NULL, 
	[ENCOWAY_PART] varchar(30) NULL, 
	[ENCOWAY_DESC] varchar(400) NULL, 
	[ENCOWAY_NUM] varchar(120) NULL, 
	[MATRL_DESC] varchar(80) NULL, 
	[DLVRY_GROUP] varchar(3) NULL, 
	[QTY_NETTED] decimal(18,3) NULL, 
	[RSN_FOR_REJN_DESC] varchar(40) NULL, 
	[SHIPG_COND] varchar(2) NULL, 
	[INCO_TRMS] varchar(3) NULL, 
	[INCO_TRMS_DESC] varchar(30) NULL, 
	[INCO_TRMS_2] varchar(28) NULL, 
	[BILLING_DATE] date NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NOT NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[CYLINDER_OWNER_CUST_KEY] int NULL, 
	[USAGE] varchar(3) NULL, 
	[HIGHER_LVL_ITM] int NULL, 
	[ALTERNATIVE_TO_ITM] int NULL, 
	[MATRL_ENTERED_KEY] int NULL, 
	[REF_DOCMT] varchar(10) NULL, 
	[REF_ITM] int NULL, 
	[REF_STATUS] varchar(1) NULL, 
	[TOTAL_REF_STATUS] varchar(1) NULL, 
	[BILLING_BLK] varchar(2) NULL, 
	[PERSON_RESPNSBLE_COST_CNTR_KEY] int NULL, 
	[PARNT_CUST_KEY] int NULL, 
	[CREATION_DATE_KEY] int NULL, 
	[CREATION_TS] datetime2(0) NULL, 
	[DLVRY_STATUS_IND] varchar(1) NULL, 
	[SALES_UOM] varchar(3) NULL, 
	[SALES_TRGT_QTY] decimal(18,3) NULL, 
	[TRGT_QTY_UOM] varchar(3) NULL, 
	[DLVRD_QTY] decimal(18,3) NULL, 
	[IS_SALES_SCHL_LINE_CREATED] varchar(1) NULL, 
	[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY] int NULL, 
	[EMPL_RESPNSBLE_2_KEY] int NULL, 
	[BOM_ITM_NUM] varchar(4) NULL, 
	[CUST_MATRL_INFO_RCD_KEY] int NULL, 
	[CHANGED_ON_DATE] date NULL, 
	[RELEVANT_FOR_BILLING] varchar(1) NULL, 
	[ITM_IS_RELEVANT_FOR_DLVRY] varchar(1) NULL, 
	[WBS_ELEMNT_KEY] int NULL, 
	[AGENCY_CUST_KEY] int NULL, 
	[PRINTER_CUST_KEY] int NULL, 
	[SUPLR_CUST_KEY] int NULL, 
	[BILLING_STATUS_OF_DLVRY] varchar(1) NULL, 
	[APPROVAL] varchar(1) NULL, 
	[ARRANGEMENT] varchar(1) NULL, 
	[DLVRY] varchar(1) NULL, 
	[FIXED_DET] varchar(1) NULL, 
	[RSLTS_ANALYSIS_KEY] varchar(6) NULL, 
	[PURCHS_ORDER_ITM_NUM] varchar(6) NULL, 
	[SALES_ORDER_LINE_ITM_SYS_STATUS] varchar(120) NULL, 
	[OVERLL_STATUS_DESC] varchar(60) NULL, 
	[DLVRY_STATUS_DESC] varchar(60) NULL, 
	[ORDER_RELD_BILL_STATUS_DESC] varchar(60) NULL, 
	[SHIPG_COND_DESC] varchar(20) NULL, 
	[USAGE_DESC] varchar(20) NULL, 
	[REF_STATUS_DESC] varchar(60) NULL, 
	[TOTAL_REF_STATUS_DESC] varchar(60) NULL, 
	[BILLING_BLK_DESC] varchar(40) NULL, 
	[DLVRY_STATUS_IND_DESC] varchar(60) NULL, 
	[RELEVANT_FOR_BILLING_DESC] varchar(60) NULL, 
	[BILLING_STATUS_OF_DLVRY_DESC] varchar(60) NULL, 
	[RSLTS_ANALYSIS_DESC] varchar(40) NULL, 
	[INTERNL_CUST_REF_NUM] varchar(12) NULL, 
	[PRDT_HIERCHY_CD] varchar(18) NULL, 
	[SOLDTO_PURCHS_ORDER_NUM] varchar(35) NULL, 
	[SOLDTO_PURCHS_ORDER_DATE] date NULL, 
	[SHIPTO_PURCHS_ORDER_NUM] varchar(35) NULL, 
	[SHIPTO_PURCHS_ORDER_DATE] date NULL, 
	[CUST_COND_GROUP_1] varchar(2) NULL, 
	[CUST_COND_GROUP_1_DESC] varchar(20) NULL, 
	[CUST_MATRL_NUM] varchar(35) NULL, 
	[EMPL_RESPNSBLE_KEY] int NULL, 
	[CUST_POJECT_MGR_KEY] int NULL, 
	[SALES_ORDER_ITM_CTGRY] varchar(8) NULL, 
	[SALES_ORDER_ITM_CTGRY_DESC] varchar(40) NULL, 
	[SHIP_TO_ADDR_KEY] int NULL, 
	[DLVRY_PRIORITY_CD] varchar(2) NULL, 
	[DLVRY_PRIORITY_DESC] varchar(20) NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[UNITIZATION_AGENT_VNDR_KEY] int NULL, 
	[DESIGNABLE_E_SERVICES_FLG] varchar(1) NULL, 
	[AGREED_DLVRY_TM] varchar(3) NULL, 
	[DECEASED_NAME] varchar(35) NULL, 
	[CUST_PURCHS_ORDER_TYP] varchar(4) NULL, 
	[CUST_PURCHS_ORDER_TYP_DESC] varchar(20) NULL, 
	[ORDER_KEY] int NULL, 
	[PRICE_GROUP] varchar(2) NULL, 
	[PRICE_GROUP_DESC] varchar(20) NULL, 
	[CMLT_CONFIRMED_QTY_IN_BASE_UNIT] decimal(18,3) NULL, 
	[BASE_UOM] varchar(3) NULL
);
CREATE TABLE [dbo].[SAU_T_F_SAL_INVITEM_CUR_D] (

	[CMPNY_KEY] int NOT NULL, 
	[INVC_KEY] int NOT NULL, 
	[INVC_ITM_ID] varchar(10) NOT NULL, 
	[INVC_DATE_KEY] int NOT NULL, 
	[PLNT_KEY] int NOT NULL, 
	[SOLDTO_CUST_KEY] int NOT NULL, 
	[BILLTO_CUST_KEY] int NOT NULL, 
	[PAYER_CUST_KEY] int NOT NULL, 
	[SHIPTO_CUST_KEY] int NOT NULL, 
	[BRANDOWNER_CUST_KEY] int NOT NULL, 
	[CUST_CONTACT_KEY] int NOT NULL, 
	[SALES_PERSON_PERSONEL_KEY] int NOT NULL, 
	[PERSON_RESPNSBLE_PERSONEL_KEY] int NOT NULL, 
	[MATRL_KEY] int NOT NULL, 
	[SEG_KEY] int NOT NULL, 
	[CUST_CMPNY_KEY] int NOT NULL, 
	[CUST_SALES_AREA_KEY] int NOT NULL, 
	[SALES_ORDER_KEY] int NOT NULL, 
	[SALES_ORDER_ITM_ID] varchar(10) NULL, 
	[DOCMT_CRNCY_KEY] int NOT NULL, 
	[CMPNY_CRNCY_KEY] int NOT NULL, 
	[GROUP_CRNCY_KEY] int NOT NULL, 
	[INVC_PYMT_REQUIRED_DATE] datetime2(0) NULL, 
	[DOCMT_CRNCY_INVC_AMT] decimal(18,3) NULL, 
	[CMPNY_CRNCY_INVC_AMT] decimal(18,3) NULL, 
	[GROUP_CRNCY_INVC_AMT] decimal(18,3) NULL, 
	[INVOICED_QTY] decimal(15,3) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[PRDT_VARIANT_CONFIG_KEY] int NULL, 
	[PROFT_CNTR_KEY] int NULL, 
	[CYLINDER_OWNER_CUST_KEY] int NULL, 
	[PARNT_CUST_KEY] int NULL
);
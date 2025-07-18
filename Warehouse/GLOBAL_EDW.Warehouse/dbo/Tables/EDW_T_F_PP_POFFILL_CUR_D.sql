CREATE TABLE [dbo].[EDW_T_F_PP_POFFILL_CUR_D] (

	[CMPNY_KEY] int NULL, 
	[VNDR_KEY] int NULL, 
	[INVOICING_VNDR_KEY] int NULL, 
	[DOCMT_DATE_KEY] int NULL, 
	[STATISTICAL_DATE_KEY] int NULL, 
	[DLVRY_DATE_KEY] int NULL, 
	[LAST_GOODS_RCPT_DATE_KEY] int NULL, 
	[LAST_INVC_RCPT_DATE_KEY] int NULL, 
	[MATRL_KEY] int NULL, 
	[PLNT_KEY] int NULL, 
	[DOCMT_CRNCY_KEY] int NULL, 
	[CMPNY_CRNCY_KEY] int NULL, 
	[QTY_PURCHS_ORDER] decimal(15,3) NULL, 
	[PURCHS_ORDER_UOM] varchar(3) NULL, 
	[DOCMT_CRNCY_AMT] decimal(15,2) NULL, 
	[CMPNY_CRNCY_AMT] decimal(15,2) NULL, 
	[GROUP_CRNCY_AMT] decimal(15,2) NULL, 
	[QTY_REQZ] decimal(15,3) NULL, 
	[QTY_GOODS_RCPT_TOTAL] decimal(15,3) NULL, 
	[CMPNY_CRNCY_GOODS_RCPT_AMT] decimal(15,2) NULL, 
	[GROUP_CRNCY_GOODS_RCPT_AMT] decimal(15,2) NULL, 
	[QTY_INVC_RCPT_TOTAL] decimal(15,3) NULL, 
	[CMPNY_CRNCY_INVC_RCPT_AMT] decimal(15,2) NULL, 
	[GROUP_CRNCY_INVC_RCPT_AMT] decimal(15,2) NULL, 
	[IS_PURCHS_ORDER_OPEN] varchar(1) NULL, 
	[DAYS_OPEN] int NULL, 
	[PO_DAYS_EARLY] int NULL, 
	[PO_DAYS_LATE] int NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[PURCHS_REQZ_ITM_KEY] int NULL, 
	[GROUP_CRNCY_KEY] int NULL, 
	[TRGT_QTY] decimal(15,3) NULL, 
	[QTY_OPEN_GOODS_RCPT_TOTAL] decimal(15,3) NULL, 
	[PURCHS_ORDER_ID] varchar(10) NOT NULL, 
	[PURCHS_ORDER_ITM_ID] varchar(5) NOT NULL, 
	[PURCHS_ORDER_ITM_KEY] int NULL, 
	[IS_PRIN_PURCHS_AGMT_ITM_OPEN] varchar(1) NULL, 
	[DOCMT_CRNCY_TRGT_VAL_FOR_PUR_AG] decimal(15,3) NULL, 
	[CMPNY_CRNCY_TRGT_VAL_FOR_PUR_AG] decimal(15,3) NULL, 
	[GROUP_CRNCY_TRGT_VAL_FOR_PUR_AG] decimal(15,3) NULL, 
	[CMPNY_CRNCY_INVC_VAL_ENTERED] decimal(15,2) NULL, 
	[GROUP_CRNCY_INVC_VAL_ENTERED] decimal(15,2) NULL, 
	[VNDR_CMPNY_KEY] int NULL
);
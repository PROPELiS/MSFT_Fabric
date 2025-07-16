CREATE TABLE [dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] (

	[CUST_MATRL_INFO_RCD_KEY] int NOT NULL, 
	[SALES_ORGZTN] varchar(4) NULL, 
	[DISTRBN_CHNL] varchar(2) NULL, 
	[CUST_ID] varchar(10) NULL, 
	[MATRL_ID] varchar(18) NULL, 
	[CUST_MATRL_ID] varchar(35) NULL, 
	[BASE_UOM] varchar(3) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[CUST_DESC] varchar(40) NULL
);
CREATE TABLE [dbo].[EDW_T_D_MST_CUSTCMP_CUR_D] (

	[CUST_CMPNY_KEY] int NOT NULL, 
	[CUST_ID] varchar(10) NULL, 
	[CMPNY_CD] varchar(4) NULL, 
	[LGCY_CUST_ID] varchar(10) NULL, 
	[PLNT_ID] varchar(4) NULL, 
	[PYMT_TRMS_CD] varchar(4) NULL, 
	[PYMT_BLK] varchar(1) NULL, 
	[ACCTNG_CLERK] varchar(2) NULL, 
	[ACCT_AT_CUST] varchar(12) NULL, 
	[PSTNG_BLK_FOR_CMPNY_CD] varchar(1) NULL, 
	[DELETION_FLG_FOR_CMPNY_CD] varchar(1) NULL, 
	[RECON_ACCT] varchar(10) NULL, 
	[PER_ACCT_STMT_FLG] varchar(1) NULL, 
	[LOCKBOX] varchar(7) NULL, 
	[SRT_KEY] varchar(3) NULL, 
	[PYMT__MTHD] varchar(10) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[CMPNY_DESC] varchar(25) NULL, 
	[PYMT_TRMS_DESC] varchar(50) NULL, 
	[PLNT_DESC] varchar(30) NULL, 
	[ACCTNG_CLERK_DESC] varchar(30) NULL, 
	[PSTNG_BLK_DESC] varchar(60) NULL, 
	[STMT_DESC] varchar(50) NULL, 
	[PYMT_BLK_DESC] varchar(20) NULL, 
	[SRT_DESC] varchar(30) NULL, 
	[HEAD_OFFC_ACCT_NUM] varchar(10) NULL, 
	[ALTERNATIVE_PAYER_ACCT_NUM] varchar(10) NULL, 
	[CLEARING_BW_CUST_AND_VNDR_IND] varchar(1) NULL, 
	[COLLECTIVE_INVC_VARIANT] varchar(1) NULL, 
	[DUNNING_NOTICE_GROUPING_KEY] varchar(2) NULL, 
	[DUNNING_NOTICE_GROUPING_DESC] varchar(50) NULL, 
	[BUYING_GROUP_ACCT_NUM] varchar(10) NULL, 
	[PYMT_MTHD_DESC] varchar(30) NULL, 
	[CREATED_ON] date NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[DELETION_BOCK_FOR_MAST_RCD] varchar(1) NULL, 
	[PERSONEL_NUM] int NULL, 
	[LOCKBOX_KEY] varchar(7) NULL, 
	[SHORT_KEY_FOR_A_HOUSE_BANK] varchar(5) NULL
);
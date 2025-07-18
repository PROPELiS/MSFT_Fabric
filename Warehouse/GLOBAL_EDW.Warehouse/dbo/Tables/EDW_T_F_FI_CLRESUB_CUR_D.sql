CREATE TABLE [dbo].[EDW_T_F_FI_CLRESUB_CUR_D] (

	[RESUBMISSION_KEY] varchar(32) NOT NULL, 
	[BIZ_PRTNR_KEY] int NULL, 
	[COLLECTION_SEG_KEY] int NULL, 
	[RESUBMISSION_DATE] date NULL, 
	[RESUBMISSION_TM] decimal(15,0) NULL, 
	[NO_CUST_CONTACT_UNTIL_RESUBMISS] varchar(1) NULL, 
	[RSN_FOR_RESUBMISSION] varchar(4) NULL, 
	[STATUS_OF_RESUBMISSION] varchar(1) NULL, 
	[CREATED_BY] varchar(12) NULL, 
	[CREATED_ON_TS] decimal(15,0) NULL, 
	[LAST_CHANGED_BY] varchar(12) NULL, 
	[LAST_CHANGED_ON_TS] decimal(15,0) NULL, 
	[NUM_OF_LINKED_ATTACHMENTS] int NULL, 
	[OBJ_TYP] varchar(10) NULL, 
	[OBJ_KEY] varchar(70) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[STATUS_OF_RESUBMISSION_DESC] varchar(60) NULL, 
	[COLLECTION_SPECIALIST_KEY] int NULL, 
	[COLLECTION_GROUP_KEY] int NULL
);
CREATE TABLE [Propelis].[GROUP_CURRENCY] (

	[CRNCY_KEY] int NOT NULL, 
	[Group Code Currency] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Group Currency Short Description] varchar(max) NULL, 
	[Group Currency Long Description] varchar(max) NULL
);
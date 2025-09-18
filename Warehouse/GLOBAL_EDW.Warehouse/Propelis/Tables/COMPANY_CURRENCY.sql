CREATE TABLE [Propelis].[COMPANY_CURRENCY] (

	[CRNCY_KEY] int NOT NULL, 
	[Company Currency] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Company Currency Short Description] varchar(max) NULL, 
	[Company Currency Long Description] varchar(max) NULL
);
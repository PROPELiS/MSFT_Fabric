CREATE TABLE [Propelis].[DOCUMENT_CURRENCY] (

	[CRNCY_KEY] int NOT NULL, 
	[Document Currency Code] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[Document Currency Short Description] varchar(max) NULL, 
	[Document Currency Long Description] varchar(max) NULL
);
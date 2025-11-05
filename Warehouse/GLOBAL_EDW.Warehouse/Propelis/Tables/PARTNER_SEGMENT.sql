CREATE TABLE [Propelis].[PARTNER_SEGMENT] (

	[SEG_KEY] int NOT NULL, 
	[Partner Segment Name] varchar(max) NULL, 
	[Partner Segment] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
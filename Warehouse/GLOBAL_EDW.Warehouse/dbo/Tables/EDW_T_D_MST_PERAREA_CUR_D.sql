CREATE TABLE [dbo].[EDW_T_D_MST_PERAREA_CUR_D] (

	[PERSONEL_AREA_KEY] int NOT NULL, 
	[Personnel Area] varchar(max) NULL, 
	[Country Grouping] varchar(max) NULL, 
	[Company Code] varchar(max) NULL, 
	[Personnel Area Text] varchar(max) NULL, 
	[House Number] varchar(max) NULL, 
	[PO Box] varchar(max) NULL, 
	[City] varchar(max) NULL, 
	[Country] varchar(max) NULL, 
	[Region] varchar(max) NULL, 
	[County] varchar(max) NULL, 
	[Address] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
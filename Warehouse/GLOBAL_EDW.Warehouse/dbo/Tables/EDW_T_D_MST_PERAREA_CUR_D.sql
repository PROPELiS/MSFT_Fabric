CREATE TABLE [dbo].[EDW_T_D_MST_PERAREA_CUR_D] (

	[PERSONEL_AREA_KEY] int NOT NULL, 
	[PERSONEL_AREA] varchar(4) NULL, 
	[CNTRY_GROUPING] varchar(2) NULL, 
	[CMPNY_CD] varchar(4) NULL, 
	[PERSONEL_AREA_TEXT] varchar(30) NULL, 
	[HOUSE_NUM] varchar(30) NULL, 
	[PO_BOX] varchar(10) NULL, 
	[CITY] varchar(25) NULL, 
	[CNTRY] varchar(3) NULL, 
	[REGN] varchar(3) NULL, 
	[CNTY] varchar(3) NULL, 
	[ADDR] varchar(10) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
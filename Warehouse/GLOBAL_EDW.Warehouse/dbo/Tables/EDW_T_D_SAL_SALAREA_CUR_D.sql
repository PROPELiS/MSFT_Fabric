CREATE TABLE [dbo].[EDW_T_D_SAL_SALAREA_CUR_D] (

	[SALES_AREA_KEY] int NOT NULL, 
	[Sales Area Sales Organization] varchar(4) NULL, 
	[Sales Area Sales Organization Name] varchar(35) NULL, 
	[Sales Area Distribution Channel] varchar(2) NULL, 
	[Distribution Channel Name] varchar(35) NULL, 
	[Sales Area Division] varchar(2) NULL, 
	[Sales Area Division Name] varchar(35) NULL, 
	[Sales Area Created Date] date NULL, 
	[Sales Area Created User ID] varchar(12) NULL, 
	[Sales Area Updated Date] date NULL, 
	[Sales Area Updated User ID] varchar(12) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[BIZ_SEG] varchar(35) NULL
);
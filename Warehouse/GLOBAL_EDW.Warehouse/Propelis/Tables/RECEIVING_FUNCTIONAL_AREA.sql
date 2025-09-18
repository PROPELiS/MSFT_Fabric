CREATE TABLE [Propelis].[RECEIVING_FUNCTIONAL_AREA] (

	[FUNCTNL_AREA_KEY] int NULL, 
	[Receiving Functional Area ID] varchar(max) NULL, 
	[Receiving Functional Area Description] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
CREATE TABLE [dbo].[EDW_T_D_MST_SEGMENT_CUR_D] (

	[SEG_KEY] int NOT NULL, 
	[Segment ID] varchar(10) NULL, 
	[Segment Code Description] varchar(40) NULL, 
	[Segment Business Unit Code] varchar(10) NULL, 
	[Segment Business Unit Code Description] varchar(40) NULL, 
	[Segment Created Date] date NULL, 
	[Segment Created User ID] varchar(12) NULL, 
	[Segment Updated Date] date NULL, 
	[Segment Updated User ID] varchar(12) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
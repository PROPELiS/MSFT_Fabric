CREATE TABLE [dbo].[EDW_T_D_HY_COSTEVZ_CUR_D] (

	[COST_ELEMNT_KEY] int NULL, 
	[CONTROLLING_AREA_ID] varchar(4) NULL, 
	[PARNT_ID] varchar(12) NULL, 
	[PARNT_DESC] varchar(64) NULL, 
	[CHILD_ID] varchar(12) NULL, 
	[CHILD_DESC] varchar(64) NULL, 
	[LVL] varchar(24) NULL, 
	[CHART_OF_ACCTS] varchar(4) NULL
);
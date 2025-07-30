CREATE TABLE [Propelis].[COST_CENTER_VERTICAL_HIERARCHY] (

	[COST_CNTR_KEY] int NULL, 
	[CONTROLLING_AREA_ID] varchar(4) NULL, 
	[PARNT_ID] varchar(12) NULL, 
	[PARNT_DESC] varchar(64) NULL, 
	[CHILD_ID] varchar(12) NULL, 
	[CHILD_DESC] varchar(64) NULL, 
	[LVL] varchar(24) NULL
);
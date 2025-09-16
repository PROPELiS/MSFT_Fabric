CREATE TABLE [Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR] (

	[COST_CNTR_KEY] int NULL, 
	[Person Resp Cost Center Controlling Area ID VH] varchar(4) NULL, 
	[Person Resp Cost Center Parent ID VH] varchar(12) NULL, 
	[Person Resp Cost Center Parent Description VH] varchar(64) NULL, 
	[Person Resp Cost Center Child ID VH] varchar(12) NULL, 
	[Person Resp Cost Center Child Description VH] varchar(64) NULL, 
	[Person Resp Cost Center Hierarchical Level VH] varchar(24) NULL
);
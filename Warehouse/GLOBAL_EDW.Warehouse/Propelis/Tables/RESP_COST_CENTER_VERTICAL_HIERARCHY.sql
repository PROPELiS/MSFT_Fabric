CREATE TABLE [Propelis].[RESP_COST_CENTER_VERTICAL_HIERARCHY] (

	[COST_CNTR_KEY] int NULL, 
	[Resp Cost Center Controlling Area ID VH] varchar(4) NULL, 
	[Resp Cost Center Parent ID VH] varchar(12) NULL, 
	[Resp Cost Center Parent Description VH] varchar(64) NULL, 
	[Resp Cost Center Child ID VH] varchar(12) NULL, 
	[Resp Cost Center Child Description VH] varchar(64) NULL, 
	[Resp Cost Center Hierarchical Level VH] varchar(24) NULL
);
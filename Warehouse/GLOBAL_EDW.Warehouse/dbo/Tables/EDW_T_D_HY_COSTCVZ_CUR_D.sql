CREATE TABLE [dbo].[EDW_T_D_HY_COSTCVZ_CUR_D] (

	[COST_CNTR_KEY] int NULL, 
	[Cost Center Controlling Area ID VH] varchar(4) NULL, 
	[Cost Center Parent ID VH] varchar(12) NULL, 
	[Cost Center Parent Description VH] varchar(64) NULL, 
	[Cost Center Child ID VH] varchar(12) NULL, 
	[Cost Center Child Description VH] varchar(64) NULL, 
	[Cost Center Hierarchical Level VH] varchar(24) NULL
);
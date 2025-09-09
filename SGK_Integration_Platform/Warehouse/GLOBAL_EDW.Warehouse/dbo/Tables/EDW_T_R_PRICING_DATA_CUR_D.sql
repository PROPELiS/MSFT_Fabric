CREATE TABLE [dbo].[EDW_T_R_PRICING_DATA_CUR_D] (

	[region] varchar(max) NULL, 
	[sales org] varchar(max) NULL, 
	[sales org name] varchar(max) NULL, 
	[sold to] varchar(max) NULL, 
	[sold to customer name] varchar(max) NULL, 
	[parent number] varchar(max) NULL, 
	[parent name] varchar(max) NULL, 
	[infinity order adj] decimal(28,6) NULL, 
	[include in hubx] varchar(max) NULL, 
	[green] decimal(28,6) NULL, 
	[yellow] decimal(28,6) NULL, 
	[red] decimal(28,6) NULL, 
	[notes] varchar(max) NULL
);
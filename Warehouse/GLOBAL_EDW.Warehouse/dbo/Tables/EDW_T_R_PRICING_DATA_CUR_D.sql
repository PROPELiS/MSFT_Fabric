CREATE TABLE [dbo].[EDW_T_R_PRICING_DATA_CUR_D] (

	[REGION] varchar(255) NULL, 
	[SALES_ORG] varchar(255) NULL, 
	[SALES_ORG_NAME] varchar(255) NULL, 
	[SOLD_TO] varchar(255) NULL, 
	[SOLD_TO_CUSTOMER_NAME] varchar(255) NULL, 
	[PARENT_NUMBER] varchar(255) NULL, 
	[PARENT_NAME] varchar(255) NULL, 
	[INFINITY_ORDER_ADJ] decimal(28,6) NULL, 
	[INCLUDE_IN_HUBX] varchar(255) NULL, 
	[GREEN] decimal(28,6) NULL, 
	[YELLOW] decimal(28,6) NULL, 
	[RED] decimal(28,6) NULL, 
	[NOTES] varchar(255) NULL
);
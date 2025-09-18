CREATE TABLE [Propelis].[ORDER_FOR_CUSTOMER] (

	[ADDR_KEY] int NOT NULL, 
	[Order for Cust Address Number] varchar(max) NULL, 
	[Order for Cust Valid From Date] date NULL, 
	[Order for Cust International Address Version ID] varchar(max) NULL, 
	[Order for Customer Name] varchar(max) NULL, 
	[Order for Cust Street] varchar(max) NULL, 
	[Order for Cust City] varchar(max) NULL, 
	[Order for Cust Region] varchar(max) NULL, 
	[Order for Cust City Postal Code] varchar(max) NULL, 
	[Order for Cust Country Key] varchar(max) NULL, 
	[Order for Cust Language Key] varchar(max) NULL, 
	[Order for Cust Telephone Number] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
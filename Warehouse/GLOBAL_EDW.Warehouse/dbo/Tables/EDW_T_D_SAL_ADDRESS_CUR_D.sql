CREATE TABLE [dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] (

	[ADDR_KEY] int NOT NULL, 
	[Address Number] varchar(max) NULL, 
	[Valid From Date] date NULL, 
	[International Address Version ID] varchar(max) NULL, 
	[Customer Name] varchar(max) NULL, 
	[Cust Street] varchar(max) NULL, 
	[Cust City] varchar(max) NULL, 
	[Order for Cust Region] varchar(max) NULL, 
	[Cust City Postal Code] varchar(max) NULL, 
	[Cust Country Key] varchar(max) NULL, 
	[Cust Language Key] varchar(max) NULL, 
	[Cust Telephone Number] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
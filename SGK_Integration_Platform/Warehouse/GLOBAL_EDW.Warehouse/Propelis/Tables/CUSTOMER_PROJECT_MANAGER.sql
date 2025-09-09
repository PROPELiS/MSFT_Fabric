CREATE TABLE [Propelis].[CUSTOMER_PROJECT_MANAGER] (

	[CUST_CONTACT_KEY] int NOT NULL, 
	[Customer Project Manager Contact Person ID] varchar(max) NULL, 
	[Customer Project Manager Customer ID] varchar(max) NULL, 
	[Customer Project Manager First Name] varchar(max) NULL, 
	[Customer Project Manager Name] varchar(max) NULL, 
	[Customer Project Manager Contact Person Dept] varchar(max) NULL, 
	[Customer Project Manager Contact Person Dept Name] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(max) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(max) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
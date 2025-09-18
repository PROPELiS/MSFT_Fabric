CREATE TABLE [dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] (

	[HubX Service Order ID] varchar(12) NOT NULL, 
	[HubX Sales Order ID] varchar(10) NULL, 
	[HubX Job ID] varchar(20) NOT NULL, 
	[HubX Project Name] varchar(255) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL, 
	[HubX ZLP BB Color Profile] varchar(255) NULL, 
	[HubX Milestone9 In Date] datetime2(0) NULL
);
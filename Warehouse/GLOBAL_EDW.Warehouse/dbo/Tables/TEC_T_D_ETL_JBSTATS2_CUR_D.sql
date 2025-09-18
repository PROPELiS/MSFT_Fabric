CREATE TABLE [dbo].[TEC_T_D_ETL_JBSTATS2_CUR_D] (

	[BODS Job ID] varchar(max) NOT NULL, 
	[Job Name] varchar(max) NOT NULL, 
	[Start Datetime] datetime2(0) NOT NULL, 
	[End Datetime] datetime2(0) NULL, 
	[Job Group Name] varchar(max) NULL, 
	[Total Time Duration] int NULL, 
	[Batch Date] date NULL, 
	[Job Status] varchar(max) NULL, 
	[Job Status Code] varchar(max) NULL
);
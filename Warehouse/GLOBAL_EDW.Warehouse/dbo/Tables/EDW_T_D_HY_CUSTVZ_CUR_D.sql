CREATE TABLE [dbo].[EDW_T_D_HY_CUSTVZ_CUR_D] (

	[CUST_ID] varchar(10) NOT NULL, 
	[PARNT_ID] varchar(10) NULL, 
	[PARNT_DESC] varchar(60) NULL, 
	[CHILD_ID] varchar(10) NULL, 
	[CHILD_DESC] varchar(60) NULL, 
	[LVL] int NULL
);
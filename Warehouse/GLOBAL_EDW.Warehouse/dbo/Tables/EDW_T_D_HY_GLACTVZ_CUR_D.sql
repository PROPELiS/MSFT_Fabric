CREATE TABLE [dbo].[EDW_T_D_HY_GLACTVZ_CUR_D] (

	[HIERCHY_GROUP] varchar(20) NULL, 
	[GL_CD] varchar(10) NULL, 
	[CHART_OF_ACCTS] varchar(10) NULL, 
	[FUNCTNL_AREA] varchar(16) NULL, 
	[PARNT_ID] varchar(50) NULL, 
	[PARNT_DESC] varchar(100) NULL, 
	[CHILD_ID] varchar(50) NULL, 
	[CHILD_DESC] varchar(100) NULL, 
	[LVL] int NULL, 
	[PARNT_NODEID] varchar(6) NULL, 
	[PARNT_IOBJNM] varchar(10) NULL, 
	[PARNT_NODENAME] varchar(60) NULL, 
	[CHILD_NODEID] varchar(6) NULL, 
	[CHILD_IOBJNM] varchar(10) NULL, 
	[CHILD_NODENAME] varchar(60) NULL
);
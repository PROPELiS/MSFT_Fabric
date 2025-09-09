CREATE TABLE [dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] (

	[Product Hierarchy] varchar(max) NOT NULL, 
	[Product Hierarchy Description] varchar(max) NULL, 
	[Product Hier Level6 Product Category Code] varchar(max) NULL, 
	[Product Hier Level6 Product Category Description] varchar(max) NULL, 
	[Product Hier Level5 Product Category Code] varchar(max) NULL, 
	[Product Hier Level5 Product Category Description] varchar(max) NULL, 
	[Product Hier Level4 Product Category Code] varchar(max) NULL, 
	[Product Hier Level4 Product Category Description] varchar(max) NULL, 
	[Product Hier Level3 Sub Family Code] varchar(max) NULL, 
	[Product Hier Level3 Sub Family Description] varchar(max) NULL, 
	[Product Hier Level2 Family Code] varchar(max) NULL, 
	[Product Hier Level2 Family Description] varchar(max) NULL, 
	[Product Hier Level1 Class Code] varchar(max) NULL, 
	[Product Hier Level1 Class Description] varchar(max) NULL, 
	[ETL_SRC_SYS_CD] varchar(10) NULL, 
	[ETL_EFFECTV_BEGIN_DATE] date NULL, 
	[ETL_EFFECTV_END_DATE] date NULL, 
	[ETL_CURR_RCD_IND] varchar(1) NULL, 
	[ETL_CREATED_TS] datetime2(0) NULL, 
	[ETL_UPDTD_TS] datetime2(0) NULL
);
CREATE  PROCEDURE [Propelis].[Proc_EDW_T_FACT_CO_COPABGT_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_COPABGT_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_COPABGT_CUR_D](
	    [COPA Currency Type],
        [COPA Currency Type Description],
        [COPA Record Type],
        [COPA Version],
        [COPA Profit Segment],
        [COPA Subnumber],
        [COPA Document Number],
        [COPA Fiscal Year/Period Block],
        [COPA Created On Date],
        [COPA Created By],
        [COPA Fiscal Year],
        [SOLDTO_CUST_KEY],
        [MATRL_KEY],
        [FOREIGN_CRNCY_KEY],
        [CRNCY_KEY],
        [CUST_CMPNY_KEY],
        [COPA Controlling Area],
        [COPA Controlling Area Description],
        [PLNT_KEY],
        [SALES_AREA_KEY],
        [CUST_SALES_AREA_KEY],
        [PROFT_CNTR_KEY],
        [COPA Time Created],
        [INDUS_KEY],
        [COPA Industry Description],
        [COPA Sales District],
        [COPA Sales District Description],
        [COPA Customer Group],
        [COPA Customer Group Description],
        [COPA Country Key],
        [COPA Customer Group 2],
        [COPA Customer Group 2 Description],
        [COPA Material Group],
        [COPA Material Group Description],
        [COPA Postal Code],
        [COPA Region],
        [COPA Sales Office],
        [COPA Sales Office Description],
        [COPA Sales Group],
        [COPA Sales Group Description],
        [COPA Customer Group Assignment Group],
        [COPA Cust Group Assignment Group Description],
        [COPA City Coordinates],
        [COPA Commission Group],
        [COPA Commission Group Description],
        [COPA Product Hierarchy Code],
        [COPA Industry Code 1],
        [COPA Industry Code 1 Description],
        [COPA Industry Code 2],
        [COPA Industry Code 2 Description],
        [COPA Accounting Clerk],
        [COPA Acctounting Clerk Description],
        [COPA Sub Brand],
        [COPA Base UoM],
        [SALES_QTY],
        [REVNU_AMT],
        [LABOR_COST_AMT],
        [MATRL_COST_AMT],
        [OVERHEAD_COST_AMT],
        [OUTSIDE_PURCHASES_AMT],
        [MATRL_SALES_KEY],
        [CMPNY_KEY],
        [COPA Sales Organization],
        [COPA Sales Organization Description],
        [COPA Distribution Channel],
        [COPA Distribution Channel Description],
        [COPA Division],
        [COPA Division Description],
        [COPA Brand],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [COPA Base UoM Description],
        [GENERAL_AND_ADMIN_AMT],
        [SELLING_EXPENSE_AMT],
        [MCHINE_COSTS_AMT],
        [SEG_KEY],
		
	--Below columns are added for the Alias tables
		
        [PROFCHZ_PROFT_CNTR_KEY],
        [PROFCVZ_PROFT_CNTR_KEY]
	)
	SELECT
	    FACT.[CRNCY_TYP],
        FACT.[CRNCY_TYP_DESC],
        FACT.[RCD_TYP],
        FACT.[VRSN],
        FACT.[PROFT_SEG],
        FACT.[SUBNUMBER],
        FACT.[DOCMT_NUM],
        FACT.[FISCAL_YR_PER_BLK],
        FACT.[CREATED_ON_DATE],
        FACT.[CREATED_BY],
        FACT.[FISCAL_YR],
        FACT.[SOLDTO_CUST_KEY],
        FACT.[MATRL_KEY],
        FACT.[FOREIGN_CRNCY_KEY],
        FACT.[CRNCY_KEY],
        FACT.[CUST_CMPNY_KEY],
        FACT.[CONTROLLING_AREA],
        FACT.[CONTROLLING_AREA_DESC],
        FACT.[PLNT_KEY],
        FACT.[SALES_AREA_KEY],
        FACT.[CUST_SALES_AREA_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[TM_CREATED],
        FACT.[INDUS_KEY],
        FACT.[INDUS_DESC],
        FACT.[SALES_DISTR],
        FACT.[SALES_DISTR_DESC],
        FACT.[CUST_GROUP],
        FACT.[CUST_GROUP_DESC],
        FACT.[CNTRY_KEY],
        FACT.[CUST_GROUP_2],
        FACT.[CUST_GROUP_2_DESC],
        FACT.[MATRL_GROUP],
        FACT.[MATRL_GROUP_DESC],
        FACT.[POSTAL_CD],
        FACT.[REGN],
        FACT.[SALES_OFFC],
        FACT.[SALES_OFFC_DESC],
        FACT.[SALES_GROUP],
        FACT.[SALES_GROUP_DESC],
        FACT.[CUST_ACCT_ASSIGN_GROUP],
        FACT.[CUST_ACCT_ASSIGN_GROUP_DESC],
        FACT.[CITY_COORDINATES],
        FACT.[COMM_GROUP],
        FACT.[COMM_GROUP_DESC],
        FACT.[PRDT_HIERCHY_CD],
        FACT.[INDUS_CD_1],
        FACT.[INDUS_CD_1_DESC],
        FACT.[INDUS_CD_2],
        FACT.[INDUS_CD_2_DESC],
        FACT.[ACCTNG_CLERK],
        FACT.[ACCTNG_CLERK_DESC],
        FACT.[SUB_BRAND],
        FACT.[BASE_UOM],
        FACT.[SALES_QTY],
        FACT.[REVNU_AMT],
        FACT.[LABOR_COST_AMT],
        FACT.[MATRL_COST_AMT],
        FACT.[OVERHEAD_COST_AMT],
        FACT.[OUTSIDE_PURCHASES_AMT],
        FACT.[MATRL_SALES_KEY],
        FACT.[CMPNY_KEY],
        FACT.[SALES_ORGZTN],
        FACT.[SALES_ORG_DESC],
        FACT.[DISTRBN_CHNL],
        FACT.[DISTRBN_CHNL_DESC],
        FACT.[DIVISION],
        FACT.[DIVISION_DESC],
        FACT.[BRAND],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[BASE_UOM_DESC],
        FACT.[GENERAL_AND_ADMIN_AMT],
        FACT.[SELLING_EXPENSE_AMT],
        FACT.[MCHINE_COSTS_AMT],
        FACT.[SEG_KEY],
		
	--Below columns are added for the Alias tables
	
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
		PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY
	
    FROM[GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_CO_COPABGT_CUR_D] AS FACT
	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[FOREIGN_CURRENCY] AS FRNG_CURNCY
	    ON FACT.FOREIGN_CRNCY_KEY = FRNG_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[CURRENCY] AS CRNCY
	    ON FACT.CRNCY_KEY = CRNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
	    ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCMP_CUR_D] AS CUSTCMP
	    ON FACT.CUST_CMPNY_KEY = CUSTCMP.CUST_CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
	    ON FACT.PLNT_KEY = PLANT.PLNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS SALAREA
	    ON FACT.SALES_AREA_KEY = SALAREA.SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTSAL_CUR_D] AS CUSTSAL
	    ON FACT.CUST_SALES_AREA_KEY = CUSTSAL.CUST_SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATSALS_CUR_D] AS MATSALS
	    ON FACT.MATRL_SALES_KEY = MATSALS.MATRL_SALES_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
	    ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D] AS CUSTOMR
	    ON FACT.SOLDTO_CUST_KEY = CUSTOMR.CUST_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] AS PRTHRCY
	    ON FACT.PRDT_HIERCHY_CD = PRTHRCY.[Product Hierarchy]
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
	    ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] AS SEGMENT
	    ON FACT.SEG_KEY = SEGMENT.SEG_KEY;
END
CREATE  PROCEDURE [Propelis].[Proc_EDW_T_FACT_CO_CCCSUMM_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_CCCSUMM_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_CCCSUMM_CUR_D](
	    [Summary Ledger],
        [Summary Object Number],
        [Summary Year],
        [Summary Value Type],
        [Summary Version],
        [COST_CNTR_KEY],
        [Summary CO Subkey],
        [Summary Business Transaction],
        [Summary Trading Partner],
        [Summary Trading Partner Business Area],
        [Summary Debit Credit Indicator],
        [DOCMT_CRNCY_KEY],
        [Summary Period Block],
        [CMPNY_KEY],
        [FUNCTNL_AREA_KEY],
        [SEG_KEY],
        [COST_ELEMNT_KEY],
        [PROFT_CNTR_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [DOCMT_CRNCY_AMT],
        [CMPNY_CRNCY_AMT],
        [GROUP_CRNCY_AMT],
        [Summary Period],
        [Summary Partner Object Type],
        [Summary Partner Object Type Description],
        [Summary Value Type Description],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Summary Partner Object],
        [Summary Source Object],
        [Summary Ledger Description],
        [Summary Trading Partner Description],
        [Summary Trading Partner Business Area Desc],
        [CONTROLLING_AREA_COST_ELEMNT_KE],
		
	--Below columns are added for the Alias tables	
		
        [COSTEHZ_COST_ELEMNT_KEY],
        [COSTEVZ_COST_ELEMNT_KEY],
        [PROFCHZ_PROFT_CNTR_KEY],
        [PROFCVZ_PROFT_CNTR_KEY],
        [COSTCHZ_COST_CNTR_KEY],
        [COSTCVZ_COST_CNTR_KEY]
    )
	SELECT
	    FACT.[LDGR],
        FACT.[OBJ_NUM],
        FACT.[YR],
        FACT.[VAL_TYP],
        FACT.[VRSN],
        FACT.[COST_CNTR_KEY],
        FACT.[CO_SUBKEY],
        FACT.[BIZ_TRNSCTN],
        FACT.[TRADING_PRTNR],
        FACT.[TRADING_PRTNR_BIZ_AREA],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[PER_BLK],
        FACT.[CMPNY_KEY],
        FACT.[FUNCTNL_AREA_KEY],
        FACT.[SEG_KEY],
        FACT.[COST_ELEMNT_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[DOCMT_CRNCY_AMT],
        FACT.[CMPNY_CRNCY_AMT],
        FACT.[GROUP_CRNCY_AMT],
        FACT.[PER],
        FACT.[PRTNR_OBJ_TYP],
        FACT.[PRTNR_OBJ_TYP_DESC],
        FACT.[VAL_TYP_DESC],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[PRTNR_OBJ],
        FACT.[SRC_OBJ],
        FACT.[LDGR_DESC],
        FACT.[TRADING_PRTNR_DESC],
        FACT.[TRADING_PRTNR_BIZ_AREA_DESC],
        FACT.[CONTROLLING_AREA_COST_ELEMNT_KE],
		
	--Below columns are added for the Alias tables
	
	    COSTELM.COST_ELEMNT_KEY AS COSTEHZ_COST_ELEMNT_KEY,
		COSTELM.COST_ELEMNT_KEY AS COSTEVZ_COST_ELEMNT_KEY,
		PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
		PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY,
		COSCNTR.COST_CNTR_KEY AS COSTCHZ_COST_CNTR_KEY,
		COSCNTR.COST_CNTR_KEY AS COSTCVZ_COST_CNTR_KEY
     
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_CO_CCCSUMM_CUR_D] AS FACT
	 
	LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS CMPY_CURNCY
	    ON FACT.CMPNY_CRNCY_KEY = CMPY_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GRP_CURNCY
	    ON FACT.GROUP_CRNCY_KEY = GRP_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] AS DOC_CURNCY
	    ON FACT.DOCMT_CRNCY_KEY = DOC_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
	    ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] AS FUNCTAR
	    ON FACT.FUNCTNL_AREA_KEY = FUNCTAR.FUNCTNL_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] AS COSTELM
	    ON FACT.COST_ELEMNT_KEY = COSTELM.COST_ELEMNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] AS COSCNTR
	    ON FACT.COST_CNTR_KEY = COSCNTR.COST_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
	    ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] AS SEGMEN2
	    ON FACT.SEG_KEY = SEGMEN2.SEG_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CACSELM_CUR_D] AS CACSELM
	    ON FACT.CONTROLLING_AREA_COST_ELEMNT_KE = CACSELM.CONTROLLING_AREA_COST_ELEMNT_KE;
END
CREATE     PROCEDURE [Propelis].[Proc_EDW_T_FACT_FI_GENLDGT2_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
    TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_GENLDGT2_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_GENLDGT2_CUR_D]
    (
        [Fiscal Year],
        [Table Group Object Number 00],
        [Table Group Object Number 01],
        [Table Group Object Number 03],
        [Table Group Object Number 04],
        [Table Group Object Number 02],
        [Table Group Object Number 05],
        [Table Group Object Number 06],
        [Table Group Object Number 07],
        [Table Group Object Number 08],
        [Max Period],
        [Debit Credit Indicator],
        [Version],
        [Version Description],
        [Controlling Area],
        [Controlling Area Description],
        [Ledger],
        [Ledger Description],
        [DOCMT_CRNCY_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [Record Type],
        [Record Type Description],
        [CMPNY_KEY],
        [PRTNR_PROFT_CNTR_KEY],
        [FUNCTNL_AREA_KEY],
        [PROFT_CNTR_KEY],
        [GL_KEY],
        [TRNSCTN_CRNCY_AMT],
        [CMPNY_CRNCY_AMT],
        [GROUP_CRNCY_AMT],
        [Posting Period],
        [SEG_KEY],
        [TRADING_PRTNR_CMPNY_KEY],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Functional Area ID],
        [General Ledger Code],
	    
	    --Below columns are added for the Alias tables
		
	    [PRTNR_PRFT_CNTR_V_HIER_KEY],
	    [PRTNR_PRFT_CNTR_H_HIER_KEY],
	    [PROFCHZ_PROFT_CNTR_KEY],
	    [PROFCVZ_PROFT_CNTR_KEY],
        [GLACTHZ_KEY]
    )
    SELECT 
        FACT.[FISCAL_YR],
        FACT.[TBL_GROUP_OBJ_NUM_00],
        FACT.[TBL_GROUP_OBJ_NUM_01],
        FACT.[TBL_GROUP_OBJ_NUM_03],
        FACT.[TBL_GROUP_OBJ_NUM_04],
        FACT.[TBL_GROUP_OBJ_NUM_02],
        FACT.[TBL_GROUP_OBJ_NUM_05],
        FACT.[TBL_GROUP_OBJ_NUM_06],
        FACT.[TBL_GROUP_OBJ_NUM_07],
        FACT.[TBL_GROUP_OBJ_NUM_08],
        FACT.[MAX_PER],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[VRSN],
        FACT.[VRSN_DESC],
        FACT.[CONTROLLING_AREA],
        FACT.[CONTROLLING_AREA_DESC],
        FACT.[LDGR],
        FACT.[LDGR_DESC],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[RCD_TYP],
        FACT.[RCD_TYP_DESC],
        FACT.[CMPNY_KEY],
        FACT.[PRTNR_PROFT_CNTR_KEY],
        FACT.[FUNCTNL_AREA_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[GL_KEY],
        FACT.[TRNSCTN_CRNCY_AMT],
        FACT.[CMPNY_CRNCY_AMT],
        FACT.[GROUP_CRNCY_AMT],
        FACT.[PSTNG_PER],
        FACT.[SEG_KEY],
        FACT.[TRADING_PRTNR_CMPNY_KEY],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[FUNCTNL_AREA_ID],
        FACT.[GL_CD],
	    
	    --Below columns are added for the Alias tables
		
	    PNTR_PFT_CNTR.PROFT_CNTR_KEY AS PRTNR_PRFT_CNTR_V_HIER_KEY,
	    PNTR_PFT_CNTR.PROFT_CNTR_KEY AS PRTNR_PRFT_CNTR_H_HIER_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY,
        CONCAT(FACT.FUNCTNL_AREA_ID, '-', FACT.GL_CD) AS [GLACTHZ_KEY]
	
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_FI_GENLDGT2_CUR_D] AS FACT
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Transaction_Currency] AS TRAN_CRNCY
        ON FACT.DOCMT_CRNCY_KEY = TRAN_CRNCY.CRNCY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GROP_CRNCY
        ON FACT.GROUP_CRNCY_KEY = GROP_CRNCY.CRNCY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS CMPNY_CRNCY
        ON FACT.CMPNY_CRNCY_KEY = CMPNY_CRNCY.CRNCY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER] AS PNTR_PFT_CNTR
        ON FACT.PRTNR_PROFT_CNTR_KEY = PNTR_PFT_CNTR.PROFT_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Trading_Partner_Company] AS TRAD_PNTR_CMPNY
        ON FACT.TRADING_PRTNR_CMPNY_KEY = TRAD_PNTR_CMPNY.CMPNY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
        ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] AS FUNCTAR
        ON FACT.FUNCTNL_AREA_KEY = FUNCTAR.FUNCTNL_AREA_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
        ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] AS GENLDGR
        ON FACT.GL_KEY = GENLDGR.GL_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] AS SEGMEN2
        ON FACT.SEG_KEY = SEGMEN2.SEG_KEY   
    
    LEFT JOIN (SELECT MAX(FUNCTNL_AREA) AS FUNCTNL_AREA,GL_CD FROM 
       [GLOBAL_EDW].[dbo].[EDW_T_D_HY_GLACTHZ_CUR_D] GROUP BY GL_CD) AS GLACTHZ_FUNC
        ON FACT.FUNCTNL_AREA_ID = GLACTHZ_FUNC.FUNCTNL_AREA
        AND FACT.GL_CD = GLACTHZ_FUNC.GL_CD;
END
CREATE       PROCEDURE [Propelis].[Proc_EDW_V_FACT_FI_GENLDGR_CUR_D]
AS
BEGIN
    
	-- Step 1: Clear existing data from the target table
	
    TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_V_FACT_FI_GENLDGR_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_V_FACT_FI_GENLDGR_CUR_D] (
        [CMPNY_KEY],
        [GL Fiscal Year],
        [GL Document Number],
        [GL Ledger],
        [GL Document Line Item],
        [GL Controlling Area],
        [TRNSCTN_CRNCY_AMT],
        [CMPNY_CRNCY_AMT],
        [GROUP_CRNCY_AMT],
        [TRNSCTN_CRNCY_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [GL Item Category],
        [GL Fiscal Period],
        [GL Document Line Item ID],
        [GL Clearing Fiscal Year],
        [GL Clearing Item],
        [CUST_KEY],
        [VNDR_KEY],
        [GL Reason Code for Payments],
        [GL Reference Date for Settlement],
        [GL Business Partner Reference Key],
        [GL Business Partner Reference Key 2],
        [GL Special GL Transaction Type],
        [GL Special GL Indicator],
        [GL Transaction Type],
        [GL Value Date],
        [GL Assignment Number],
        [GL Item Text],
        [QTY],
        [GL Quantity UOM],
        [FUNCTNL_AREA_KEY],
        [AMT_FOR_UPDATING_IN_GL],
        [GL Update Currency],
        [GL Is Open Item Management],
        [GL Tax Jurisdiction],
        [GL Tax On Sales Or Purchases],
        [GL Tax Type],
        [TRADING_PRTNR_CMPNY_KEY],
        [WBS_ELEMNT_KEY],
        [GL Account Type],
        [PRTNR_FUNCTNL_AREA_KEY],
        [COST_CNTR_KEY],
        [COST_ELEMNT_KEY],
        [GL Reference Key for Line Item],
        [GL Payment Reference],
        [PLNT_KEY],
        [CLEARING_DATE_KEY],
        [GL Clearing Document Number],
        [PROFT_CNTR_KEY],
        [SEG_KEY],
        [GL Debit/Credit Indicator],
        [GL Posting Key],
        [GL Invoice Document Number],
        [GL_KEY],
        [MATRL_KEY],
        [PRTNR_PROFT_CNTR_KEY],
        [PRTNR_SEG_KEY],
        [GL Document Type],
        [GL Ledger in GL Accounting],
        [DOCMT_DATE_KEY],
        [GL Document Creation Date],
        [GL Document Creation Time],
        [GL Document Creation User Name],
        [PSTNG_DATE_KEY],
        [GL Business Transaction],
        [GL Document Status],
        [GL Reference Document Number],
        [GL Reference Key],
        [GL Reference Transaction],
        [BILLING_DOCMT_KEY],
        [GL Order Number],
        [GL Purchase Order Item ID],
        [PURCHS_ORDER_ITM_KEY],
        [GL Sales Order Item ID],
        [SALES_ORDER_KEY],
        [LCL_CRNCY_3_KEY],
        [GL Parked Document User Name],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [GL Assignment Number for Special Accounts],
        [CMPNY_CRNCY_TAX_AMT],
        [DOCMT_CRNCY_TAX_AMT],
        [GL Ledger Description],
        [GL Controlling Area Description],
        [GL Item Category Description],
        [GL Reason Code Description],
        [GL Special GI Indicator Description],
        [GL Transaction Type Description],
        [GL Tax Type Description],
        [GL Account Type Description],
        [GL Posting Key Description],
        [GL Document Type Description],
        [GL Document Status Description],
        [GL Timestamp],
        [GL Base UoM Description],
        [DOCMT_CRNCY_KEY],
        [DOCMT_CRNCY_AMT],
        [GL_CMPNY_KEY],
        [GL Transaction Code],
        [ASSET_KEY],
        [GL Asset Transaction Type],
        [GL Asset Transaction Type Description],
        [GL Document Line Item In BSEG],
	    
	    --Below columns are added for the Alias tables
		
	    [PRFT_CNTR_TRD_PRTNR_GRP_ID],
	    [PRTNR_PRFT_CNTR_TRD_PRTNR_GRP_ID],
	    [PRTNR_PRFT_CNTR_V_HIER_KEY],
	    [PRTNR_PRFT_CNTR_H_HIER_KEY],
	    [COSTCHZ_COST_CNTR_KEY],
	    [COSTCVZ_COST_CNTR_KEY],
	    [PROFCHZ_PROFT_CNTR_KEY],
	    [PROFCVZ_PROFT_CNTR_KEY],
	    [COSTEHZ_COST_ELEMNT_KEY],
	    [COSTEVZ_COST_ELEMNT_KEY],
	    [ICGLGRP_GL_CD],
        [CMPNYDT_KEY]
    )
    SELECT 
        FACT.[CMPNY_KEY],
        FACT.[FISCAL_YR],
        FACT.[DOCMT_NUM],
        FACT.[LDGR],
        FACT.[DOCMT_LINE_ITM],
        FACT.[CONTROLLING_AREA],
        FACT.[TRNSCTN_CRNCY_AMT],
        FACT.[CMPNY_CRNCY_AMT],
        FACT.[GROUP_CRNCY_AMT],
        FACT.[TRNSCTN_CRNCY_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[ITM_CTGRY],
        FACT.[FISCAL_PER],
        FACT.[DOCMT_LINE_ITM_ID],
        FACT.[CLEARING_FISCAL_YR],
        FACT.[CLEARING_ITM],
        FACT.[CUST_KEY],
        FACT.[VNDR_KEY],
        FACT.[RSN_CD_FOR_PAYMENTS],
        FACT.[REF_DATE_FOR_SETTLEMENT],
        FACT.[BIZ_PRTNR_REF_KEY],
        FACT.[BIZ_PRTNR_REF_KEY_2],
        FACT.[SPCL_GL_TRNSCTN_TYP],
        FACT.[SPCL_GL_IND],
        FACT.[TRNSCTN_TYP],
        FACT.[VAL_DATE],
        FACT.[ASSIGN_NUM],
        FACT.[ITM_TEXT],
        FACT.[QTY],
        FACT.[QTY_UOM],
        FACT.[FUNCTNL_AREA_KEY],
        FACT.[AMT_FOR_UPDATING_IN_GL],
        FACT.[UPDT_CRNCY],
        FACT.[IS_OPEN_ITM_MGMNT],
        FACT.[TAX_JRSDCTN],
        FACT.[TAX_ON_SALES_OR_PURCHASES],
        FACT.[TAX_TYP],
        FACT.[TRADING_PRTNR_CMPNY_KEY],
        FACT.[WBS_ELEMNT_KEY],
        FACT.[ACCT_TYP],
        FACT.[PRTNR_FUNCTNL_AREA_KEY],
        FACT.[COST_CNTR_KEY],
        FACT.[COST_ELEMNT_KEY],
        FACT.[REF_KEY_FOR_LINE_ITM],
        FACT.[PYMT_REF],
        FACT.[PLNT_KEY],
        FACT.[CLEARING_DATE_KEY],
        FACT.[CLEARING_DOCMT_NUM],
        FACT.[PROFT_CNTR_KEY],
        FACT.[SEG_KEY],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[PSTNG_KEY],
        FACT.[INVC_DOCMT_NUM],
        FACT.[GL_KEY],
        FACT.[MATRL_KEY],
        FACT.[PRTNR_PROFT_CNTR_KEY],
        FACT.[PRTNR_SEG_KEY],
        FACT.[DOCMT_TYP],
        FACT.[LDGR_IN_GL_ACCTNG],
        FACT.[DOCMT_DATE_KEY],
        FACT.[DOCMT_CREATION_DATE],
        FACT.[DOCMT_CREATION_TM],
        FACT.[DOCMT_CREATION_USR_NAME],
        FACT.[PSTNG_DATE_KEY],
        FACT.[BIZ_TRNSCTN],
        FACT.[DOCMT_STATUS],
        FACT.[REF_DOCMT_NUM],
        FACT.[REF_KEY],
        FACT.[REF_TRNSCTN],
        FACT.[BILLING_DOCMT_KEY],
        FACT.[ORDER_NUM],
        FACT.[PURCHS_ORDER_ITM_ID],
        FACT.[PURCHS_ORDER_ITM_KEY],
        FACT.[SALES_ORDER_ITM_ID],
        FACT.[SALES_ORDER_KEY],
        FACT.[LCL_CRNCY_3_KEY],
        FACT.[PARKED_DOCMT_USR_NAME],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[ASSIGN_NUM_SPCL_GL_ACCTS],
        FACT.[CMPNY_CRNCY_TAX_AMT],
        FACT.[DOCMT_CRNCY_TAX_AMT],
        FACT.[LDGR_DESC],
        FACT.[CONTROLLING_AREA_DESC],
        FACT.[ITM_CTGRY_DESC],
        FACT.[RSN_CD_DESC],
        FACT.[SPCL_GL_IND_DESC],
        FACT.[TRNSCTN_TYP_DESC],
        FACT.[TAX_TYP_DESC],
        FACT.[ACCT_TYP_DESC],
        FACT.[PSTNG_KEY_DESC],
        FACT.[DOCMT_TYP_DESC],
        FACT.[DOCMT_STATUS_DESC],
        FACT.[GL_TIMESTAMP],
        FACT.[BASE_UOM_DESC],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[DOCMT_CRNCY_AMT],
        FACT.[GL_CMPNY_KEY],
        FACT.[TRNSCTN_CD],
        FACT.[ASSET_KEY],
        FACT.[ASSET_TRNSCTN_TYP],
        FACT.[ASSET_TRNSCTN_TYP_DESC],
        FACT.[DOCMT_LINE_ITM_IN_BSEG],
	    
	    --Below columns are added for the Alias tables
		
	    PROFCTR.[Profit Center ID] AS PRFT_CNTR_TRD_PRTNR_GRP_ID,
	    PNTR_PFT_CNTR.[Partner Profit Center ID] AS PRTNR_PRFT_CNTR_TRD_PRTNR_GRP_ID,
	    PNTR_PFT_CNTR.PROFT_CNTR_KEY AS PRTNR_PRFT_CNTR_V_HIER_KEY,
	    PNTR_PFT_CNTR.PROFT_CNTR_KEY AS PRTNR_PRFT_CNTR_H_HIER_KEY,
	    COSCNTR.COST_CNTR_KEY AS COSTCHZ_COST_CNTR_KEY,
	    COSCNTR.COST_CNTR_KEY AS COSTCVZ_COST_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY,
	    COSTELM.COST_ELEMNT_KEY AS COSTEHZ_COST_ELEMNT_KEY,
	    COSTELM.COST_ELEMNT_KEY AS COSTEVZ_COST_ELEMNT_KEY,
	    GENLDGR.GL_CD AS ICGLGRP_GL_CD,
        CONCAT(CMPNYCD.[Company Code],'-',FACT.PSTNG_DATE_KEY) AS CMPNYDT_KEY
	
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_FI_GENLDG4_CUR_D] AS FACT

        LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] AS PSTG_DT
            ON FACT.PSTNG_DATE_KEY = PSTG_DT.DATE_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] AS DCMT_CRNCY
            ON FACT.DOCMT_CRNCY_KEY = DCMT_CRNCY.CRNCY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS CMPNY_CRNCY
            ON FACT.CMPNY_CRNCY_KEY = CMPNY_CRNCY.CRNCY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GRP_CRNCY
            ON FACT.GROUP_CRNCY_KEY = GRP_CRNCY.CRNCY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[CLEARING_DATE] AS CLR_DT
            ON FACT.CLEARING_DATE_KEY = CLR_DT.DATE_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] AS DCMT_DT
            ON FACT.DOCMT_DATE_KEY = DCMT_DT.DATE_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[FUNCTIONAL_AREA] AS FUN_AREA
            ON FACT.FUNCTNL_AREA_KEY = FUN_AREA.FUNCTNL_AREA_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_FUNCTIONAL_AREA] AS PNTR_FUN_AREA
            ON FACT.PRTNR_FUNCTNL_AREA_KEY = PNTR_FUN_AREA.FUNCTNL_AREA_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER] AS PNTR_PFT_CNTR
            ON FACT.PRTNR_PROFT_CNTR_KEY = PNTR_PFT_CNTR.PROFT_CNTR_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[Transaction_Currency] AS TRANS_CRNCY
            ON FACT.TRNSCTN_CRNCY_KEY = TRANS_CRNCY.CRNCY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[Trading_Partner_Company] AS TRD_PNTR_CMPNY
            ON FACT.TRADING_PRTNR_CMPNY_KEY =  TRD_PNTR_CMPNY.CMPNY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_SEGMENT] AS PNTR_SEG
            ON FACT.PRTNR_SEG_KEY = PNTR_SEG.SEG_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] AS GENLDGR
            ON FACT.GL_KEY = GENLDGR.GL_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] AS COSCNTR
            ON FACT.COST_CNTR_KEY = COSCNTR.COST_CNTR_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
            ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D] AS CUSTOMR
            ON FACT.CUST_KEY = CUSTOMR.CUST_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] AS COSTELM
            ON FACT.COST_ELEMNT_KEY = COSTELM.COST_ELEMNT_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
            ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
            ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_VENDOR_CUR_D] AS VENDOR
            ON FACT.VNDR_KEY = VENDOR.VNDR_KEY 
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PP_PURORDL_CUR_D] AS PURORDL
            ON FACT.PURCHS_ORDER_ITM_KEY = PURORDL.PURCHS_ORDER_ITM_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRS_WBSELEM_CUR_D] AS WBSELEM
            ON FACT.WBS_ELEMNT_KEY = WBSELEM.WBS_ELEMNT_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
            ON FACT.PLNT_KEY = PLANT.PLNT_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_INVOICE_CUR_D] AS INVOICE
            ON FACT.BILLING_DOCMT_KEY = INVOICE.INVC_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
            ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] AS SEGMEN2
            ON FACT.SEG_KEY = SEGMEN2.SEG_KEY
        	
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GLCMPCD_CUR_D] AS GLCMPCD
            ON FACT.GL_CMPNY_KEY = GLCMPCD.GL_CMPNY_KEY
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ASSETHR_CUR_D] AS ASSETHR
           ON FACT.ASSET_KEY = ASSETHR.ASSET_KEY;
END
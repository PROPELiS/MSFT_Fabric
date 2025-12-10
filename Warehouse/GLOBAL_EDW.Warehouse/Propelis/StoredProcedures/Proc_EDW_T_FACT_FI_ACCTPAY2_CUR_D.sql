CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_FI_ACCTPAY2_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_ACCTPAY2_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_ACCTPAY2_CUR_D](
        [CMPNY_KEY],
        [AP Fiscal Year],
        [AP Document Header Number],
        [AP Ledger Posting Item Number],
        [AP Ledger Code],
        [VNDR_KEY],
        [AP Payment Method],
        [AP Payment Method Description],
        [AP Document Type Description],
        [AP Posting Description],
        [AP Payment Terms Description],
        [AP Tax Code On Purchases],
        [AP Special GL Accounts Assignment Number],
        [AP Entered By],
        [SEG_KEY],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [AP Special GL Indicator Description],
        [AP Ledger Code Description],
        [AP Payment Terms],
        [CASH_DISCOUNTS_DAY_1],
        [CASH_DISCOUNTS_DAY_2],
        [AP Net Payments Term Period],
        [AP Reference Key Line Item],
        [PROFT_CNTR_KEY],
        [DOCMT_CRNCY_AMT],
        [GROUP_CRNCY_AMT],
        [PSTNG_KEY],
        [AP Debit Credit Indicator],
        [GL_KEY],
        [BSLINE_DUE_DATE_KEY],
        [AP Document Type],
        [AP Reference Number],
        [AP Invoice Reference Number],
        [AP Reason Code],
        [AP Reason Code Description],
        [CMPNY_CRNCY_AMT],
        [DOCMT_DATE_KEY],
        [PSTNG_DATE_KEY],
        [DOCMT_CRNCY_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [AP Posting Period],
        [AP Document Line Item Number],
        [AP Clearing Document Number],
        [CLEARING_DATE_KEY],
        [AP Assignment Number],
        [AP Special GL Transaction Type],
        [AP Special GL Indicator],
	    
	--Below columns are added for the Alias tables	    
	    
        [PROFCHZ_PROFT_CNTR_KEY],
        [PROFCVZ_PROFT_CNTR_KEY ]
)
SELECT 
        FACT.[CMPNY_KEY],
        FACT.[FISCAL_YR],
        FACT.[DOCMT_HDR_NUM],
        FACT.[LDGR_PSTNG_ITM_NUM],
        FACT.[LDGR_CD],
        FACT.[VNDR_KEY],
        FACT.[PYMT_MTHD],
        FACT.[PYMT_MTHD_DESC],
        FACT.[DOCMT_TYP_DESC],
        FACT.[PSTNG_DESC],
        FACT.[TRMS_OF_PYMT_DESC],
        FACT.[TAX_CD_ON_PURCHS],
        FACT.[SPCL_GL_ACCTS_ASSIGN_NUM],
        FACT.[ENTERED_BY],
        FACT.[SEG_KEY],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[SPCL_GL_IND_DESC],
        FACT.[LDGR_CD_DESC],
        FACT.[PYMT_TRMS],
        FACT.[CASH_DISCOUNTS_DAY_1],
        FACT.[CASH_DISCOUNTS_DAY_2],
        FACT.[NET_PAYMENTS_TERM_PER],
        FACT.[REF_KEY_FOR_LINE_ITM],
        FACT.[PROFT_CNTR_KEY],
        FACT.[DOCMT_CRNCY_AMT],
        FACT.[GROUP_CRNCY_AMT],
        FACT.[PSTNG_KEY],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[GL_KEY],
        FACT.[BSLINE_DUE_DATE_KEY],
        FACT.[DOCMT_TYP],
        FACT.[REF_NUM],
        FACT.[INVC_REF_NUM],
        FACT.[RSN_CD],
        FACT.[RSN_CD_DESC],
        FACT.[CMPNY_CRNCY_AMT],
        FACT.[DOCMT_DATE_KEY],
        FACT.[PSTNG_DATE_KEY],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[PSTNG_PER],
        FACT.[DOCMT_LINE_ITM_NUM],
        FACT.[CLEARING_DOCMT_NUM],
        FACT.[CLEARING_DATE_KEY],
        FACT.[ASSIGN_NUM],
        FACT.[SPCL_GL_TRNSCTN_TYP],
        FACT.[SPCL_GL_IND],
		
		--Below columns are added for the Alias tables
		
		PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
		PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY
    
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_FI_ACCTPAY2_CUR_D] AS FACT
	
	LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] AS DOC_DT
	    ON FACT.DOCMT_DATE_KEY = DOC_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] AS PST_DT
	    ON FACT.PSTNG_DATE_KEY = PST_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[CLEARING_DATE] AS CLR_DT
	    ON FACT.CLEARING_DATE_KEY = CLR_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[BASELINE_DUE_DATE] AS BLE_DU_DT
	    ON FACT.BSLINE_DUE_DATE_KEY = BLE_DU_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] AS DOC_CURNCY
	    ON FACT.DOCMT_CRNCY_KEY = DOC_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GRP_CURNCY
	    ON FACT.GROUP_CRNCY_KEY = GRP_CURNCY.CRNCY_KEY
    
	LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS CMP_CURNCY
	    ON FACT.CMPNY_CRNCY_KEY = CMP_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
	    ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_VENDOR_CUR_D] AS VENDOR
	    ON FACT.VNDR_KEY = VENDOR.VNDR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] AS GENLDGR
	    ON FACT.GL_KEY = GENLDGR.GL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS	PROFCTR
	    ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] AS SEGMEN2
	    ON FACT.SEG_KEY = SEGMEN2.SEG_KEY;
END
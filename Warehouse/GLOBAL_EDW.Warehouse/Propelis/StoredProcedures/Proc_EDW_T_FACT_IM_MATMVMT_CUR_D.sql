CREATE     PROCEDURE [Propelis].[Proc_EDW_T_FACT_IM_MATMVMT_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_IM_MATMVMT_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_IM_MATMVMT_CUR_D] (
        [Material Movement Document Number],
        [Material Movement Document Item Number],
        [Material Movement Document Year],
        [SALES_ORDER_KEY],
        [DOCMT_DATE_KEY],
        [PSTNG_DATE_KEY],
        [MATRL_KEY],
        [CMPNY_KEY],
        [PROFT_CNTR_KEY],
        [PLNT_KEY],
        [STOR_LOC_KEY],
        [CUST_KEY],
        [VNDR_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [COST_CNTR_KEY],
        [SALES_ORDER_ITM_ID],
        [Material Movement Type Code],
        [Material Movement Document Type],
        [Material Movement Transaction Type],
        [Material Movement Batch Number],
        [Material Movement Order ID],
        [Material Movement Purchase Order ID],
        [Material Movement Reference Document Number],
        [Material Movement Debit Credit Indicator],
        [CMPNY_CRNCY_AMT],
        [GROUP_CRNCY_AMT],
        [QTY],
        [Material Movement Quantity Unit of Measure],
        [QTY_ENTRY_UNIT],
        [Material Movement Quantity Entry Unit UOM],
        [QTY_ORDER_UNIT],
        [Material Movement Quantity Order Unit UOM],
        [Material Movement Special Stock Indicator],
        [Material Movement Special Stock Description],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [CREATION_DATE_KEY],
        [Material Movement Creation Timestamp],
        [Material Movement Purchase Order Item ID],
        [RECVNG_PLNT_KEY],
        [PURCHS_ORDER_ITM_KEY],
        [Material Movement Line Item Text],
        [PLNT_MATRL_KEY],
        [Material Movement Serial Number],
        [Material Movement Stock Type],
        [Material Movement Stock Type Description],
	    
	    --Below columns are added for the Alias tables
	    
	    [COSTCHZ_COST_CNTR_KEY],
	    [COSTCVZ_COST_CNTR_KEY],
	    [PROFCHZ_PROFT_CNTR_KEY],
	    [PROFCVZ_PROFT_CNTR_KEY]
    )
    SELECT 
        FACT.[MATRL_DOCMT_NUM],
        FACT.[MATRL_DOCMT_ITM_NUM],
        FACT.[MATRL_DOCMT_YR],
        FACT.[SALES_ORDER_KEY],
        FACT.[DOCMT_DATE_KEY],
        FACT.[PSTNG_DATE_KEY],
        FACT.[MATRL_KEY],
        FACT.[CMPNY_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[PLNT_KEY],
        FACT.[STOR_LOC_KEY],
        FACT.[CUST_KEY],
        FACT.[VNDR_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[COST_CNTR_KEY],
        FACT.[SALES_ORDER_ITM_ID],
        FACT.[MOVEMNT_TYP_CD],
        FACT.[DOCMT_TYP],
        FACT.[TRNSCTN_TYP],
        FACT.[BATCH_NUM],
        FACT.[ORDER_ID],
        FACT.[PURCHS_ORDER_ID],
        FACT.[REF_DOCMT_NUM],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[CMPNY_CRNCY_AMT],
        FACT.[GROUP_CRNCY_AMT],
        FACT.[QTY],
        FACT.[QTY_UOM],
        FACT.[QTY_ENTRY_UNIT],
        FACT.[QTY_ENTRY_UNIT_UOM],
        FACT.[QTY_ORDER_UNIT],
        FACT.[QTY_ORDER_UNIT_UOM],
        FACT.[SPCL_STK_CD],
        FACT.[SPCL_STK_DESC],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[CREATION_DATE_KEY],
        FACT.[CREATION_TS],
        FACT.[PURCHS_ORDER_ITM_ID],
        FACT.[RECVNG_PLNT_KEY],
        FACT.[PURCHS_ORDER_ITM_KEY],
        FACT.[LINE_ITM_TEXT],
        FACT.[PLNT_MATRL_KEY],
        FACT.[SERIAL_NUM],
        FACT.[STK_TYP],
        FACT.[STK_TYP_DESC],
	    
	    --Below columns are added for the Alias tables
	    
	    COSCNTR.COST_CNTR_KEY AS COSTCHZ_COST_CNTR_KEY,
	    COSCNTR.COST_CNTR_KEY AS COSTCVZ_COST_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY
	
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_IM_MATMVMT_CUR_D] FACT
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
        ON FACT.PLNT_KEY = PLANT.PLNT_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
        ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_STORLOC_CUR_D] AS STORLOC
        ON FACT.STOR_LOC_KEY = STORLOC.STOR_LOC_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D] AS CUSTOMR
        ON FACT.CUST_KEY = CUSTOMR.CUST_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_VENDOR_CUR_D] AS VENDOR
        ON FACT.VNDR_KEY = VENDOR.VNDR_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
        ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
        ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
        ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] AS COSCNTR
        ON FACT.COST_CNTR_KEY = COSCNTR.COST_CNTR_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLNTMAT_CUR_D] AS PLNTMAT
        ON FACT.PLNT_MATRL_KEY = PLNTMAT.PLNT_MATRL_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS CMPNY_CRNCY
        ON FACT.CMPNY_CRNCY_KEY = CMPNY_CRNCY.CRNCY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] AS DOMT_DT
        ON FACT.DOCMT_DATE_KEY = DOMT_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] AS PSTNG_DT
        ON FACT.PSTNG_DATE_KEY = PSTNG_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GRP_CRNCY
        ON FACT.GROUP_CRNCY_KEY = GRP_CRNCY.CRNCY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CREATION_DATE] AS CRT_DT
        ON FACT.CREATION_DATE_KEY = CRT_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[RECEIVING_PLANT] AS REV_PLNT
        ON FACT.RECVNG_PLNT_KEY = REV_PLNT.PLNT_KEY;
END
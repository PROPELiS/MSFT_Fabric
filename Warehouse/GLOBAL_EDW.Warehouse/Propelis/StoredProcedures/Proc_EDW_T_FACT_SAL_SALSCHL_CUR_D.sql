CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_SAL_SALSCHL_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_SALSCHL_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins

    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_SALSCHL_CUR_D](
        [SALES_ORDER_KEY],
        [Sales Order Item ID],
        [Schedule Item ID],
        [CMPNY_KEY],
        [PLNT_KEY],
        [SEG_KEY],
        [SALES_AREA_KEY],
        [MATRL_KEY],
        [SCHL_ITM_DATE_KEY],
        [MATRL_AVAILABILITY_DATE_KEY],
        [LOADING_DATE_KEY],
        [GOODS_ISSUE_DATE_KEY],
        [Schedule Line Category Code],
        [QTY_ORDERED],
        [QTY_CONFIRMED],
        [QTY_REQUIRED],
        [Quantity Uom],
        [Release for Planning Timestamp],
        [Delivery Block],
        [QTY_COMMITED],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Reason for Rejection],
        [Delivery Block Description],
        [Schedule Line Category Description],
        [Technical Release Timestamp],
        [Sales Order ID]
    )
    SELECT
        FACT.[SALES_ORDER_KEY],
        FACT.[SALES_ORDER_ITM_ID],
        FACT.[SCHL_ITM_ID],
        FACT.[CMPNY_KEY],
        FACT.[PLNT_KEY],
        FACT.[SEG_KEY],
        FACT.[SALES_AREA_KEY],
        FACT.[MATRL_KEY],
        FACT.[SCHL_ITM_DATE_KEY],
        FACT.[MATRL_AVAILABILITY_DATE_KEY],
        FACT.[LOADING_DATE_KEY],
        FACT.[GOODS_ISSUE_DATE_KEY],
        FACT.[SCHL_LINE_CTGRY_CD],
        FACT.[QTY_ORDERED],
        FACT.[QTY_CONFIRMED],
        FACT.[QTY_REQUIRED],
        FACT.[QTY_UOM],
        FACT.[PLNG_REL_TS],
        FACT.[DLVRY_BLK],
        FACT.[QTY_COMMITED],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[RSN_FOR_REJN],
        FACT.[DLVRY_BLK_DESC],
        FACT.[SCHL_LINE_CTGRY_DESC],
        FACT.[TECH_REL_TS],
        FACT.[SALES_ORDER_ID]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_SAL_SALSCHL_CUR_D] FACT
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
       ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
        
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
       ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
       
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] AS SEGMENT
       ON FACT.SEG_KEY = SEGMENT.SEG_KEY
       
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
       ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
       
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
       ON FACT.PLNT_KEY = PLANT.PLNT_KEY
       
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS SALAREA
       ON FACT.SALES_AREA_KEY = SALAREA.SALES_AREA_KEY
       
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SCHL_ITM_DATE] AS SC_IT_DT
       ON FACT.SCHL_ITM_DATE_KEY = SC_IT_DT.DATE_KEY
       
    LEFT JOIN [GLOBAL_EDW].[Propelis].[MATRL_AVAILABILITY_DATE] AS MAT_AVA_DT
       ON FACT.MATRL_AVAILABILITY_DATE_KEY = MAT_AVA_DT.DATE_KEY
       
    LEFT JOIN [GLOBAL_EDW].[Propelis].[LOADING_DATE] AS LNG_DT
       ON FACT.LOADING_DATE_KEY = LNG_DT.DATE_KEY
       
    LEFT JOIN [GLOBAL_EDW].[Propelis].[GOODS_ISSUE_DATE] AS GD_ISS_DT
       ON FACT.GOODS_ISSUE_DATE_KEY = GD_ISS_DT.DATE_KEY;
END
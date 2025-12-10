CREATE   PROCEDURE [Propelis].[Proc_EDW_T_F_PRD_SERVRES_CUR_D]
AS
BEGIN
    
	-- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVRES_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVRES_CUR_D](
        [Service Order],
        [Operation Activity ID],
        [Operation Task List ID],
        [Operation Counter ID],
        [Number of Reservation],
        [DOCMT_CRNCY_KEY],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Quantity Withdrawn],
        [Price in Document Currency],
        [Price Unit],
        [Quantity In Unit Of Entry],
        [Requirement Quantity],
        [Value Withdrawn Amount],
        [PLNT_KEY],
        [Debit Credit Indicator],
        [MOVEMNT_TYP],
        [Fixed Quantity],
        [Requirement Type],
        [Base UoM],
        [Unit Of Entry],
        [Item Number of Reservation],
        [Record Type],
        [REQ_DATE_KEY],
        [GL_KEY],
        [MATRL_KEY],
        [BOM Item Number],
        [SERVC_ORDER_KEY],
		
	--Below columns are added for the Alias tables
	
	    [PLANT_REGION_PLANT]
	
	)
	SELECT 
	    FACT.[SERVC_ORDER],
        FACT.[OPERATION_ACTIVITY_ID],
        FACT.[OPERATION_TASK_LIST_ID],
        FACT.[OPERATION_COUNTER_ID],
        FACT.[NUM_OF_RESRVTN],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[QTY_WTHDRN],
        FACT.[PRICE_IN_DOCMT_CRNCY],
        FACT.[PRICE_UNIT],
        FACT.[QTY_IN_UNIT_OF_ENTRY],
        FACT.[REQ_QTY],
        FACT.[VAL_WTHDRN_AMT],
        FACT.[PLNT_KEY],
        FACT.[DEBIT_CREDIT_IND],
        FACT.[MOVEMNT_TYP],
        FACT.[FIXED_QTY],
        FACT.[REQ_TYP],
        FACT.[BASE_UOM],
        FACT.[UNIT_OF_ENTRY],
        FACT.[ITM_NUM_OF_RESRVTN],
        FACT.[RCD_TYP],
        FACT.[REQ_DATE_KEY],
        FACT.[GL_KEY],
        FACT.[MATRL_KEY],
        FACT.[BOM_ITM_NUM],
        FACT.[SERVC_ORDER_KEY],
	
    --Below columns are added for the Alias tables	
		
		PNT.[Plant ID] AS PLANT_REGION_PLANT
    
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_PRD_SERVRES_CUR_D] AS FACT
	
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRD_SERVORD_CUR_D] AS SERVORD
	    ON FACT.SERVC_ORDER_KEY = SERVORD.SERVC_ORDER_KEY
		
	LEFT JOIN  [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
	    ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY

    LEFT JOIN  [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D] AS CRNCY
	    ON FACT.DOCMT_CRNCY_KEY = CRNCY.CRNCY_KEY
		
	LEFT JOIN  [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] AS GENLDGR
	    ON FACT.GL_KEY = GENLDGR.GL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[REQUIREMENT_DATE] AS REQ_DT
	    ON FACT.REQ_DATE_KEY = REQ_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANT] AS PNT
	    ON FACT.PLNT_KEY = PNT.PLNT_KEY;
END
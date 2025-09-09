CREATE      PROCEDURE [dbo].[Proc_EDW_T_O_PRD_HBPRJNM_CUR_D]
AS
BEGIN
    -- Step 1: Update existing records in target table
    UPDATE T
    SET
                T.[HubX Service Order ID]	= S.[SERVC_ORDER_ID],
                T.[HubX Sales Order ID]	= S.[SALES_ORDER_ID],
                T.[HubX Job ID]	= S.[JOB_ID],
                T.[HubX Project Name]	= S.[PROJ_NAME],
                T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
                T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
                T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
                T.[HubX ZLP BB Color Profile]	= S.[ZLP_BB_COL_PROFILE],
                T.[HubX Milestone9 In Date]	= S.[MILESTONE9_IN_DATE]

    FROM [GLOBAL_EDW].[dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] AS S
        ON T.[HubX Service Order ID] = S.[SERVC_ORDER_ID]
		AND T.[HubX Job ID] = S.[JOB_ID];

    -- Step 2: Insert new records from mirror to target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] (
                 [HubX Service Order ID],
                 [HubX Sales Order ID],
                 [HubX Job ID],
                 [HubX Project Name],
                 [ETL_SRC_SYS_CD],
                 [ETL_CREATED_TS],
                 [ETL_UPDTD_TS],
                 [HubX ZLP BB Color Profile],
                 [HubX Milestone9 In Date]

    )
    SELECT
         S.[SERVC_ORDER_ID],
         S.[SALES_ORDER_ID],
         S.[JOB_ID],
         S.[PROJ_NAME],
         S.[ETL_SRC_SYS_CD],
         S.[ETL_CREATED_TS],
         S.[ETL_UPDTD_TS],
         S.[ZLP_BB_COL_PROFILE],
         S.[MILESTONE9_IN_DATE]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_O_PRD_HBPRJNM_CUR_D] AS T
        ON T.[HubX Service Order ID] = S.[SERVC_ORDER_ID]
		AND T.[HubX Job ID] = S.[JOB_ID]
    WHERE T.[HubX Service Order ID] IS NULL 
	AND T.[HubX Job ID] IS NULL;

END
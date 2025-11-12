CREATE       PROCEDURE [dbo].[Proc_EDW_T_D_MST_COSTELM_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[COST_ELEMNT_KEY]	= S.[COST_ELEMNT_KEY],
        T.[Cost Element ID]	= S.[COST_ELEMNT_ID],
        T.[Cost Element Chart Of Accounts]	= S.[CHART_OF_ACCTS],
        T.[Cost Element Name]	= S.[COST_ELEMNT_NAME],
        T.[Cost Element Description]	= S.[COST_ELEMNT_DESC],
        T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        T.[Current Record Indicator of Cost Element]	= S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
        T.[Cost Element Chart Of Accounts Description]	= S.[CHART_OF_ACCTS_DESC]

 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_COSTELM_CUR_D] AS S
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] (
        [COST_ELEMNT_KEY],
        [Cost Element ID],
        [Cost Element Chart Of Accounts],
        [Cost Element Name],
        [Cost Element Description],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [Current Record Indicator of Cost Element],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Cost Element Chart Of Accounts Description]

)
    SELECT
       S.[COST_ELEMNT_KEY],
       S.[COST_ELEMNT_ID],
       S.[CHART_OF_ACCTS],
       S.[COST_ELEMNT_NAME],
       S.[COST_ELEMNT_DESC],
       S.[ETL_SRC_SYS_CD],
       S.[ETL_EFFECTV_BEGIN_DATE],
       S.[ETL_EFFECTV_END_DATE],
       S.[ETL_CURR_RCD_IND],
       S.[ETL_CREATED_TS],
       S.[ETL_UPDTD_TS],
       S.[CHART_OF_ACCTS_DESC]
    
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_COSTELM_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] AS T
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY]
    WHERE T.[COST_ELEMNT_KEY] IS NULL;
END
CREATE         PROCEDURE [dbo].[Proc_TEC_T_D_ETL_JBSTATS2_CUR_D]
AS
BEGIN
   

    -- 1. UPDATE existing rows
    UPDATE T
    SET
        T.[Start Datetime]      = S.[START_DATETIME],
        T.[End Datetime]        = S.[END_DATETIME],
        T.[Job Group Name]      = S.[JOB_GROUP_NAME],
        T.[Total Time Duration] = S.[TOTAL_TM_DUR],
        T.[Batch Date]          = S.[BATCH_DATE],
        T.[Job Status]          = S.[JOB_STATUS],
        T.[Job Status Code]     = S.[JOB_STATUS_CD]
    FROM [GLOBAL_EDW].[dbo].[TEC_T_D_ETL_JBSTATS2_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[TEC_T_D_ETL_JBSTATS2_CUR_D] AS S
        ON T.[BODS Job ID] = S.[BODS_JOB_ID]
       AND T.[Job Name]    = S.[JOB_NAME];

    -- 2. INSERT new rows
    INSERT INTO [GLOBAL_EDW].[dbo].[TEC_T_D_ETL_JBSTATS2_CUR_D]
    (
        [BODS Job ID],
        [Job Name],
        [Start Datetime],
        [End Datetime],
        [Job Group Name],
        [Total Time Duration],
        [Batch Date],
        [Job Status],
        [Job Status Code]
    )
    SELECT
        S.[BODS_JOB_ID],
        S.[JOB_NAME],
        S.[START_DATETIME],
        S.[END_DATETIME],
        S.[JOB_GROUP_NAME],
        S.[TOTAL_TM_DUR],
        S.[BATCH_DATE],
        S.[JOB_STATUS],
        S.[JOB_STATUS_CD]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[TEC_T_D_ETL_JBSTATS2_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[TEC_T_D_ETL_JBSTATS2_CUR_D] AS T
        ON T.[BODS Job ID] = S.[BODS_JOB_ID]
       AND T.[Job Name]    = S.[JOB_NAME]
    WHERE T.[BODS Job ID] IS NULL;
END;
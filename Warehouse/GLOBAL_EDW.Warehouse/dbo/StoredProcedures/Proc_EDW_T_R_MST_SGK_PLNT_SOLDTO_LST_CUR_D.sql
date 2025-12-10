CREATE   PROCEDURE [dbo].[Proc_EDW_T_R_MST_SGK_PLNT_SOLDTO_LST_CUR_D]
AS
BEGIN
    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[SALES_ORG],
        S.[PLANT],
        S.[CUSTOMER],
        S.[SOLD_TO],
        S.[STATUS],
        HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(S.[SALES_ORG], ''), '|',
                ISNULL(S.[PLANT], ''), '|',
                ISNULL(S.[CUSTOMER], ''), '|',
                ISNULL(S.[SOLD_TO], ''), '|',
                ISNULL(S.[STATUS], '')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_MST_SGK_PLNT_SOLDTO_LST_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [SALES_ORG] = S.[SALES_ORG],
        [PLANT]     = S.[PLANT],
        [CUSTOMER]  = S.[CUSTOMER],
        [SOLD_TO]   = S.[SOLD_TO],
        [STATUS]    = S.[STATUS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_PLNT_SOLDTO_LST_CUR_D] AS T
    INNER JOIN #SourceData AS S
        ON T.[SALES_ORG] = S.[SALES_ORG]
       AND T.[PLANT]     = S.[PLANT]
       AND T.[CUSTOMER]  = S.[CUSTOMER]
       AND T.[SOLD_TO]   = S.[SOLD_TO]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[SALES_ORG], ''), '|',
                ISNULL(T.[PLANT], ''), '|',
                ISNULL(T.[CUSTOMER], ''), '|',
                ISNULL(T.[SOLD_TO], ''), '|',
                ISNULL(T.[STATUS], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_PLNT_SOLDTO_LST_CUR_D] (
        [SALES_ORG],
        [PLANT],
        [CUSTOMER],
        [SOLD_TO],
        [STATUS]
    )
    SELECT 
        S.[SALES_ORG],
        S.[PLANT],
        S.[CUSTOMER],
        S.[SOLD_TO],
        S.[STATUS]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_PLNT_SOLDTO_LST_CUR_D] AS T
        WHERE T.[SALES_ORG] = S.[SALES_ORG]
          AND T.[PLANT]     = S.[PLANT]
          AND T.[CUSTOMER]  = S.[CUSTOMER]
          AND T.[SOLD_TO]   = S.[SOLD_TO]
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';
END;
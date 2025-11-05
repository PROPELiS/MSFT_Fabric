CREATE   PROCEDURE [Propelis].[Proc_Profit_Center_Trading_Partner_Grouping]
AS
BEGIN
    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[PROFT_CNTR_ID],
        S.[TRADING_PRTNR],
       
        HASHBYTES('SHA2_256', CONCAT(
            ISNULL(S.[PROFT_CNTR_ID], ''), '|',
            ISNULL(S.[TRADING_PRTNR], '')
        )) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_FI_ICPTRDG_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [PROFT_CNTR_ID] = S.[PROFT_CNTR_ID],
        [Trading Partner ID] = S.[TRADING_PRTNR]
    FROM [GLOBAL_EDW].[Propelis].[Profit_Center_Trading_Partner_Grouping] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_ID] = S.[PROFT_CNTR_ID]
    WHERE HASHBYTES('SHA2_256', CONCAT(
            ISNULL(T.[PROFT_CNTR_ID], ''), '|',
            ISNULL(T.[Trading Partner ID], '')
        )) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Profit_Center_Trading_Partner_Grouping]
    (
        [PROFT_CNTR_ID],
        [Trading Partner ID]
    )
    SELECT 
        S.[PROFT_CNTR_ID],
        S.[TRADING_PRTNR]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[Profit_Center_Trading_Partner_Grouping] AS T
        WHERE T.[PROFT_CNTR_ID] = S.[PROFT_CNTR_ID]
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';
END;
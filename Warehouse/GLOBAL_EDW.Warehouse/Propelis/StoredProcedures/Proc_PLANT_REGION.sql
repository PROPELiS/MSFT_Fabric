CREATE       PROCEDURE [Propelis].[Proc_PLANT_REGION]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT
        S.[Business Function],
        S.[SGK Plant Region],
        S.[PLANT],
        HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(S.[Business Function], ''), '|',
                ISNULL(S.[SGK Plant Region], ''), '|',
                ISNULL(S.[PLANT], '')
            )
        ) AS HashKey
    INTO #SourceData
 FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [Plant Business Function] = S.[Business Function],
        [Plant Region]            = S.[SGK Plant Region],
        [PLANT]                   = S.[PLANT]
    FROM [GLOBAL_EDW].[Propelis].[PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[PLANT] = S.[PLANT]   -- join on business key (PLANT)
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[Plant Business Function], ''), '|',
                ISNULL(T.[Plant Region], ''), '|',
                ISNULL(T.[PLANT], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PLANT_REGION]
    (
        [Plant Business Function],
        [Plant Region],
        [PLANT]
    )
    SELECT 
        S.[Business Function],
        S.[SGK Plant Region],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PLANT_REGION] AS T
        WHERE T.[PLANT] = S.[PLANT]
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';

END;
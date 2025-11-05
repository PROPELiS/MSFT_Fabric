CREATE   PROCEDURE [Propelis].[Proc_MAINTENANCE_PLANT_REGION]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT],
        HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(S.[BUSINESS_FUNCTION], ''), '|',
                ISNULL(S.[REGION], ''), '|',
                ISNULL(S.[PLANT], '')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [Maintenance Business Function] =  S.[BUSINESS_FUNCTION],
        [Maintenance SGK Plant Region]            = S.[REGION]
    FROM [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[PLANT] = S.[PLANT]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[Maintenance Business Function], ''), '|',
                ISNULL(T.[Maintenance SGK Plant Region], ''), '|',
                ISNULL(T.[PLANT], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT_REGION]
    (
        [Maintenance Business Function],
        [Maintenance SGK Plant Region],
        [PLANT]
    )
    SELECT 
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT_REGION] AS T
        WHERE T.[PLANT] = S.[PLANT]
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';

END;
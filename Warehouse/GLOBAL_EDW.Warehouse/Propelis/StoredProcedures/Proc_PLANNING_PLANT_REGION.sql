CREATE         PROCEDURE [Propelis].[Proc_PLANNING_PLANT_REGION]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT DISTINCT
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_SGK_OPERREG_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [Planning Plant Business Function] = S.[BUSINESS_FUNCTION],
        [Planning Plant Region]            = S.[REGION],
		[Planning Plant]                   = S.[PLANT]
    FROM [GLOBAL_EDW].[Propelis].[PLANNING_PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[Planning Plant] = S.[PLANT]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[Planning Plant Business Function], ''), '|',
                ISNULL(T.[Planning Plant Region], ''), '|',
                ISNULL(T.[Planning Plant], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PLANNING_PLANT_REGION]
    (
        [Planning Plant Business Function],
        [Planning Plant Region],
        [Planning Plant]
    )
    SELECT 
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PLANNING_PLANT_REGION] AS T
        WHERE T.[Planning Plant] = S.[PLANT]
    );

  END;
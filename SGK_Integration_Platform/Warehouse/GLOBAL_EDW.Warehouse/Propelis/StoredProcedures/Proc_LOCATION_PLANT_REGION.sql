CREATE   PROCEDURE [Propelis].[Proc_LOCATION_PLANT_REGION]
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
        [Location Plant Business Function] = S.[BUSINESS_FUNCTION],
        [Location Plant Region]            = S.[REGION],
		[Location Plant]                   = S.[PLANT]
    FROM [GLOBAL_EDW].[Propelis].[LOCATION_PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[Location Plant] = S.[PLANT]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[Location Plant Business Function], ''), '|',
                ISNULL(T.[Location Plant Region], ''), '|',
                ISNULL(T.[Location Plant], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[LOCATION_PLANT_REGION]
    (
        [Location Plant Business Function],
        [Location Plant Region],
        [Location Plant]
    )
    SELECT 
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[LOCATION_PLANT_REGION] AS T
        WHERE T.[Location Plant] = S.[PLANT]
    );

  END;
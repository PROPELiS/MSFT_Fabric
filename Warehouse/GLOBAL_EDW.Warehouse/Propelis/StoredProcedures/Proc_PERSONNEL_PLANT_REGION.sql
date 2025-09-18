CREATE   PROCEDURE [Propelis].[Proc_PERSONNEL_PLANT_REGION]
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
        [Personnel Business Function] = S.[BUSINESS_FUNCTION],
        [Personnel Plant Region]            = S.[REGION],
		[Personnel Plant]                   = S.[PLANT]
    FROM [GLOBAL_EDW].[Propelis].[PERSONNEL_PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[Personnel Plant] = S.[PLANT]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[Personnel Business Function], ''), '|',
                ISNULL(T.[Personnel Plant Region], ''), '|',
                ISNULL(T.[Personnel Plant], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSONNEL_PLANT_REGION]
    (
        [Personnel Business Function],
        [Personnel Plant Region],
        [Personnel Plant]
    )
    SELECT 
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PERSONNEL_PLANT_REGION] AS T
        WHERE T.[Personnel Plant] = S.[PLANT]
    );

  END;
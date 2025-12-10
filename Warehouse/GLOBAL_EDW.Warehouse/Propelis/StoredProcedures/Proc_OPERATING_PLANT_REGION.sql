CREATE   PROCEDURE [Propelis].[Proc_OPERATING_PLANT_REGION]
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_SGK_OPERREG_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Operating Plant Business Function] = S.[BUSINESS_FUNCTION],
        T.[Operating Plant Region]           = S.[REGION],
        T.[Operating Plant]                  = S.[PLANT]
    FROM [GLOBAL_EDW].[Propelis].[OPERATING_PLANT_REGION] AS T
    INNER JOIN #SourceData AS S
        ON T.[Operating Plant] = S.[PLANT]
    WHERE HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Operating Plant Business Function], ''), '|',
                ISNULL(T.[Operating Plant Region], ''), '|',
                ISNULL(T.[Operating Plant], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new records not in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[OPERATING_PLANT_REGION]
    (
        [Operating Plant Business Function],
        [Operating Plant Region],
        [Operating Plant]
    )
    SELECT
        S.[BUSINESS_FUNCTION],
        S.[REGION],
        S.[PLANT]
    FROM #SourceData AS S
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[OPERATING_PLANT_REGION] AS T
        WHERE T.[Operating Plant] = S.[PLANT]
    );

END;
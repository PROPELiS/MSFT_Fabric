CREATE   PROCEDURE [Propelis].[Proc_PROFIT_CENTER_VERTICAL]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[PROFT_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[PARNT_ID],
        S.[PARNT_DESC],
        S.[CHILD_ID],
        S.[CHILD_DESC],
        S.[LVL],

        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(S.[CONTROLLING_AREA_ID], ''), '|',
                ISNULL(S.[PARNT_ID], ''), '|',
                ISNULL(S.[PARNT_DESC], ''), '|',
                ISNULL(S.[CHILD_ID], ''), '|',
                ISNULL(S.[CHILD_DESC], ''), '|',
                ISNULL(S.[LVL], '')
            )
        ) AS HashKey

    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCVZ_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Profit Center Controlling Area ID of VH]     = S.[CONTROLLING_AREA_ID],
        T.[Profit Center Parent ID of VH]               = S.[PARNT_ID],
        T.[Profit Center Parent Description of VH]      = S.[PARNT_DESC],
        T.[Profit Center Child ID of VH]                = S.[CHILD_ID],
        T.[Profit Center Child ID Description of VH]    = S.[CHILD_DESC],
        T.[Profit Center Level of VH]                   = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Profit Center Controlling Area ID of VH], ''), '|',
                ISNULL(T.[Profit Center Parent ID of VH], ''), '|',
                ISNULL(T.[Profit Center Parent Description of VH], ''), '|',
                ISNULL(T.[Profit Center Child ID of VH], ''), '|',
                ISNULL(T.[Profit Center Child ID Description of VH], ''), '|',
                ISNULL(T.[Profit Center Level of VH], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL]
    (
        [PROFT_CNTR_KEY],
        [Profit Center Controlling Area ID of VH],
        [Profit Center Parent ID of VH],
        [Profit Center Parent Description of VH],
        [Profit Center Child ID of VH],
        [Profit Center Child ID Description of VH],
        [Profit Center Level of VH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[PARNT_ID],
        S.[PARNT_DESC],
        S.[CHILD_ID],
        S.[CHILD_DESC],
        S.[LVL]
    FROM #SourceData AS S
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL] AS T
        WHERE T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
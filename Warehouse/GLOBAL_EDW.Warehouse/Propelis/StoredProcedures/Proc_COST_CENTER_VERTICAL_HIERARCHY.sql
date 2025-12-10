CREATE   PROCEDURE [Propelis].[Proc_COST_CENTER_VERTICAL_HIERARCHY]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[COST_CNTR_KEY],
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCVZ_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Resp Cost Center Controlling Area ID VH]    = S.[CONTROLLING_AREA_ID],
        T.[Resp Cost Center Parent ID VH]              = S.[PARNT_ID],
        T.[Resp Cost Center Parent Description VH]     = S.[PARNT_DESC],
        T.[Resp Cost Center Child ID VH]               = S.[CHILD_ID],
        T.[Resp Cost Center Child Description VH]      = S.[CHILD_DESC],
        T.[Resp Cost Center Hierarchical Level VH]     = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Resp Cost Center Controlling Area ID VH], ''), '|',
                ISNULL(T.[Resp Cost Center Parent ID VH], ''), '|',
                ISNULL(T.[Resp Cost Center Parent Description VH], ''), '|',
                ISNULL(T.[Resp Cost Center Child ID VH], ''), '|',
                ISNULL(T.[Resp Cost Center Child Description VH], ''), '|',
                ISNULL(T.[Resp Cost Center Hierarchical Level VH], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY]
    (
        [COST_CNTR_KEY],
        [Resp Cost Center Controlling Area ID VH],
        [Resp Cost Center Parent ID VH],
        [Resp Cost Center Parent Description VH],
        [Resp Cost Center Child ID VH],
        [Resp Cost Center Child Description VH],
        [Resp Cost Center Hierarchical Level VH]
    )
    SELECT
        S.[COST_CNTR_KEY],
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
        FROM [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
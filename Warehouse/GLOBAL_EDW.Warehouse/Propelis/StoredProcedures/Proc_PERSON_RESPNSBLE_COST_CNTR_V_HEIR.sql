CREATE   PROCEDURE [Propelis].[Proc_PERSON_RESPNSBLE_COST_CNTR_V_HEIR]
AS
BEGIN

    ----------------------------------------------------------------------
    -- Step 1: Load source data into temp table with HashKey
    ----------------------------------------------------------------------
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


    ----------------------------------------------------------------------
    -- Step 2: Update rows only where HashKey is different
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Person Resp Cost Center Controlling Area ID VH]   = S.[CONTROLLING_AREA_ID],
        T.[Person Resp Cost Center Parent ID VH]             = S.[PARNT_ID],
        T.[Person Resp Cost Center Parent Description VH]    = S.[PARNT_DESC],
        T.[Person Resp Cost Center Child ID VH]              = S.[CHILD_ID],
        T.[Person Resp Cost Center Child Description VH]     = S.[CHILD_DESC],
        T.[Person Resp Cost Center Hierarchical Level VH]    = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Person Resp Cost Center Controlling Area ID VH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Parent ID VH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Parent Description VH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Child ID VH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Child Description VH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Hierarchical Level VH], '')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert NEW rows (those not found in target)
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR]
    (
        [COST_CNTR_KEY],
        [Person Resp Cost Center Controlling Area ID VH],
        [Person Resp Cost Center Parent ID VH],
        [Person Resp Cost Center Parent Description VH],
        [Person Resp Cost Center Child ID VH],
        [Person Resp Cost Center Child Description VH],
        [Person Resp Cost Center Hierarchical Level VH]
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
        FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
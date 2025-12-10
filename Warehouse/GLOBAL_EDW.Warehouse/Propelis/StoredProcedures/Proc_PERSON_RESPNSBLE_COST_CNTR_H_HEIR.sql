CREATE   PROCEDURE [Propelis].[Proc_PERSON_RESPNSBLE_COST_CNTR_H_HEIR]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data with HashKey into a temp table
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT
        S.[COST_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[LEAF_LVL],
        S.[LVL_0_ID],
        S.[LVL_0_DESC],
        S.[LVL_1_ID],
        S.[LVL_1_DESC],
        S.[LVL_2_ID],
        S.[LVL_2_DESC],
        S.[LVL_3_ID],
        S.[LVL_3_DESC],
        S.[LVL_4_ID],
        S.[LVL_4_DESC],
        S.[LVL_5_ID],
        S.[LVL_5_DESC],
        S.[LVL_6_ID],
        S.[LVL_6_DESC],
        S.[LVL_7_ID],
        S.[LVL_7_DESC],
        S.[LVL_8_ID],
        S.[LVL_8_DESC],

        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(S.[CONTROLLING_AREA_ID], ''), '|',
                ISNULL(S.[LEAF_LVL], ''), '|',
                ISNULL(S.[LVL_0_ID], ''), '|',
                ISNULL(S.[LVL_0_DESC], ''), '|',
                ISNULL(S.[LVL_1_ID], ''), '|',
                ISNULL(S.[LVL_1_DESC], ''), '|',
                ISNULL(S.[LVL_2_ID], ''), '|',
                ISNULL(S.[LVL_2_DESC], ''), '|',
                ISNULL(S.[LVL_3_ID], ''), '|',
                ISNULL(S.[LVL_3_DESC], ''), '|',
                ISNULL(S.[LVL_4_ID], ''), '|',
                ISNULL(S.[LVL_4_DESC], ''), '|',
                ISNULL(S.[LVL_5_ID], ''), '|',
                ISNULL(S.[LVL_5_DESC], ''), '|',
                ISNULL(S.[LVL_6_ID], ''), '|',
                ISNULL(S.[LVL_6_DESC], ''), '|',
                ISNULL(S.[LVL_7_ID], ''), '|',
                ISNULL(S.[LVL_7_DESC], ''), '|',
                ISNULL(S.[LVL_8_ID], ''), '|',
                ISNULL(S.[LVL_8_DESC], '')
            )
        ) AS HashKey

    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S;


    ------------------------------------------------------------------
    -- Step 2: Update records where HashKey does NOT match
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Person Resp Cost Center Controlling Area ID HH]  = S.[CONTROLLING_AREA_ID],
        T.[Person Resp Cost Center Leaf level HH]           = S.[LEAF_LVL],
        T.[LVL_0_ID]                                        = S.[LVL_0_ID],
        T.[LVL_0_DESC]                                      = S.[LVL_0_DESC],
        T.[Person Resp Cost Center Level 1 ID HH]           = S.[LVL_1_ID],
        T.[Person Resp Cost Center Level 1 Description HH]  = S.[LVL_1_DESC],
        T.[Person Resp Cost Center Level 2 ID HH]           = S.[LVL_2_ID],
        T.[Person Resp Cost Center Level 2 Description HH]  = S.[LVL_2_DESC],
        T.[Person Resp Cost Center Level 3 ID HH]           = S.[LVL_3_ID],
        T.[Person Resp Cost Center Level 3 Description HH]  = S.[LVL_3_DESC],
        T.[Person Resp Cost Center Level 4 ID HH]           = S.[LVL_4_ID],
        T.[Person Resp Cost Center Level 4 Description HH]  = S.[LVL_4_DESC],
        T.[Person Resp Cost Center Level 5 ID HH]           = S.[LVL_5_ID],
        T.[Person Resp Cost Center Level 5 Description HH]  = S.[LVL_5_DESC],
        T.[Person Resp Cost Center Level 6 ID HH]           = S.[LVL_6_ID],
        T.[Person Resp Cost Center Level 6 Description HH]  = S.[LVL_6_DESC],
        T.[Person Resp Cost Center Level 7 ID HH]           = S.[LVL_7_ID],
        T.[Person Resp Cost Center Level 7 Description HH]  = S.[LVL_7_DESC],
        T.[Person Resp Cost Center Level 8 ID HH]           = S.[LVL_8_ID],
        T.[Person Resp Cost Center Level 8 Description HH]  = S.[LVL_8_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Person Resp Cost Center Controlling Area ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Leaf level HH], ''), '|',
                ISNULL(T.[LVL_0_ID], ''), '|',
                ISNULL(T.[LVL_0_DESC], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 1 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 1 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 2 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 2 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 3 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 3 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 4 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 4 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 5 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 5 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 6 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 6 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 7 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 7 Description HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 8 ID HH], ''), '|',
                ISNULL(T.[Person Resp Cost Center Level 8 Description HH], '')
            )
        ) <> S.HashKey;


    ------------------------------------------------------------------
    -- Step 3: Insert rows that do NOT exist in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR]
    (
        [COST_CNTR_KEY],
        [Person Resp Cost Center Controlling Area ID HH],
        [Person Resp Cost Center Leaf level HH],
        [LVL_0_ID],
        [LVL_0_DESC],
        [Person Resp Cost Center Level 1 ID HH],
        [Person Resp Cost Center Level 1 Description HH],
        [Person Resp Cost Center Level 2 ID HH],
        [Person Resp Cost Center Level 2 Description HH],
        [Person Resp Cost Center Level 3 ID HH],
        [Person Resp Cost Center Level 3 Description HH],
        [Person Resp Cost Center Level 4 ID HH],
        [Person Resp Cost Center Level 4 Description HH],
        [Person Resp Cost Center Level 5 ID HH],
        [Person Resp Cost Center Level 5 Description HH],
        [Person Resp Cost Center Level 6 ID HH],
        [Person Resp Cost Center Level 6 Description HH],
        [Person Resp Cost Center Level 7 ID HH],
        [Person Resp Cost Center Level 7 Description HH],
        [Person Resp Cost Center Level 8 ID HH],
        [Person Resp Cost Center Level 8 Description HH]
    )
    SELECT
        S.[COST_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[LEAF_LVL],
        S.[LVL_0_ID],
        S.[LVL_0_DESC],
        S.[LVL_1_ID],
        S.[LVL_1_DESC],
        S.[LVL_2_ID],
        S.[LVL_2_DESC],
        S.[LVL_3_ID],
        S.[LVL_3_DESC],
        S.[LVL_4_ID],
        S.[LVL_4_DESC],
        S.[LVL_5_ID],
        S.[LVL_5_DESC],
        S.[LVL_6_ID],
        S.[LVL_6_DESC],
        S.[LVL_7_ID],
        S.[LVL_7_DESC],
        S.[LVL_8_ID],
        S.[LVL_8_DESC]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
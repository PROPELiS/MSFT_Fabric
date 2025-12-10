CREATE   PROCEDURE [Propelis].[Proc_PROFIT_CENTER_HORIZONTAL]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT DISTINCT
        S.[PROFT_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[LEAF_LVL],
        S.[LVL_0_ID],  S.[LVL_0_DESC],
        S.[LVL_1_ID],  S.[LVL_1_DESC],
        S.[LVL_2_ID],  S.[LVL_2_DESC],
        S.[LVL_3_ID],  S.[LVL_3_DESC],
        S.[LVL_4_ID],  S.[LVL_4_DESC],
        S.[LVL_5_ID],  S.[LVL_5_DESC],
        S.[LVL_6_ID],  S.[LVL_6_DESC],
        S.[LVL_7_ID],  S.[LVL_7_DESC],
        S.[LVL_8_ID],  S.[LVL_8_DESC],
        S.[LVL_9_ID],  S.[LVL_9_DESC],

        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(S.[CONTROLLING_AREA_ID], ''), '|',
                ISNULL(S.[LEAF_LVL], ''), '|',
                ISNULL(S.[LVL_0_ID], ''), '|',  ISNULL(S.[LVL_0_DESC], ''), '|',
                ISNULL(S.[LVL_1_ID], ''), '|',  ISNULL(S.[LVL_1_DESC], ''), '|',
                ISNULL(S.[LVL_2_ID], ''), '|',  ISNULL(S.[LVL_2_DESC], ''), '|',
                ISNULL(S.[LVL_3_ID], ''), '|',  ISNULL(S.[LVL_3_DESC], ''), '|',
                ISNULL(S.[LVL_4_ID], ''), '|',  ISNULL(S.[LVL_4_DESC], ''), '|',
                ISNULL(S.[LVL_5_ID], ''), '|',  ISNULL(S.[LVL_5_DESC], ''), '|',
                ISNULL(S.[LVL_6_ID], ''), '|',  ISNULL(S.[LVL_6_DESC], ''), '|',
                ISNULL(S.[LVL_7_ID], ''), '|',  ISNULL(S.[LVL_7_DESC], ''), '|',
                ISNULL(S.[LVL_8_ID], ''), '|',  ISNULL(S.[LVL_8_DESC], ''), '|',
                ISNULL(S.[LVL_9_ID], ''), '|',  ISNULL(S.[LVL_9_DESC], '')
            )
        ) AS HashKey

    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCHZ_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Profit Center Controlling Area ID of HH]     = S.[CONTROLLING_AREA_ID],
        T.[Profit Center Leaf Level of HH]              = S.[LEAF_LVL],
        T.[Profit Center Level 0 ID of HH]              = S.[LVL_0_ID],
        T.[Profit Center Level 0 Description of HH]     = S.[LVL_0_DESC],
        T.[Profit Center Level 1 ID of HH]              = S.[LVL_1_ID],
        T.[Profit Center Level 1 Description of HH]     = S.[LVL_1_DESC],
        T.[Profit Center Level 2 ID of HH]              = S.[LVL_2_ID],
        T.[Profit Center Level 2 Description of HH]     = S.[LVL_2_DESC],
        T.[Profit Center Level 3 ID of HH]              = S.[LVL_3_ID],
        T.[Profit Center Level 3 Description of HH]     = S.[LVL_3_DESC],
        T.[Profit Center Level 4 ID of HH]              = S.[LVL_4_ID],
        T.[Profit Center Level 4 Description of HH]     = S.[LVL_4_DESC],
        T.[Profit Center Level 5 ID of HH]              = S.[LVL_5_ID],
        T.[Profit Center Level 5 Description of HH]     = S.[LVL_5_DESC],
        T.[Profit Center Level 6 ID of HH]              = S.[LVL_6_ID],
        T.[Profit Center Level 6 Description of HH]     = S.[LVL_6_DESC],
        T.[Profit Center Level 7 ID of HH]              = S.[LVL_7_ID],
        T.[Profit Center Level 7 Description of HH]     = S.[LVL_7_DESC],
        T.[Profit Center Level 8 ID of HH]              = S.[LVL_8_ID],
        T.[Profit Center Level 8 Description of HH]     = S.[LVL_8_DESC],
        T.[Profit Center Level 9 ID of HH]              = S.[LVL_9_ID],
        T.[Profit Center Level 9 Description of HH]     = S.[LVL_9_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Profit Center Controlling Area ID of HH], ''), '|',
                ISNULL(T.[Profit Center Leaf Level of HH], ''), '|',
                ISNULL(T.[Profit Center Level 0 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 0 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 1 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 1 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 2 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 2 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 3 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 3 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 4 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 4 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 5 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 5 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 6 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 6 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 7 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 7 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 8 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 8 Description of HH], ''), '|',
                ISNULL(T.[Profit Center Level 9 ID of HH], ''), '|', ISNULL(T.[Profit Center Level 9 Description of HH], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL]
    (
        [PROFT_CNTR_KEY],
        [Profit Center Controlling Area ID of HH],
        [Profit Center Leaf Level of HH],
        [Profit Center Level 0 ID of HH],
        [Profit Center Level 0 Description of HH],
        [Profit Center Level 1 ID of HH],
        [Profit Center Level 1 Description of HH],
        [Profit Center Level 2 ID of HH],
        [Profit Center Level 2 Description of HH],
        [Profit Center Level 3 ID of HH],
        [Profit Center Level 3 Description of HH],
        [Profit Center Level 4 ID of HH],
        [Profit Center Level 4 Description of HH],
        [Profit Center Level 5 ID of HH],
        [Profit Center Level 5 Description of HH],
        [Profit Center Level 6 ID of HH],
        [Profit Center Level 6 Description of HH],
        [Profit Center Level 7 ID of HH],
        [Profit Center Level 7 Description of HH],
        [Profit Center Level 8 ID of HH],
        [Profit Center Level 8 Description of HH],
        [Profit Center Level 9 ID of HH],
        [Profit Center Level 9 Description of HH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[LEAF_LVL],
        S.[LVL_0_ID],  S.[LVL_0_DESC],
        S.[LVL_1_ID],  S.[LVL_1_DESC],
        S.[LVL_2_ID],  S.[LVL_2_DESC],
        S.[LVL_3_ID],  S.[LVL_3_DESC],
        S.[LVL_4_ID],  S.[LVL_4_DESC],
        S.[LVL_5_ID],  S.[LVL_5_DESC],
        S.[LVL_6_ID],  S.[LVL_6_DESC],
        S.[LVL_7_ID],  S.[LVL_7_DESC],
        S.[LVL_8_ID],  S.[LVL_8_DESC],
        S.[LVL_9_ID],  S.[LVL_9_DESC]
    FROM #SourceData AS S
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL] AS T
        WHERE T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
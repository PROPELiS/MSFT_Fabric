CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_HY_COSTCHZ_CUR_D]
AS
BEGIN
    --Load source data into a temp table with a hash key
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
                ISNULL(S.[COST_CNTR_KEY], ''), '|',
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S;

    --Update changed rows in target (hash mismatch)
    UPDATE T
    SET 
        [CONTROLLING_AREA_ID]              = S.[CONTROLLING_AREA_ID],
        [Cost Center ID Leaf level HH]     = S.[LEAF_LVL],
        [LVL_0_ID]                         = S.[LVL_0_ID],
        [LVL_0_DESC]                       = S.[LVL_0_DESC],
        [Cost Center Level 1 ID HH]        = S.[LVL_1_ID],
        [Cost Center Level 1 Description HH] = S.[LVL_1_DESC],
        [Cost Center Level 2 ID HH]        = S.[LVL_2_ID],
        [Cost Center Level 2 Description HH] = S.[LVL_2_DESC],
        [Cost Center Level 3 ID HH]        = S.[LVL_3_ID],
        [Cost Center Level 3 Description HH] = S.[LVL_3_DESC],
        [Cost Center Level 4 ID HH]        = S.[LVL_4_ID],
        [Cost Center Level 4 Description HH] = S.[LVL_4_DESC],
        [Cost Center Level 5 ID HH]        = S.[LVL_5_ID],
        [Cost Center Level 5 Description HH] = S.[LVL_5_DESC],
        [Cost Center Level 6 ID HH]        = S.[LVL_6_ID],
        [Cost Center Level 6 Description HH] = S.[LVL_6_DESC],
        [Cost Center Level 7 ID HH]        = S.[LVL_7_ID],
        [Cost Center Level 7 Description HH] = S.[LVL_7_DESC],
        [Cost Center Level 8 ID HH]        = S.[LVL_8_ID],
        [Cost Center Level 8 Description HH] = S.[LVL_8_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[COST_CNTR_KEY], ''), '|',
                ISNULL(T.[CONTROLLING_AREA_ID], ''), '|',
                ISNULL(T.[Cost Center ID Leaf level HH], ''), '|',
                ISNULL(T.[LVL_0_ID], ''), '|',
                ISNULL(T.[LVL_0_DESC], ''), '|',
                ISNULL(T.[Cost Center Level 1 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 1 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 2 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 2 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 3 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 3 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 4 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 4 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 5 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 5 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 6 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 6 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 7 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 7 Description HH], ''), '|',
                ISNULL(T.[Cost Center Level 8 ID HH], ''), '|',
                ISNULL(T.[Cost Center Level 8 Description HH], '')
            )
        ) <> S.HashKey;

  
    --Insert only new rows not already in target

    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D]
    (
        [COST_CNTR_KEY],
        [CONTROLLING_AREA_ID],
        [Cost Center ID Leaf level HH],
        [LVL_0_ID],
        [LVL_0_DESC],
        [Cost Center Level 1 ID HH],
        [Cost Center Level 1 Description HH],
        [Cost Center Level 2 ID HH],
        [Cost Center Level 2 Description HH],
        [Cost Center Level 3 ID HH],
        [Cost Center Level 3 Description HH],
        [Cost Center Level 4 ID HH],
        [Cost Center Level 4 Description HH],
        [Cost Center Level 5 ID HH],
        [Cost Center Level 5 Description HH],
        [Cost Center Level 6 ID HH],
        [Cost Center Level 6 Description HH],
        [Cost Center Level 7 ID HH],
        [Cost Center Level 7 Description HH],
        [Cost Center Level 8 ID HH],
        [Cost Center Level 8 Description HH]
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
        FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';

END;
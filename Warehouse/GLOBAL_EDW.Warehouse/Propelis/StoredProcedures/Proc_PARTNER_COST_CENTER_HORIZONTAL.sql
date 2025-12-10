CREATE   PROCEDURE [Propelis].[Proc_PARTNER_COST_CENTER_HORIZONTAL]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------
    -- Step 1: Load source data into temp table with HashKey
    ----------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT
        S.[COST_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[LEAF_LVL],
        S.[LVL_0_ID], S.[LVL_0_DESC],
        S.[LVL_1_ID], S.[LVL_1_DESC],
        S.[LVL_2_ID], S.[LVL_2_DESC],
        S.[LVL_3_ID], S.[LVL_3_DESC],
        S.[LVL_4_ID], S.[LVL_4_DESC],
        S.[LVL_5_ID], S.[LVL_5_DESC],
        S.[LVL_6_ID], S.[LVL_6_DESC],
        S.[LVL_7_ID], S.[LVL_7_DESC],
        S.[LVL_8_ID], S.[LVL_8_DESC],

        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(S.[CONTROLLING_AREA_ID],''),'|',
                ISNULL(S.[LEAF_LVL],''),'|',
                ISNULL(S.[LVL_0_ID],''),'|', ISNULL(S.[LVL_0_DESC],''),'|',
                ISNULL(S.[LVL_1_ID],''),'|', ISNULL(S.[LVL_1_DESC],''),'|',
                ISNULL(S.[LVL_2_ID],''),'|', ISNULL(S.[LVL_2_DESC],''),'|',
                ISNULL(S.[LVL_3_ID],''),'|', ISNULL(S.[LVL_3_DESC],''),'|',
                ISNULL(S.[LVL_4_ID],''),'|', ISNULL(S.[LVL_4_DESC],''),'|',
                ISNULL(S.[LVL_5_ID],''),'|', ISNULL(S.[LVL_5_DESC],''),'|',
                ISNULL(S.[LVL_6_ID],''),'|', ISNULL(S.[LVL_6_DESC],''),'|',
                ISNULL(S.[LVL_7_ID],''),'|', ISNULL(S.[LVL_7_DESC],''),'|',
                ISNULL(S.[LVL_8_ID],''),'|', ISNULL(S.[LVL_8_DESC],'')
            )
        ) AS HashKey

    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S;


    ----------------------------------------------------------------------
    -- Step 2: Update only changed rows (HashKey mismatch)
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Cost Center Controlling Area ID HH]       = S.[CONTROLLING_AREA_ID],
        T.[Partner Cost Center ID Leaf level HH]             = S.[LEAF_LVL],
        T.[Partner Cost Center Level 0 ID HH]                = S.[LVL_0_ID],
        T.[Partner Cost Center Level 0 Description HH]       = S.[LVL_0_DESC],
        T.[Partner Cost Center Level 1 ID HH]                = S.[LVL_1_ID],
        T.[Partner Cost Center Level 1 Description HH]       = S.[LVL_1_DESC],
        T.[Partner Cost Center Level 2 ID HH]                = S.[LVL_2_ID],
        T.[Partner Cost Center Level 2 Description HH]       = S.[LVL_2_DESC],
        T.[Partner Cost Center Level 3 ID HH]                = S.[LVL_3_ID],
        T.[Partner Cost Center Level 3 Description HH]       = S.[LVL_3_DESC],
        T.[Partner Cost Center Level 4 ID HH]                = S.[LVL_4_ID],
        T.[Partner Cost Center Level 4 Description HH]       = S.[LVL_4_DESC],
        T.[Partner Cost Center Level 5 ID HH]                = S.[LVL_5_ID],
        T.[Partner Cost Center Level 5 Description HH]       = S.[LVL_5_DESC],
        T.[Partner Cost Center Level 6 ID HH]                = S.[LVL_6_ID],
        T.[Partner Cost Center Level 6 Description HH]       = S.[LVL_6_DESC],
        T.[Partner Cost Center Level 7 ID HH]                = S.[LVL_7_ID],
        T.[Partner Cost Center Level 7 Description HH]       = S.[LVL_7_DESC],
        T.[Partner Cost Center Level 8 ID HH]                = S.[LVL_8_ID],
        T.[Partner Cost Center Level 8 Description HH]       = S.[LVL_8_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Partner Cost Center Controlling Area ID HH],''),'|',
                ISNULL(T.[Partner Cost Center ID Leaf level HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 0 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 0 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 1 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 1 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 2 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 2 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 3 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 3 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 4 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 4 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 5 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 5 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 6 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 6 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 7 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 7 Description HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 8 ID HH],''),'|',
                ISNULL(T.[Partner Cost Center Level 8 Description HH],'')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL]
    (
        [COST_CNTR_KEY],
        [Partner Cost Center Controlling Area ID HH],
        [Partner Cost Center ID Leaf level HH],
        [Partner Cost Center Level 0 ID HH],
        [Partner Cost Center Level 0 Description HH],
        [Partner Cost Center Level 1 ID HH],
        [Partner Cost Center Level 1 Description HH],
        [Partner Cost Center Level 2 ID HH],
        [Partner Cost Center Level 2 Description HH],
        [Partner Cost Center Level 3 ID HH],
        [Partner Cost Center Level 3 Description HH],
        [Partner Cost Center Level 4 ID HH],
        [Partner Cost Center Level 4 Description HH],
        [Partner Cost Center Level 5 ID HH],
        [Partner Cost Center Level 5 Description HH],
        [Partner Cost Center Level 6 ID HH],
        [Partner Cost Center Level 6 Description HH],
        [Partner Cost Center Level 7 ID HH],
        [Partner Cost Center Level 7 Description HH],
        [Partner Cost Center Level 8 ID HH],
        [Partner Cost Center Level 8 Description HH]
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
        FROM [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL] T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
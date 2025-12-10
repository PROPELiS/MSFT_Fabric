CREATE   PROCEDURE [Propelis].[Proc_PARTNER_PROFIT_CENTER_HORIZONTAL]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------
    -- Step 1: Load source data into temp table with HashKey
    ----------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

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
        S.[LVL_9_ID],  S.[LVL_9_DESC],

        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(S.[CONTROLLING_AREA_ID],''),'|',
                ISNULL(S.[LEAF_LVL],''),'|',
                ISNULL(S.[LVL_0_ID],''),'|',  ISNULL(S.[LVL_0_DESC],''),'|',
                ISNULL(S.[LVL_1_ID],''),'|',  ISNULL(S.[LVL_1_DESC],''),'|',
                ISNULL(S.[LVL_2_ID],''),'|',  ISNULL(S.[LVL_2_DESC],''),'|',
                ISNULL(S.[LVL_3_ID],''),'|',  ISNULL(S.[LVL_3_DESC],''),'|',
                ISNULL(S.[LVL_4_ID],''),'|',  ISNULL(S.[LVL_4_DESC],''),'|',
                ISNULL(S.[LVL_5_ID],''),'|',  ISNULL(S.[LVL_5_DESC],''),'|',
                ISNULL(S.[LVL_6_ID],''),'|',  ISNULL(S.[LVL_6_DESC],''),'|',
                ISNULL(S.[LVL_7_ID],''),'|',  ISNULL(S.[LVL_7_DESC],''),'|',
                ISNULL(S.[LVL_8_ID],''),'|',  ISNULL(S.[LVL_8_DESC],''),'|',
                ISNULL(S.[LVL_9_ID],''),'|',  ISNULL(S.[LVL_9_DESC],'')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCHZ_CUR_D] AS S;


    ----------------------------------------------------------------------
    -- Step 2: Update rows where hash is different
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Profit Center Controlling Area ID of HH] = S.[CONTROLLING_AREA_ID],
        T.[Partner Profit Center Leaf Level of HH]          = S.[LEAF_LVL],
        T.[Partner Profit Center Level 0 ID of HH]          = S.[LVL_0_ID],
        T.[Partner Profit Center Level 0 Description of HH] = S.[LVL_0_DESC],
        T.[Partner Profit Center Level 1 ID of HH]          = S.[LVL_1_ID],
        T.[Partner Profit Center Level 1 Description of HH] = S.[LVL_1_DESC],
        T.[Partner Profit Center Level 2 ID of HH]          = S.[LVL_2_ID],
        T.[Partner Profit Center Level 2 Description of HH] = S.[LVL_2_DESC],
        T.[Partner Profit Center Level 3 ID of HH]          = S.[LVL_3_ID],
        T.[Partner Profit Center Level 3 Description of HH] = S.[LVL_3_DESC],
        T.[Partner Profit Center Level 4 ID of HH]          = S.[LVL_4_ID],
        T.[Partner Profit Center Level 4 Description of HH] = S.[LVL_4_DESC],
        T.[Partner Profit Center Level 5 ID of HH]          = S.[LVL_5_ID],
        T.[Partner Profit Center Level 5 Description of HH] = S.[LVL_5_DESC],
        T.[Partner Profit Center Level 6 ID of HH]          = S.[LVL_6_ID],
        T.[Partner Profit Center Level 6 Description of HH] = S.[LVL_6_DESC],
        T.[Partner Profit Center Level 7 ID of HH]          = S.[LVL_7_ID],
        T.[Partner Profit Center Level 7 Description of HH] = S.[LVL_7_DESC],
        T.[Partner Profit Center Level 8 ID of HH]          = S.[LVL_8_ID],
        T.[Partner Profit Center Level 8 Description of HH] = S.[LVL_8_DESC],
        T.[Partner Profit Center Level 9 ID of HH]          = S.[LVL_9_ID],
        T.[Partner Profit Center Level 9 Description of HH] = S.[LVL_9_DESC]
    FROM [Propelis].[PARTNER_PROFIT_CENTER_HORIZONTAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFIT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Partner Profit Center Controlling Area ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Leaf Level of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 0 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 0 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 1 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 1 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 2 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 2 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 3 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 3 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 4 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 4 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 5 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 5 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 6 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 6 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 7 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 7 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 8 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 8 Description of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 9 ID of HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 9 Description of HH],'')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert new records not present in target
    ----------------------------------------------------------------------
    INSERT INTO [Propelis].[PARTNER_PROFIT_CENTER_HORIZONTAL]
    (
        [PROFIT_CNTR_KEY],
        [Partner Profit Center Controlling Area ID of HH],
        [Partner Profit Center Leaf Level of HH],
        [Partner Profit Center Level 0 ID of HH],
        [Partner Profit Center Level 0 Description of HH],
        [Partner Profit Center Level 1 ID of HH],
        [Partner Profit Center Level 1 Description of HH],
        [Partner Profit Center Level 2 ID of HH],
        [Partner Profit Center Level 2 Description of HH],
        [Partner Profit Center Level 3 ID of HH],
        [Partner Profit Center Level 3 Description of HH],
        [Partner Profit Center Level 4 ID of HH],
        [Partner Profit Center Level 4 Description of HH],
        [Partner Profit Center Level 5 ID of HH],
        [Partner Profit Center Level 5 Description of HH],
        [Partner Profit Center Level 6 ID of HH],
        [Partner Profit Center Level 6 Description of HH],
        [Partner Profit Center Level 7 ID of HH],
        [Partner Profit Center Level 7 Description of HH],
        [Partner Profit Center Level 8 ID of HH],
        [Partner Profit Center Level 8 Description of HH],
        [Partner Profit Center Level 9 ID of HH],
        [Partner Profit Center Level 9 Description of HH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
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
        S.[LVL_9_ID],
        S.[LVL_9_DESC]
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Propelis].[PARTNER_PROFIT_CENTER_HORIZONTAL] AS T
        WHERE T.[PROFIT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
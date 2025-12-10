CREATE   PROCEDURE [Propelis].[Proc_Partner_Profit_Center_H_Hier]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------
    -- Step 1: Load source with HashKey into temp table
    ----------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT
        S.[PROFT_CNTR_KEY],
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
        S.[LVL_9_ID], S.[LVL_9_DESC],

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
                ISNULL(S.[LVL_8_ID],''),'|', ISNULL(S.[LVL_8_DESC],''),'|',
                ISNULL(S.[LVL_9_ID],''),'|', ISNULL(S.[LVL_9_DESC],'')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCHZ_CUR_D] AS S;


    ----------------------------------------------------------------------
    -- Step 2: Update rows only when HashKey differs
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[CONTROLLING_AREA_ID]                          = S.[CONTROLLING_AREA_ID],
        T.[LEAF_LVL]                                     = S.[LEAF_LVL],
        T.[Partner Profit Center Level 0 ID HH]          = S.[LVL_0_ID],
        T.[Partner Profit Center Level 0 Description HH] = S.[LVL_0_DESC],
        T.[Partner Profit Center Level 1 ID HH]          = S.[LVL_1_ID],
        T.[Partner Profit Center Level 1 Description HH] = S.[LVL_1_DESC],
        T.[Partner Profit Center Level 2 ID HH]          = S.[LVL_2_ID],
        T.[Partner Profit Center Level 2 Description HH] = S.[LVL_2_DESC],
        T.[Partner Profit Center Level 3 ID HH]          = S.[LVL_3_ID],
        T.[Partner Profit Center Level 3 Description HH] = S.[LVL_3_DESC],
        T.[Partner Profit Center Level 4 ID HH]          = S.[LVL_4_ID],
        T.[Partner Profit Center Level 4 Description HH] = S.[LVL_4_DESC],
        T.[Partner Profit Center Level 5 ID HH]          = S.[LVL_5_ID],
        T.[Partner Profit Center Level 5 Description HH] = S.[LVL_5_DESC],
        T.[Partner Profit Center Level 6 ID HH]          = S.[LVL_6_ID],
        T.[Partner Profit Center Level 6 Description HH] = S.[LVL_6_DESC],
        T.[Partner Profit Center Level 7 ID HH]          = S.[LVL_7_ID],
        T.[Partner Profit Center Level 7 Description HH] = S.[LVL_7_DESC],
        T.[Partner Profit Center Level 8 ID HH]          = S.[LVL_8_ID],
        T.[Partner Profit Center Level 8 Description HH] = S.[LVL_8_DESC],
        T.[Partner Profit Center Level 9 ID HH]          = S.[LVL_9_ID],
        T.[Partner Profit Center Level 9 Description HH] = S.[LVL_9_DESC]
    FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[CONTROLLING_AREA_ID],''),'|',
                ISNULL(T.[LEAF_LVL],''),'|',
                ISNULL(T.[Partner Profit Center Level 0 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 0 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 1 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 1 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 2 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 2 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 3 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 3 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 4 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 4 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 5 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 5 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 6 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 6 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 7 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 7 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 8 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 8 Description HH],''),'|',
                ISNULL(T.[Partner Profit Center Level 9 ID HH],''),'|', 
                ISNULL(T.[Partner Profit Center Level 9 Description HH],'')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert new rows
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier]
    (
        [PROFT_CNTR_KEY],
        [CONTROLLING_AREA_ID],
        [LEAF_LVL],
        [Partner Profit Center Level 0 ID HH],
        [Partner Profit Center Level 0 Description HH],
        [Partner Profit Center Level 1 ID HH],
        [Partner Profit Center Level 1 Description HH],
        [Partner Profit Center Level 2 ID HH],
        [Partner Profit Center Level 2 Description HH],
        [Partner Profit Center Level 3 ID HH],
        [Partner Profit Center Level 3 Description HH],
        [Partner Profit Center Level 4 ID HH],
        [Partner Profit Center Level 4 Description HH],
        [Partner Profit Center Level 5 ID HH],
        [Partner Profit Center Level 5 Description HH],
        [Partner Profit Center Level 6 ID HH],
        [Partner Profit Center Level 6 Description HH],
        [Partner Profit Center Level 7 ID HH],
        [Partner Profit Center Level 7 Description HH],
        [Partner Profit Center Level 8 ID HH],
        [Partner Profit Center Level 8 Description HH],
        [Partner Profit Center Level 9 ID HH],
        [Partner Profit Center Level 9 Description HH]
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
        FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier] T
        WHERE T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
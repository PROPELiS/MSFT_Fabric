CREATE   PROCEDURE [Propelis].[Proc_Partner_Profit_Center_V_Hier]
AS
BEGIN

    ----------------------------------------------------------------------
    -- Step 1: Load source data into #SourceData with HashKey
    ----------------------------------------------------------------------
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


    ----------------------------------------------------------------------
    -- Step 2: Update ONLY changed rows (where hash is different)
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Profit Center Controlling Area ID VH]      = S.[CONTROLLING_AREA_ID],
        T.[Partner Profit Center Parent ID VH]                = S.[PARNT_ID],
        T.[Partner Profit Center Parent Description VH]       = S.[PARNT_DESC],
        T.[Partner Profit Center Child ID VH]                 = S.[CHILD_ID],
        T.[Partner Profit Center Child ID Description VH]     = S.[CHILD_DESC],
        T.[Partner Profit Center Level VH]                    = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Partner Profit Center Controlling Area ID VH], ''), '|',
                ISNULL(T.[Partner Profit Center Parent ID VH], ''), '|',
                ISNULL(T.[Partner Profit Center Parent Description VH], ''), '|',
                ISNULL(T.[Partner Profit Center Child ID VH], ''), '|',
                ISNULL(T.[Partner Profit Center Child ID Description VH], ''), '|',
                ISNULL(T.[Partner Profit Center Level VH], '')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert NEW rows not present in target
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier]
    (
        [PROFT_CNTR_KEY],
        [Partner Profit Center Controlling Area ID VH],
        [Partner Profit Center Parent ID VH],
        [Partner Profit Center Parent Description VH],
        [Partner Profit Center Child ID VH],
        [Partner Profit Center Child ID Description VH],
        [Partner Profit Center Level VH]
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
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier] AS T
        WHERE T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
CREATE   PROCEDURE [Propelis].[Proc_PARTNER_PROFIT_CENTER_VERTICAL]
AS
BEGIN

    ----------------------------------------------------------------------
    -- Step 1: Load source into temp table with HashKey
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
    -- Step 2: Update rows where the existing hash differs from new hash
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Profit Center Controlling Area ID of VH]      = S.[CONTROLLING_AREA_ID],
        T.[Partner Profit Center Parent ID of VH]                = S.[PARNT_ID],
        T.[Partner Profit Center Parent Description of VH]       = S.[PARNT_DESC],
        T.[Partner Profit Center Child ID VH]                    = S.[CHILD_ID],
        T.[Partner Profit Center Child ID Description of VH]     = S.[CHILD_DESC],
        T.[Partner Profit Center Level of VH]                    = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFIT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Partner Profit Center Controlling Area ID of VH], ''), '|',
                ISNULL(T.[Partner Profit Center Parent ID of VH], ''), '|',
                ISNULL(T.[Partner Profit Center Parent Description of VH], ''), '|',
                ISNULL(T.[Partner Profit Center Child ID VH], ''), '|',
                ISNULL(T.[Partner Profit Center Child ID Description of VH], ''), '|',
                ISNULL(T.[Partner Profit Center Level of VH], '')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL]
    (
        [PROFIT_CNTR_KEY],
        [Partner Profit Center Controlling Area ID of VH],
        [Partner Profit Center Parent ID of VH],
        [Partner Profit Center Parent Description of VH],
        [Partner Profit Center Child ID VH],
        [Partner Profit Center Child ID Description of VH],
        [Partner Profit Center Level of VH]
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
        FROM [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL] AS T
        WHERE T.[PROFIT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
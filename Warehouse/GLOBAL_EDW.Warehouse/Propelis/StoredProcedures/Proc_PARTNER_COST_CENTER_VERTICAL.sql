CREATE   PROCEDURE [Propelis].[Proc_PARTNER_COST_CENTER_VERTICAL]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------
    -- Step 1: Load source records with HashKey into temp table
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
                ISNULL(S.[CONTROLLING_AREA_ID],''), '|',
                ISNULL(S.[PARNT_ID],''), '|',
                ISNULL(S.[PARNT_DESC],''), '|',
                ISNULL(S.[CHILD_ID],''), '|',
                ISNULL(S.[CHILD_DESC],''), '|',
                ISNULL(S.[LVL],'')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCVZ_CUR_D] AS S;


    ----------------------------------------------------------------------
    -- Step 2: Update only changed target rows using HashKey comparison
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Cost Center Controlling Area ID VH]  = S.[CONTROLLING_AREA_ID],
        T.[Partner Cost Center Parent ID VH]            = S.[PARNT_ID],
        T.[Partner Cost Center Parent Description VH]   = S.[PARNT_DESC],
        T.[Partner Cost Center Child ID VH]             = S.[CHILD_ID],
        T.[Partner Cost Center Child Description VH]    = S.[CHILD_DESC],
        T.[Partner Cost Center Hierarchical Level VH]   = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_VERTICAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Partner Cost Center Controlling Area ID VH],''), '|',
                ISNULL(T.[Partner Cost Center Parent ID VH],''), '|',
                ISNULL(T.[Partner Cost Center Parent Description VH],''), '|',
                ISNULL(T.[Partner Cost Center Child ID VH],''), '|',
                ISNULL(T.[Partner Cost Center Child Description VH],''), '|',
                ISNULL(T.[Partner Cost Center Hierarchical Level VH],'')
            )
        ) <> S.HashKey;


    ----------------------------------------------------------------------
    -- Step 3: Insert new rows not present in target
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_VERTICAL]
    (
        [COST_CNTR_KEY],
        [Partner Cost Center Controlling Area ID VH],
        [Partner Cost Center Parent ID VH],
        [Partner Cost Center Parent Description VH],
        [Partner Cost Center Child ID VH],
        [Partner Cost Center Child Description VH],
        [Partner Cost Center Hierarchical Level VH]
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
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_VERTICAL] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
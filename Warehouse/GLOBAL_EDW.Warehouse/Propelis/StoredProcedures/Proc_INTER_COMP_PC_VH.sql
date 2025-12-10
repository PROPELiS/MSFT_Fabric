CREATE   PROCEDURE [Propelis].[Proc_INTER_COMP_PC_VH]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data with HashKey into a temp table
    ------------------------------------------------------------------
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


    ------------------------------------------------------------------
    -- Step 2: Update only changed rows based on HashKey comparison
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Inter Comp Profit Center Controlling Area ID VH] = S.[CONTROLLING_AREA_ID],
        T.[Inter Comp Profit Center Parent ID VH]           = S.[PARNT_ID],
        T.[Inter Comp Profit Center Parent Description VH]  = S.[PARNT_DESC],
        T.[Inter Comp Profit Center Child ID VH]            = S.[CHILD_ID],
        T.[Inter Comp Profit Center Child ID Description VH] = S.[CHILD_DESC],
        T.[Inter Comp Profit Center Level VH]               = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH] AS T
    INNER JOIN #SourceData AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE
        HASHBYTES('SHA2_256',
            CONCAT(
                ISNULL(T.[Inter Comp Profit Center Controlling Area ID VH], ''), '|',
                ISNULL(T.[Inter Comp Profit Center Parent ID VH], ''), '|',
                ISNULL(T.[Inter Comp Profit Center Parent Description VH], ''), '|',
                ISNULL(T.[Inter Comp Profit Center Child ID VH], ''), '|',
                ISNULL(T.[Inter Comp Profit Center Child ID Description VH], ''), '|',
                ISNULL(T.[Inter Comp Profit Center Level VH], '')
            )
        ) <> S.HashKey;


    ------------------------------------------------------------------
    -- Step 3: Insert records that do not exist in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH]
    (
        [PROFT_CNTR_KEY],
        [Inter Comp Profit Center Controlling Area ID VH],
        [Inter Comp Profit Center Parent ID VH],
        [Inter Comp Profit Center Parent Description VH],
        [Inter Comp Profit Center Child ID VH],
        [Inter Comp Profit Center Child ID Description VH],
        [Inter Comp Profit Center Level VH]
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
        FROM [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH] AS T
        WHERE T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    );

END;
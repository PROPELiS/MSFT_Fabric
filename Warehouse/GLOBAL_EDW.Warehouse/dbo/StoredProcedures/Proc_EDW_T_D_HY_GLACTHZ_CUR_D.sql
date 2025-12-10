CREATE   PROCEDURE dbo.Proc_EDW_T_D_HY_GLACTHZ_CUR_D
AS
BEGIN
    SET NOCOUNT ON;

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        HIERCHY_GROUP,
        GL_CD,
        CHART_OF_ACCTS,
        FUNCTNL_AREA,
        LEAF_LVL,
        LVL_0_ID,
        LVL_0_DESC,
        LVL_1_ID,
        LVL_1_DESC,
        LVL_2_ID,
        LVL_2_DESC,
        LVL_3_ID,
        LVL_3_DESC,
        LVL_4_ID,
        LVL_4_DESC,
        LVL_5_ID,
        LVL_5_DESC,
        LVL_6_ID,
        LVL_6_DESC,
        LVL_1_NODEID,
        LVL_1_IOBJNM,
        LVL_1_NODENAME,
        LVL_2_NODEID,
        LVL_2_IOBJNM,
        LVL_2_NODENAME,
        LVL_3_NODEID,
        LVL_3_IOBJNM,
        LVL_3_NODENAME,
        LVL_4_NODEID,
        LVL_4_IOBJNM,
        LVL_4_NODENAME,
        LVL_5_NODEID,
        LVL_5_IOBJNM,
        LVL_5_NODENAME,
        LVL_6_NODEID,
        LVL_6_IOBJNM,
        LVL_6_NODENAME,

        HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(HIERCHY_GROUP, ''), '|',
                ISNULL(GL_CD, ''), '|',
                ISNULL(CHART_OF_ACCTS, ''), '|',
                ISNULL(FUNCTNL_AREA, ''), '|',
                ISNULL(CAST(LEAF_LVL AS VARCHAR(10)), ''), '|',
                ISNULL(LVL_0_ID, ''), '|',
                ISNULL(LVL_0_DESC, ''), '|',
                ISNULL(LVL_1_ID, ''), '|',
                ISNULL(LVL_1_DESC, ''), '|',
                ISNULL(LVL_2_ID, ''), '|',
                ISNULL(LVL_2_DESC, ''), '|',
                ISNULL(LVL_3_ID, ''), '|',
                ISNULL(LVL_3_DESC, ''), '|',
                ISNULL(LVL_4_ID, ''), '|',
                ISNULL(LVL_4_DESC, ''), '|',
                ISNULL(LVL_5_ID, ''), '|',
                ISNULL(LVL_5_DESC, ''), '|',
                ISNULL(LVL_6_ID, ''), '|',
                ISNULL(LVL_6_DESC, ''), '|',
                ISNULL(LVL_1_NODEID, ''), '|',
                ISNULL(LVL_1_IOBJNM, ''), '|',
                ISNULL(LVL_1_NODENAME, ''), '|',
                ISNULL(LVL_2_NODEID, ''), '|',
                ISNULL(LVL_2_IOBJNM, ''), '|',
                ISNULL(LVL_2_NODENAME, ''), '|',
                ISNULL(LVL_3_NODEID, ''), '|',
                ISNULL(LVL_3_IOBJNM, ''), '|',
                ISNULL(LVL_3_NODENAME, ''), '|',
                ISNULL(LVL_4_NODEID, ''), '|',
                ISNULL(LVL_4_IOBJNM, ''), '|',
                ISNULL(LVL_4_NODENAME, ''), '|',
                ISNULL(LVL_5_NODEID, ''), '|',
                ISNULL(LVL_5_IOBJNM, ''), '|',
                ISNULL(LVL_5_NODENAME, ''), '|',
                ISNULL(LVL_6_NODEID, ''), '|',
                ISNULL(LVL_6_IOBJNM, ''), '|',
                ISNULL(LVL_6_NODENAME, '')
            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_GLACTHZ_CUR_D];

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [General Ledger ID Hierarchy Group] = S.HIERCHY_GROUP,
        GL_CD = S.GL_CD,
        CHART_OF_ACCTS = S.CHART_OF_ACCTS,
        FUNCTNL_AREA = S.FUNCTNL_AREA,
        [General Ledger Leaf Level HH] = S.LEAF_LVL,
        [General Ledger Level 0 ID HH] = S.LVL_0_ID,
        [General Ledger Level 0 Description HH] = S.LVL_0_DESC,
        [General Ledger Level 1 ID HH] = S.LVL_1_ID,
        [General Ledger Level 1 Description HH] = S.LVL_1_DESC,
        [General Ledger Level 2 ID HH] = S.LVL_2_ID,
        [General Ledger Level 2 Description HH] = S.LVL_2_DESC,
        [General Ledger Level 3 ID HH] = S.LVL_3_ID,
        [General Ledger Level 3 Description HH] = S.LVL_3_DESC,
        [General Ledger Level 4 ID HH] = S.LVL_4_ID,
        [General Ledger Level 4 Description HH] = S.LVL_4_DESC,
        [General Ledger Level 5 ID HH] = S.LVL_5_ID,
        [General Ledger Level 5 Description HH] = S.LVL_5_DESC,
        [General Ledger Level 6 ID HH] = S.LVL_6_ID,
        [General Ledger Level 6 Description HH] = S.LVL_6_DESC,
        [General Ledger Level 1 NODEID] = S.LVL_1_NODEID,
        [General Ledger Level 1 IOBJNM] = S.LVL_1_IOBJNM,
        [General Ledger Level 1 NODENAME] = S.LVL_1_NODENAME,
        [General Ledger Level 2 NODEID] = S.LVL_2_NODEID,
        [General Ledger Level 2 IOBJNM] = S.LVL_2_IOBJNM,
        [General Ledger Level 2 NODENAME] = S.LVL_2_NODENAME,
        [General Ledger Level 3 NODEID] = S.LVL_3_NODEID,
        [General Ledger Level 3 IOBJNM] = S.LVL_3_IOBJNM,
        [General Ledger Level 3 NODENAME] = S.LVL_3_NODENAME,
        [General Ledger Level 4 NODEID] = S.LVL_4_NODEID,
        [General Ledger Level 4 IOBJNM] = S.LVL_4_IOBJNM,
        [General Ledger Level 4 NODENAME] = S.LVL_4_NODENAME,
        [General Ledger Level 5 NODEID] = S.LVL_5_NODEID,
        [General Ledger Level 5 IOBJNM] = S.LVL_5_IOBJNM,
        [General Ledger Level 5 NODENAME] = S.LVL_5_NODENAME,
        [General Ledger Level 6 NODEID] = S.LVL_6_NODEID,
        [General Ledger Level 6 IOBJNM] = S.LVL_6_IOBJNM,
        [General Ledger Level 6 NODENAME] = S.LVL_6_NODENAME
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_GLACTHZ_CUR_D] AS T
    INNER JOIN #SourceData AS S
        ON T.GL_CD = S.GL_CD
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
                ISNULL(T.[General Ledger ID Hierarchy Group], ''), '|',
                ISNULL(T.GL_CD, ''), '|',
                ISNULL(T.CHART_OF_ACCTS, ''), '|',
                ISNULL(T.FUNCTNL_AREA, ''), '|',
                ISNULL(CAST(T.[General Ledger Leaf Level HH] AS VARCHAR(10)), ''), '|',
                ISNULL(T.[General Ledger Level 0 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 0 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 1 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 1 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 2 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 2 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 3 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 3 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 4 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 4 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 5 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 5 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 6 ID HH], ''), '|',
                ISNULL(T.[General Ledger Level 6 Description HH], ''), '|',
                ISNULL(T.[General Ledger Level 1 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 1 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 1 NODENAME], ''), '|',
                ISNULL(T.[General Ledger Level 2 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 2 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 2 NODENAME], ''), '|',
                ISNULL(T.[General Ledger Level 3 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 3 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 3 NODENAME], ''), '|',
                ISNULL(T.[General Ledger Level 4 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 4 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 4 NODENAME], ''), '|',
                ISNULL(T.[General Ledger Level 5 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 5 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 5 NODENAME], ''), '|',
                ISNULL(T.[General Ledger Level 6 NODEID], ''), '|',
                ISNULL(T.[General Ledger Level 6 IOBJNM], ''), '|',
                ISNULL(T.[General Ledger Level 6 NODENAME], '')
            )
        ) <> S.HashKey;

    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_GLACTHZ_CUR_D]
    (
        [General Ledger ID Hierarchy Group],
        GL_CD,
        CHART_OF_ACCTS,
        FUNCTNL_AREA,
        [General Ledger Leaf Level HH],
        [General Ledger Level 0 ID HH],
        [General Ledger Level 0 Description HH],
        [General Ledger Level 1 ID HH],
        [General Ledger Level 1 Description HH],
        [General Ledger Level 2 ID HH],
        [General Ledger Level 2 Description HH],
        [General Ledger Level 3 ID HH],
        [General Ledger Level 3 Description HH],
        [General Ledger Level 4 ID HH],
        [General Ledger Level 4 Description HH],
        [General Ledger Level 5 ID HH],
        [General Ledger Level 5 Description HH],
        [General Ledger Level 6 ID HH],
        [General Ledger Level 6 Description HH],
        [General Ledger Level 1 NODEID],
        [General Ledger Level 1 IOBJNM],
        [General Ledger Level 1 NODENAME],
        [General Ledger Level 2 NODEID],
        [General Ledger Level 2 IOBJNM],
        [General Ledger Level 2 NODENAME],
        [General Ledger Level 3 NODEID],
        [General Ledger Level 3 IOBJNM],
        [General Ledger Level 3 NODENAME],
        [General Ledger Level 4 NODEID],
        [General Ledger Level 4 IOBJNM],
        [General Ledger Level 4 NODENAME],
        [General Ledger Level 5 NODEID],
        [General Ledger Level 5 IOBJNM],
        [General Ledger Level 5 NODENAME],
        [General Ledger Level 6 NODEID],
        [General Ledger Level 6 IOBJNM],
        [General Ledger Level 6 NODENAME]
    )
    SELECT 
        HIERCHY_GROUP,
        GL_CD,
        CHART_OF_ACCTS,
        FUNCTNL_AREA,
        LEAF_LVL,
        LVL_0_ID,
        LVL_0_DESC,
        LVL_1_ID,
        LVL_1_DESC,
        LVL_2_ID,
        LVL_2_DESC,
        LVL_3_ID,
        LVL_3_DESC,
        LVL_4_ID,
        LVL_4_DESC,
        LVL_5_ID,
        LVL_5_DESC,
        LVL_6_ID,
        LVL_6_DESC,
        LVL_1_NODEID,
        LVL_1_IOBJNM,
        LVL_1_NODENAME,
        LVL_2_NODEID,
        LVL_2_IOBJNM,
        LVL_2_NODENAME,
        LVL_3_NODEID,
        LVL_3_IOBJNM,
        LVL_3_NODENAME,
        LVL_4_NODEID,
        LVL_4_IOBJNM,
        LVL_4_NODENAME,
        LVL_5_NODEID,
        LVL_5_IOBJNM,
        LVL_5_NODENAME,
        LVL_6_NODEID,
        LVL_6_IOBJNM,
        LVL_6_NODENAME
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_GLACTHZ_CUR_D] AS T
        WHERE T.GL_CD = S.GL_CD
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';

END;
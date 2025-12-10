CREATE     PROCEDURE Propelis.Proc_RESP_COST_CENTER_HORIZONTAL

AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S;

    -- Step 2: Update changed rows in target (hash mismatch)
    UPDATE T
    SET 
           [COST_CNTR_KEY]	= S.[COST_CNTR_KEY],
           [Resp Cost Center Controlling Area ID HH]	= S.[CONTROLLING_AREA_ID],
           [Resp Cost Center ID Leaf level HH]	= S.[LEAF_LVL],
           [Resp Cost Center Level 0 ID HH]	= S.[LVL_0_ID],
           [Resp Cost Center Level 0 Description HH]	= S.[LVL_0_DESC],
           [Resp Cost Center Level 1 ID HH]	= S.[LVL_1_ID],
           [Resp Cost Center Level 1 Description HH]	= S.[LVL_1_DESC],
           [Resp Cost Center Level 2 ID HH]	= S.[LVL_2_ID],
           [Resp Cost Center Level 2 Description HH]	= S.[LVL_2_DESC],
           [Resp Cost Center Level 3 ID HH]	= S.[LVL_3_ID],
           [Resp Cost Center Level 3 Description HH]	= S.[LVL_3_DESC],
           [Resp Cost Center Level 4 ID HH]	= S.[LVL_4_ID],
           [Resp Cost Center Level 4 Description HH]	= S.[LVL_4_DESC],
           [Resp Cost Center Level 5 ID HH]	= S.[LVL_5_ID],
           [Resp Cost Center Level 5 Description HH]	= S.[LVL_5_DESC],
           [Resp Cost Center Level 6 ID HH]	= S.[LVL_6_ID],
           [Resp Cost Center Level 6 Description HH]	= S.[LVL_6_DESC],
           [Resp Cost Center Level 7 ID HH]	= S.[LVL_7_ID],
           [Resp Cost Center Level 7 Description HH]	= S.[LVL_7_DESC],
           [Resp Cost Center Level 8 ID HH]	= S.[LVL_8_ID],
           [Resp Cost Center Level 8 Description HH]	= S.[LVL_8_DESC]

    FROM [GLOBAL_EDW].[Propelis].[RESP_COST_CENTER_HORIZONTAL] AS T
    INNER JOIN #SourceData AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
				ISNULL(T.[COST_CNTR_KEY], ''), '|',
                ISNULL(T.[Resp Cost Center Controlling Area ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center ID Leaf level HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 0 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 0 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 1 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 1 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 2 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 2 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 3 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 3 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 4 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 4 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 5 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 5 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 6 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 6 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 7 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 7 Description HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 8 ID HH], ''), '|',
                ISNULL(T.[Resp Cost Center Level 8 Description HH], '')

            )
        ) <> S.HashKey;

    -- Step 3: Insert only new rows not already in target
	
    INSERT INTO [GLOBAL_EDW].[Propelis].[RESP_COST_CENTER_HORIZONTAL]
    (
    [COST_CNTR_KEY],
	[Resp Cost Center Controlling Area ID HH],
	[Resp Cost Center ID Leaf level HH],
	[Resp Cost Center Level 0 ID HH],
	[Resp Cost Center Level 0 Description HH],
	[Resp Cost Center Level 1 ID HH],
	[Resp Cost Center Level 1 Description HH],
	[Resp Cost Center Level 2 ID HH],
	[Resp Cost Center Level 2 Description HH],
	[Resp Cost Center Level 3 ID HH],
	[Resp Cost Center Level 3 Description HH],
	[Resp Cost Center Level 4 ID HH],
	[Resp Cost Center Level 4 Description HH],
	[Resp Cost Center Level 5 ID HH],
	[Resp Cost Center Level 5 Description HH],
	[Resp Cost Center Level 6 ID HH],
	[Resp Cost Center Level 6 Description HH],
	[Resp Cost Center Level 7 ID HH],
	[Resp Cost Center Level 7 Description HH],
	[Resp Cost Center Level 8 ID HH],
	[Resp Cost Center Level 8 Description HH]
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
        FROM [GLOBAL_EDW].[Propelis].[RESP_COST_CENTER_HORIZONTAL] AS T
        WHERE T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    );

END;
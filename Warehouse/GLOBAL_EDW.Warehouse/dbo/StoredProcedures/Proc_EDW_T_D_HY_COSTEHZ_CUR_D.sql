CREATE       PROCEDURE [dbo].[Proc_EDW_T_D_HY_COSTEHZ_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[COST_ELEMNT_KEY]	= S.[COST_ELEMNT_KEY],
        T.[Cost Element Controlling Area ID HH]	= S.[CONTROLLING_AREA_ID],
        T.[Cost Element ID Leaf level HH]	= S.[LEAF_LVL],
        T.[Cost Element Level 0 ID HH]	= S.[LVL_0_ID],
        T.[Cost Element Level 0 Description HH]	= S.[LVL_0_DESC],
        T.[Cost Element Level 1 ID HH]	= S.[LVL_1_ID],
        T.[Cost Element Level 1 Description HH]	= S.[LVL_1_DESC],
        T.[Cost Element Level 2 ID HH]	= S.[LVL_2_ID],
        T.[Cost Element Level 2 Description HH]	= S.[LVL_2_DESC],
        T.[Cost Element Level 3 ID HH]	= S.[LVL_3_ID],
        T.[Cost Element Level 3 Description HH]	= S.[LVL_3_DESC],
        T.[Cost Element Level 4 ID HH]	= S.[LVL_4_ID],
        T.[Cost Element Level 4 Description HH]	= S.[LVL_4_DESC],
        T.[Cost Element Level 5 ID HH]	= S.[LVL_5_ID],
        T.[Cost Element Level 5 Description HH]	= S.[LVL_5_DESC],
        T.[Cost Element Level 6 ID HH]	= S.[LVL_6_ID],
        T.[Cost Element Level 6 Description HH]	= S.[LVL_6_DESC],
        T.[Cost Element Level 7 ID HH]	= S.[LVL_7_ID],
        T.[Cost Element Level 7 Description HH]	= S.[LVL_7_DESC],
        T.[Cost Element Level 8 ID HH]	= S.[LVL_8_ID],
        T.[Cost Element Level 8 Description HH]	= S.[LVL_8_DESC],
        T.[Chart of Accounts]	= S.[CHART_OF_ACCTS]


 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEHZ_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTEHZ_CUR_D] AS S
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEHZ_CUR_D] (
        [COST_ELEMNT_KEY],
        [Cost Element Controlling Area ID HH],
        [Cost Element ID Leaf level HH],
        [Cost Element Level 0 ID HH],
        [Cost Element Level 0 Description HH],
        [Cost Element Level 1 ID HH],
        [Cost Element Level 1 Description HH],
        [Cost Element Level 2 ID HH],
        [Cost Element Level 2 Description HH],
        [Cost Element Level 3 ID HH],
        [Cost Element Level 3 Description HH],
        [Cost Element Level 4 ID HH],
        [Cost Element Level 4 Description HH],
        [Cost Element Level 5 ID HH],
        [Cost Element Level 5 Description HH],
        [Cost Element Level 6 ID HH],
        [Cost Element Level 6 Description HH],
        [Cost Element Level 7 ID HH],
        [Cost Element Level 7 Description HH],
        [Cost Element Level 8 ID HH],
        [Cost Element Level 8 Description HH],
        [Chart of Accounts]
)
    SELECT
       S.[COST_ELEMNT_KEY],
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
       S.[CHART_OF_ACCTS]
       
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTEHZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEHZ_CUR_D] AS T
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY]
    WHERE T.[COST_ELEMNT_KEY] IS NULL;
END
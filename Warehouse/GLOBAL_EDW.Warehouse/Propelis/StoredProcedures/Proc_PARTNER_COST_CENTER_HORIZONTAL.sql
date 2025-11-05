CREATE   PROCEDURE  [Propelis].[Proc_PARTNER_COST_CENTER_HORIZONTAL]
AS
BEGIN
    --------------------------------------------
    -- Step 1: Update existing records
    --------------------------------------------
    UPDATE T
    SET
        [Partner Cost Center Controlling Area ID HH] = S.[CONTROLLING_AREA_ID],
        [Partner Cost Center ID Leaf level HH] = S.[LEAF_LVL],
        [Partner Cost Center Level 0 ID HH] = S.[LVL_0_ID],
        [Partner Cost Center Level 0 Description HH] = S.[LVL_0_DESC],
        [Partner Cost Center Level 1 ID HH] = S.[LVL_1_ID],
        [Partner Cost Center Level 1 Description HH] = S.[LVL_1_DESC],
        [Partner Cost Center Level 2 ID HH] = S.[LVL_2_ID],
        [Partner Cost Center Level 2 Description HH] = S.[LVL_2_DESC],
        [Partner Cost Center Level 3 ID HH] = S.[LVL_3_ID],
        [Partner Cost Center Level 3 Description HH] = S.[LVL_3_DESC],
        [Partner Cost Center Level 4 ID HH] = S.[LVL_4_ID],
        [Partner Cost Center Level 4 Description HH] = S.[LVL_4_DESC],
        [Partner Cost Center Level 5 ID HH] = S.[LVL_5_ID],
        [Partner Cost Center Level 5 Description HH] = S.[LVL_5_DESC],
        [Partner Cost Center Level 6 ID HH] = S.[LVL_6_ID],
        [Partner Cost Center Level 6 Description HH] = S.[LVL_6_DESC],
        [Partner Cost Center Level 7 ID HH] = S.[LVL_7_ID],
        [Partner Cost Center Level 7 Description HH] = S.[LVL_7_DESC],
        [Partner Cost Center Level 8 ID HH] = S.[LVL_8_ID],
        [Partner Cost Center Level 8 Description HH] = S.[LVL_8_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY];

    --------------------------------------------
    -- Step 2: Insert new records
    --------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL] (
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_COST_CENTER_HORIZONTAL] AS T
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE T.[COST_CNTR_KEY] IS NULL;
END;
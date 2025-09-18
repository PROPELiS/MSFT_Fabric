CREATE   PROCEDURE [Propelis].[Proc_PERSON_RESPNSBLE_COST_CNTR_H_HEIR]
AS
BEGIN
    ------------------------------------------------------------------
    -- Step 1: Update existing records
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Person Resp Cost Center Controlling Area ID HH]  = S.[CONTROLLING_AREA_ID],
        T.[Person Resp Cost Center Leaf level HH]           = S.[LEAF_LVL],
        T.[LVL_0_ID]                                        = S.[LVL_0_ID],
        T.[LVL_0_DESC]                                      = S.[LVL_0_DESC],
        T.[Person Resp Cost Center Level 1 ID HH]           = S.[LVL_1_ID],
        T.[Person Resp Cost Center Level 1 Description HH]  = S.[LVL_1_DESC],
        T.[Person Resp Cost Center Level 2 ID HH]           = S.[LVL_2_ID],
        T.[Person Resp Cost Center Level 2 Description HH]  = S.[LVL_2_DESC],
        T.[Person Resp Cost Center Level 3 ID HH]           = S.[LVL_3_ID],
        T.[Person Resp Cost Center Level 3 Description HH]  = S.[LVL_3_DESC],
        T.[Person Resp Cost Center Level 4 ID HH]           = S.[LVL_4_ID],
        T.[Person Resp Cost Center Level 4 Description HH]  = S.[LVL_4_DESC],
        T.[Person Resp Cost Center Level 5 ID HH]           = S.[LVL_5_ID],
        T.[Person Resp Cost Center Level 5 Description HH]  = S.[LVL_5_DESC],
        T.[Person Resp Cost Center Level 6 ID HH]           = S.[LVL_6_ID],
        T.[Person Resp Cost Center Level 6 Description HH]  = S.[LVL_6_DESC],
        T.[Person Resp Cost Center Level 7 ID HH]           = S.[LVL_7_ID],
        T.[Person Resp Cost Center Level 7 Description HH]  = S.[LVL_7_DESC],
        T.[Person Resp Cost Center Level 8 ID HH]           = S.[LVL_8_ID],
        T.[Person Resp Cost Center Level 8 Description HH]  = S.[LVL_8_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY];

    ------------------------------------------------------------------
    -- Step 2: Insert new records (those not already in target)
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR]
    (
        [COST_CNTR_KEY],
        [Person Resp Cost Center Controlling Area ID HH],
        [Person Resp Cost Center Leaf level HH],
        [LVL_0_ID],
        [LVL_0_DESC],
        [Person Resp Cost Center Level 1 ID HH],
        [Person Resp Cost Center Level 1 Description HH],
        [Person Resp Cost Center Level 2 ID HH],
        [Person Resp Cost Center Level 2 Description HH],
        [Person Resp Cost Center Level 3 ID HH],
        [Person Resp Cost Center Level 3 Description HH],
        [Person Resp Cost Center Level 4 ID HH],
        [Person Resp Cost Center Level 4 Description HH],
        [Person Resp Cost Center Level 5 ID HH],
        [Person Resp Cost Center Level 5 Description HH],
        [Person Resp Cost Center Level 6 ID HH],
        [Person Resp Cost Center Level 6 Description HH],
        [Person Resp Cost Center Level 7 ID HH],
        [Person Resp Cost Center Level 7 Description HH],
        [Person Resp Cost Center Level 8 ID HH],
        [Person Resp Cost Center Level 8 Description HH]
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_H_HEIR] T
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE T.[COST_CNTR_KEY] IS NULL;
END
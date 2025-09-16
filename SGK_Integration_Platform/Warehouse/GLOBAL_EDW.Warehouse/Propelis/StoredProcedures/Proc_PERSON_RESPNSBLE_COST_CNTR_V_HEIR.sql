CREATE   PROCEDURE [Propelis].[Proc_PERSON_RESPNSBLE_COST_CNTR_V_HEIR]
AS
BEGIN
    ------------------------------------------------------------------
    -- Step 1: Update existing records
    ------------------------------------------------------------------
    UPDATE T
    SET
        T.[Person Resp Cost Center Controlling Area ID VH]   = S.[CONTROLLING_AREA_ID],
        T.[Person Resp Cost Center Parent ID VH]             = S.[PARNT_ID],
        T.[Person Resp Cost Center Parent Description VH]    = S.[PARNT_DESC],
        T.[Person Resp Cost Center Child ID VH]              = S.[CHILD_ID],
        T.[Person Resp Cost Center Child Description VH]     = S.[CHILD_DESC],
        T.[Person Resp Cost Center Hierarchical Level VH]    = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D] S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY];

    ------------------------------------------------------------------
    -- Step 2: Insert new records (those not already in target)
    ------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR]
    (
        [COST_CNTR_KEY],
        [Person Resp Cost Center Controlling Area ID VH],
        [Person Resp Cost Center Parent ID VH],
        [Person Resp Cost Center Parent Description VH],
        [Person Resp Cost Center Child ID VH],
        [Person Resp Cost Center Child Description VH],
        [Person Resp Cost Center Hierarchical Level VH]
    )
    SELECT
        S.[COST_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[PARNT_ID],
        S.[PARNT_DESC],
        S.[CHILD_ID],
        S.[CHILD_DESC],
        S.[LVL]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR_V_HEIR] T
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE T.[COST_CNTR_KEY] IS NULL;
END
CREATE   PROCEDURE [Propelis].[Proc_COST_CENTER_VERTICAL_HIERARCHY]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Resp Cost Center Controlling Area ID VH] = S.[CONTROLLING_AREA_ID],
        T.[Resp Cost Center Parent ID VH] = S.[PARNT_ID],
        T.[Resp Cost Center Parent Description VH] = S.[PARNT_DESC],
        T.[Resp Cost Center Child ID VH] = S.[CHILD_ID],
        T.[Resp Cost Center Child Description VH] = S.[CHILD_DESC],
        T.[Resp Cost Center Hierarchical Level VH] = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D] S
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY]
    (
        [COST_CNTR_KEY],
        [Resp Cost Center Controlling Area ID VH],
        [Resp Cost Center Parent ID VH],
        [Resp Cost Center Parent Description VH],
        [Resp Cost Center Child ID VH],
        [Resp Cost Center Child Description VH],
        [Resp Cost Center Hierarchical Level VH]
    )
    SELECT
        S.[COST_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[PARNT_ID],
        S.[PARNT_DESC],
        S.[CHILD_ID],
        S.[CHILD_DESC],
        S.[LVL]
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COST_CENTER_VERTICAL_HIERARCHY] T
        ON T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY]
    WHERE T.[COST_CNTR_KEY] IS NULL;
END
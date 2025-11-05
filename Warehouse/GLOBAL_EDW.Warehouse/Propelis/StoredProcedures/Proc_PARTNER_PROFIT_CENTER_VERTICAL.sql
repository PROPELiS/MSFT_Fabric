CREATE   PROCEDURE [Propelis].[Proc_PARTNER_PROFIT_CENTER_VERTICAL]
AS
BEGIN
    SET NOCOUNT ON;

    -- =================================================================
    -- Step 1: Update existing records that match on the PROFIT_CNTR_KEY
    -- =================================================================
    UPDATE T
    SET
        T.[Partner Profit Center Controlling Area ID of VH] = S.[CONTROLLING_AREA_ID],
        T.[Partner Profit Center Parent ID of VH]           = S.[PARNT_ID],
        T.[Partner Profit Center Parent Description of VH]  = S.[PARNT_DESC],
        T.[Partner Profit Center Child ID VH]               = S.[CHILD_ID],
        T.[Partner Profit Center Child ID Description of VH] = S.[CHILD_DESC],
        T.[Partner Profit Center Level of VH]               = S.[LVL]
    FROM
        [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL] T
    INNER JOIN
        [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] S ON T.[PROFIT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    -- =================================================================
    -- Step 2: Insert new records that do not exist in the target
    -- =================================================================
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL] (
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
    FROM
        [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] S
    LEFT JOIN
        [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER_VERTICAL] T ON S.[PROFT_CNTR_KEY] = T.[PROFIT_CNTR_KEY]
    WHERE
        T.[PROFIT_CNTR_KEY] IS NULL;

END;
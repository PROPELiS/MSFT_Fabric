CREATE   PROCEDURE [Propelis].[Proc_Partner_Profit_Center_V_Hier]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Partner Profit Center Controlling Area ID VH] = S.[CONTROLLING_AREA_ID],
        T.[Partner Profit Center Parent ID VH] = S.[PARNT_ID],
        T.[Partner Profit Center Parent Description VH] = S.[PARNT_DESC],
        T.[Partner Profit Center Child ID VH] = S.[CHILD_ID],
        T.[Partner Profit Center Child ID Description VH] = S.[CHILD_DESC],
        T.[Partner Profit Center Level VH] = S.[LVL]
    FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier] (
        [PROFT_CNTR_KEY],
        [Partner Profit Center Controlling Area ID VH],
        [Partner Profit Center Parent ID VH],
        [Partner Profit Center Parent Description VH],
        [Partner Profit Center Child ID VH],
        [Partner Profit Center Child ID Description VH],
        [Partner Profit Center Level VH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
        S.[CONTROLLING_AREA_ID],
        S.[PARNT_ID],
        S.[PARNT_DESC],
        S.[CHILD_ID],
        S.[CHILD_DESC],
        S.[LVL]
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_V_Hier] T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;
END
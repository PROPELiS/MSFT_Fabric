CREATE   PROCEDURE [Propelis].[Proc_Partner_Profit_Center_H_Hier]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[CONTROLLING_AREA_ID] = S.[CONTROLLING_AREA_ID],
        T.[LEAF_LVL] = S.[LEAF_LVL],
        T.[Partner Profit Center Level 0 ID HH] = S.[LVL_0_ID],
        T.[Partner Profit Center Level 0 Description HH] = S.[LVL_0_DESC],
        T.[Partner Profit Center Level 1 ID HH] = S.[LVL_1_ID],
        T.[Partner Profit Center Level 1 Description HH] = S.[LVL_1_DESC],
        T.[Partner Profit Center Level 2 ID HH] = S.[LVL_2_ID],
        T.[Partner Profit Center Level 2 Description HH] = S.[LVL_2_DESC],
        T.[Partner Profit Center Level 3 ID HH] = S.[LVL_3_ID],
        T.[Partner Profit Center Level 3 Description HH] = S.[LVL_3_DESC],
        T.[Partner Profit Center Level 4 ID HH] = S.[LVL_4_ID],
        T.[Partner Profit Center Level 4 Description HH] = S.[LVL_4_DESC],
        T.[Partner Profit Center Level 5 ID HH] = S.[LVL_5_ID],
        T.[Partner Profit Center Level 5 Description HH] = S.[LVL_5_DESC],
        T.[Partner Profit Center Level 6 ID HH] = S.[LVL_6_ID],
        T.[Partner Profit Center Level 6 Description HH] = S.[LVL_6_DESC],
        T.[Partner Profit Center Level 7 ID HH] = S.[LVL_7_ID],
        T.[Partner Profit Center Level 7 Description HH] = S.[LVL_7_DESC],
        T.[Partner Profit Center Level 8 ID HH] = S.[LVL_8_ID],
        T.[Partner Profit Center Level 8 Description HH] = S.[LVL_8_DESC],
        T.[Partner Profit Center Level 9 ID HH] = S.[LVL_9_ID],
        T.[Partner Profit Center Level 9 Description HH] = S.[LVL_9_DESC]
    FROM [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier] (
        [PROFT_CNTR_KEY],
        [CONTROLLING_AREA_ID],
        [LEAF_LVL],
        [Partner Profit Center Level 0 ID HH],
        [Partner Profit Center Level 0 Description HH],
        [Partner Profit Center Level 1 ID HH],
        [Partner Profit Center Level 1 Description HH],
        [Partner Profit Center Level 2 ID HH],
        [Partner Profit Center Level 2 Description HH],
        [Partner Profit Center Level 3 ID HH],
        [Partner Profit Center Level 3 Description HH],
        [Partner Profit Center Level 4 ID HH],
        [Partner Profit Center Level 4 Description HH],
        [Partner Profit Center Level 5 ID HH],
        [Partner Profit Center Level 5 Description HH],
        [Partner Profit Center Level 6 ID HH],
        [Partner Profit Center Level 6 Description HH],
        [Partner Profit Center Level 7 ID HH],
        [Partner Profit Center Level 7 Description HH],
        [Partner Profit Center Level 8 ID HH],
        [Partner Profit Center Level 8 Description HH],
        [Partner Profit Center Level 9 ID HH],
        [Partner Profit Center Level 9 Description HH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
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
        S.[LVL_9_ID],
        S.[LVL_9_DESC]
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Partner_Profit_Center_H_Hier] T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;
END
CREATE   PROCEDURE [Propelis].[Proc_INTER_COMP_PC_VH]
AS
BEGIN
    -- ============================================
    -- Step 1: Update existing records
    -- ============================================
    UPDATE T
    SET
        T.[Inter Comp Profit Center Controlling Area ID VH] = S.[Profit Center Controlling Area ID VH],
        T.[Inter Comp Profit Center Parent ID VH] = S.[Profit Center Parent ID VH],
        T.[Inter Comp Profit Center Parent Description VH] = S.[Profit Center Parent Description VH],
        T.[Inter Comp Profit Center Child ID VH] = S.[Profit Center Child ID VH],
        T.[Inter Comp Profit Center Child ID Description VH] = S.[Profit Center Child ID Description VH],
        T.[Inter Comp Profit Center Level VH] = S.[Profit Center Level VH]
    FROM [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH] AS T
    INNER JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    -- ============================================
    -- Step 2: Insert new records
    -- ============================================
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH]
    (
        [PROFT_CNTR_KEY],
        [Inter Comp Profit Center Controlling Area ID VH],
        [Inter Comp Profit Center Parent ID VH],
        [Inter Comp Profit Center Parent Description VH],
        [Inter Comp Profit Center Child ID VH],
        [Inter Comp Profit Center Child ID Description VH],
        [Inter Comp Profit Center Level VH]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
        S.[Profit Center Controlling Area ID VH],
        S.[Profit Center Parent ID VH],
        S.[Profit Center Parent Description VH],
        S.[Profit Center Child ID VH],
        S.[Profit Center Child ID Description VH],
        S.[Profit Center Level VH]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[INTER_COMP_PC_VH] AS T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;

END;
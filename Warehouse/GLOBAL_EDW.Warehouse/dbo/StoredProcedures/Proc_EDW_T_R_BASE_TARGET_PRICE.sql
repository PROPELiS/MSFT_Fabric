CREATE     PROCEDURE [dbo].[Proc_EDW_T_R_BASE_TARGET_PRICE]
AS
BEGIN
    -- 1. UPDATE existing rows
    UPDATE T
    SET
        T.[Base Target Price Currency] = S.[CURRENCY],
        T.[Base Target Price]          = S.[PRICE],
        T.[Base Target Price Date]     = S.[PRICE_DATE]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICE] AS T   -- target
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_BASE_TARGET_PRICE] AS S  -- source
        ON T.[Base Target Price Sales Order] = S.[SALES_ORDER];

    -- 2. INSERT new rows
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICE]
    (
        [Base Target Price Sales Order],
        [Base Target Price Currency],
        [Base Target Price],
        [Base Target Price Date]
    )
    SELECT
        S.[SALES_ORDER],
        S.[CURRENCY],
        S.[PRICE],
        S.[PRICE_DATE]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_BASE_TARGET_PRICE] AS S  -- source
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICE] AS T   -- target
        ON T.[Base Target Price Sales Order] = S.[SALES_ORDER]
    WHERE T.[Base Target Price Sales Order] IS NULL;
END;
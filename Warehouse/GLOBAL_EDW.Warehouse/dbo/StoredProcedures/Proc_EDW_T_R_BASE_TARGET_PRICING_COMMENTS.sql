CREATE     PROCEDURE [dbo].[Proc_EDW_T_R_BASE_TARGET_PRICING_COMMENTS]
AS
BEGIN
    -- 1. UPDATE existing rows
    UPDATE T
    SET
        T.[Customer Partner Customer ID]            = S.[ID],
        T.[Base Target Pricing Comments Sales Order] = S.[SALES_ORDER],
        T.[Base Target Pricing Comments Code]        = S.[CODE],
        T.[Base Target Pricing Comments Email]       = S.[EMAIL],
        T.[Base Target Pricing Comments]             = S.[COMMENT],
        T.[Base Target Pricing Comments Date Created]= S.[DATE_CREATE]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICING_COMMENTS] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_BASE_TARGET_PRICING_COMMENTS] AS S
        ON T.[Customer Partner Customer ID] = S.[ID];

    -- 2. INSERT new rows
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICING_COMMENTS]
    (
        [Customer Partner Customer ID],
        [Base Target Pricing Comments Sales Order],
        [Base Target Pricing Comments Code],
        [Base Target Pricing Comments Email],
        [Base Target Pricing Comments],
        [Base Target Pricing Comments Date Created]
    )
    SELECT
        S.[ID],
        S.[SALES_ORDER],
        S.[CODE],
        S.[EMAIL],
        S.[COMMENT],
        S.[DATE_CREATE]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_BASE_TARGET_PRICING_COMMENTS] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_R_BASE_TARGET_PRICING_COMMENTS] AS T
        ON T.[Customer Partner Customer ID] = S.[ID]
    WHERE T.[Customer Partner Customer ID] IS NULL;
END;
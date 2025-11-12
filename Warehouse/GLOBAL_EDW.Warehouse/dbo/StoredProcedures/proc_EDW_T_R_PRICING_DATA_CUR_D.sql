CREATE   PROCEDURE [dbo].[proc_EDW_T_R_PRICING_DATA_CUR_D]
AS
BEGIN

    UPDATE T
    SET
        T.[region]                = S.[REGION],
        T.[sales org]             = S.[SALES_ORG],
        T.[sales org name]        = S.[SALES_ORG_NAME],
        T.[sold to]               = S.[SOLD_TO],
        T.[sold to customer name] = S.[SOLD_TO_CUSTOMER_NAME],
        T.[parent number]         = S.[PARENT_NUMBER],
        T.[parent name]           = S.[PARENT_NAME],
        T.[infinity order adj]    = S.[INFINITY_ORDER_ADJ],
        T.[include in hubx]       = S.[INCLUDE_IN_HUBX],
        T.[green]                 = S.[GREEN],
        T.[yellow]                = S.[YELLOW],
        T.[red]                   = S.[RED],
        T.[notes]                 = S.[NOTES]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_R_PRICING_DATA_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_PRICING_DATA_CUR_D] AS S
        ON T.[sold to] = S.[SOLD_TO];   


    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_PRICING_DATA_CUR_D]
    (
        [region],
        [sales org],
        [sales org name],
        [sold to],
        [sold to customer name],
        [parent number],
        [parent name],
        [infinity order adj],
        [include in hubx],
        [green],
        [yellow],
        [red],
        [notes]
    )
    SELECT
        S.[REGION],
        S.[SALES_ORG],
        S.[SALES_ORG_NAME],
        S.[SOLD_TO],
        S.[SOLD_TO_CUSTOMER_NAME],
        S.[PARENT_NUMBER],
        S.[PARENT_NAME],
        S.[INFINITY_ORDER_ADJ],
        S.[INCLUDE_IN_HUBX],
        S.[GREEN],
        S.[YELLOW],
        S.[RED],
        S.[NOTES]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_R_PRICING_DATA_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_R_PRICING_DATA_CUR_D] AS T
        ON T.[sold to] = S.[SOLD_TO]
    WHERE T.[sold to] IS NULL;   

END
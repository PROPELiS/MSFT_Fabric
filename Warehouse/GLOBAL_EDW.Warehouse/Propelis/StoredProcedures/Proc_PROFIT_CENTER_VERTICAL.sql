CREATE    PROCEDURE [Propelis].[Proc_PROFIT_CENTER_VERTICAL]
AS
BEGIN
    
    UPDATE T
    SET
      T.[PROFT_CNTR_KEY]	                          = S.[PROFT_CNTR_KEY],
      T.[Profit Center Controlling Area ID of VH]	  = S.[CONTROLLING_AREA_ID],
      T.[Profit Center Parent ID of VH]	              = S.[PARNT_ID],
      T.[Profit Center Parent Description of VH]	  = S.[PARNT_DESC],
      T.[Profit Center Child ID of VH]	                  = S.[CHILD_ID],
      T.[Profit Center Child ID Description of VH]	  = S.[CHILD_DESC],
      T.[Profit Center Level of VH]	                  = S.[LVL]


    FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    
    INSERT INTO [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL] (
         [PROFT_CNTR_KEY],
         [Profit Center Controlling Area ID of VH],
         [Profit Center Parent ID of VH],
         [Profit Center Parent Description of VH],
         [Profit Center Child ID of VH],
         [Profit Center Child ID Description of VH],
         [Profit Center Level of VH]

    )
    SELECT
         S.[PROFT_CNTR_KEY],
         S.[CONTROLLING_AREA_ID],
         S.[PARNT_ID],
         S.[PARNT_DESC],
         S.[CHILD_ID],
         S.[CHILD_DESC],
         S.[LVL]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_VERTICAL] AS T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;

END
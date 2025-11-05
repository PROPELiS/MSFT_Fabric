CREATE    PROCEDURE [Propelis].[Proc_PROFIT_CENTER_HORIZONTAL]
AS
BEGIN
    
    UPDATE T
    SET
      T.[PROFT_CNTR_KEY]	                         = S.[PROFT_CNTR_KEY],
      T.[Profit Center Controlling Area ID of HH]	 = S.[CONTROLLING_AREA_ID],
      T.[Profit Center Leaf Level of HH]	         = S.[LEAF_LVL],
      T.[Profit Center Level 0 ID of HH]	         = S.[LVL_0_ID],
      T.[Profit Center Level 0 Description of HH]	 = S.[LVL_0_DESC],
      T.[Profit Center Level 1 ID of HH]	         = S.[LVL_1_ID],
      T.[Profit Center Level 1 Description of HH]	 = S.[LVL_1_DESC],
      T.[Profit Center Level 2 ID of HH]	         = S.[LVL_2_ID],
      T.[Profit Center Level 2 Description of HH]	 = S.[LVL_2_DESC],
      T.[Profit Center Level 3 ID of HH]	         = S.[LVL_3_ID],
      T.[Profit Center Level 3 Description of HH]	         = S.[LVL_3_DESC],
      T.[Profit Center Level 4 ID of HH]	         = S.[LVL_4_ID],
      T.[Profit Center Level 4 Description of HH]	         = S.[LVL_4_DESC],
      T.[Profit Center Level 5 ID of HH]	         = S.[LVL_5_ID],
      T.[Profit Center Level 5 Description of HH]	         = S.[LVL_5_DESC],
      T.[Profit Center Level 6 ID of HH]	         = S.[LVL_6_ID],
      T.[Profit Center Level 6 Description of HH]	         = S.[LVL_6_DESC],
      T.[Profit Center Level 7 ID of HH]	         = S.[LVL_7_ID],
      T.[Profit Center Level 7 Description of HH]	         = S.[LVL_7_DESC],
      T.[Profit Center Level 8 ID of HH]	         = S.[LVL_8_ID],
      T.[Profit Center Level 8 Description of HH]	         = S.[LVL_8_DESC],
      T.[Profit Center Level 9 ID of HH]	         = S.[LVL_9_ID],
      T.[Profit Center Level 9 Description of HH]	         = S.[LVL_9_DESC]



    FROM [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

    
    INSERT INTO [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL] (
         [PROFT_CNTR_KEY],
         [Profit Center Controlling Area ID of HH],
         [Profit Center Leaf Level of HH],
         [Profit Center Level 0 ID of HH],
         [Profit Center Level 0 Description of HH],
         [Profit Center Level 1 ID of HH],
         [Profit Center Level 1 Description of HH],
         [Profit Center Level 2 ID of HH],
         [Profit Center Level 2 Description of HH],
         [Profit Center Level 3 ID of HH],
         [Profit Center Level 3 Description of HH],
         [Profit Center Level 4 ID of HH],
         [Profit Center Level 4 Description of HH],
         [Profit Center Level 5 ID of HH],
         [Profit Center Level 5 Description of HH],
         [Profit Center Level 6 ID of HH],
         [Profit Center Level 6 Description of HH],
         [Profit Center Level 7 ID of HH],
         [Profit Center Level 7 Description of HH],
         [Profit Center Level 8 ID of HH],
         [Profit Center Level 8 Description of HH],
         [Profit Center Level 9 ID of HH],
         [Profit Center Level 9 Description of HH]
		 
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

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PROFIT_CENTER_HORIZONTAL] AS T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;

END
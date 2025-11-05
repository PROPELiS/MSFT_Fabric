CREATE PROCEDURE [dbo].[Proc_EDW_T_D_HY_BPCGLHZ_CUR_D]
AS  
BEGIN  
    -- Update existing records
    UPDATE T
    SET
      T.[BPC GL Account ID]=S.[ACCT_ID],
      T.[BPC GL Level 1 ID]=S.[LVL_1_ID],
      T.[BPC GL Level 1 Description]=S.[LVL_1_DESC],
      T.[BPC GL Level 2 ID]=S.[LVL_2_ID],
      T.[BPC GL Level 2 Description]=S.[LVL_2_DESC],
      T.[BPC GL Level 3 ID]=S.[LVL_3_ID],
      T.[BPC GL Level 3 Description]=S.[LVL_3_DESC],
      T.[BPC GL Level 4 ID]=S.[LVL_4_ID],
      T.[BPC GL Level 4 Description]=S.[LVL_4_DESC],
      T.[BPC GL Level 5 ID]=S.[LVL_5_ID],
      T.[BPC GL Level 5 Description]=S.[LVL_5_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_BPCGLHZ_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_HY_BPCGLHZ_CUR_D] AS S
        ON T.[BPC GL Account ID] = S.[ACCT_ID];

    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_BPCGLHZ_CUR_D]  (
         [BPC GL Account ID],
         [BPC GL Level 1 ID],
         [BPC GL Level 1 Description],
         [BPC GL Level 2 ID],
         [BPC GL Level 2 Description],
         [BPC GL Level 3 ID],
         [BPC GL Level 3 Description],
         [BPC GL Level 4 ID],
         [BPC GL Level 4 Description],
         [BPC GL Level 5 ID],
         [BPC GL Level 5 Description]

    )
    SELECT
        S.[ACCT_ID],
        S.[LVL_1_ID],
        S.[LVL_1_DESC],
        S.[LVL_2_ID],
        S.[LVL_2_DESC],
        S.[LVL_3_ID],
        S.[LVL_3_DESC],
        S.[LVL_4_ID],
        S.[LVL_4_DESC],
        S.[LVL_5_ID],
        S.[LVL_5_DESC]

    FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_HY_BPCGLHZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_BPCGLHZ_CUR_D]  AS T
        ON T.[BPC GL Account ID] = S.[ACCT_ID]
    WHERE T.[BPC GL Account ID] IS NULL;
END;
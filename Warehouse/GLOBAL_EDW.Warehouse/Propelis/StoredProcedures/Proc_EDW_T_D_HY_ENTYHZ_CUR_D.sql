CREATE   PROCEDURE [Propelis].[Proc_EDW_T_D_HY_ENTYHZ_CUR_D] 
AS
BEGIN 

 
UPDATE T 
SET 
T.[Entity ID] = S.[ENTTY_ID],
T.[Entity Level] = S.[LEAF_LVL],
T.[Lvl 0 Id] = S.[LVL_0_ID],
T.[Entity Level 1] = S.[LVL_1_ID],
T.[Entity Level 2] = S.[LVL_2_ID],
T.[Entity Level 3] = S.[LVL_3_ID],
T.[Entity Level 4] = S.[LVL_4_ID]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] T 
INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_ENTYHZ_CUR_D] S 
ON T.[Entity Level] = S.[LEAF_LVL];

INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D]
(
[Entity ID],
[Entity Level],
[Lvl 0 Id],
[Entity Level 1],
[Entity Level 2],
[Entity Level 3],
[Entity Level 4] 
)
SELECT 
S.[ENTTY_ID],
S.[LEAF_LVL],
S.[LVL_0_ID],
S.[LVL_1_ID],
S.[LVL_2_ID],
S.[LVL_3_ID],
S.[LVL_4_ID]
FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_ENTYHZ_CUR_D] S
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] T
ON S.[LEAF_LVL] = T.[Entity Level]
WHERE T.[Entity Level] IS NULL;

END;
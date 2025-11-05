CREATE PROCEDURE [Propelis].[Proc_EDW_T_D_HY_ENTYHZ_CUR_D] 
AS
BEGIN 
 
UPDATE T 
SET 
T.[Entity ID] = S.[Entity ID],
T.[Entity Level] = S.[Entity Level],
T.[Lvl 0 Id] = S.[Lvl 0 Id],
T.[Entity Level 1] = S.[Entity Level 1],
T.[Entity Level 2] = S.[Entity Level 2],
T.[Entity Level 3] = S.[Entity Level 3],
T.[Entity Level 4] = S.[Entity Level 4]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] T 
INNER JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] S 
ON T.[Entity Level] = S.[Entity Level];

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
S.[Entity ID],
S.[Entity Level],
S.[Lvl 0 Id],
S.[Entity Level 1],
S.[Entity Level 2],
S.[Entity Level 3],
S.[Entity Level 4]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] S
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_ENTYHZ_CUR_D] T
ON S.[Entity Level] = T.[Entity Level]
WHERE T.[Entity Level] IS NULL;

END;
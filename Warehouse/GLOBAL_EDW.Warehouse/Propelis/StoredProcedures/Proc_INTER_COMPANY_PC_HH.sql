CREATE PROCEDURE [Propelis].[Proc_INTER_COMPANY_PC_HH] 
AS
BEGIN

    -- Update existing records
    UPDATE T
    SET  
	T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY] ,
	T.[Profit Center Controlling Area ID HH] = S.[Profit Center Controlling Area ID HH] ,
	T.[Profit Center Leaf Level HH] = S.[Profit Center Leaf Level HH] ,
	T.[Profit Center Level 0 ID HH] = S.[Profit Center Level 0 ID HH],
	T.[Profit Center Level 0 Description HH] = S.[Profit Center Level 0 Description HH] ,
	T.[Inter Comp Profit Center Level 1 ID HH] = S.[Profit Center Level 1 ID HH] ,
	T.[Inter Comp Profit Center Level 1 Description HH] = S.[Profit Center Level 1 Description HH],
	T.[Inter Comp Profit Center Level 2 ID HH] = S.[Profit Center Level 2 ID HH] ,
	T.[Inter Comp Profit Center Level 2 Description HH] = S.[Profit Center Level 2 Description HH] ,
	T.[Inter Comp Profit Center Level 3 ID HH] = S.[Profit Center Level 3 ID HH] ,
	T.[Inter Comp Profit Center Level 3 Description HH] = S.[Profit Center Level 3 Description HH],
	T.[Inter Comp Profit Center Level 4 ID HH] = S.[Profit Center Level 4 ID HH],
	T.[Inter Comp Profit Center Level 4 Description HH] = S.[Profit Center Level 4 Description HH] ,
	T.[Inter Comp Profit Center Level 5 ID HH] = S.[Profit Center Level 5 ID HH] ,
	T.[Inter Comp Profit Center Level 5 Description HH] = S.[Profit Center Level 5 Description HH],
	T.[Inter Comp Profit Center Level 6 ID HH] = S.[Profit Center Level 6 ID HH],
	T.[Inter Comp Profit Center Level 6 Description HH] = S.[Profit Center Level 6 Description HH],
	T.[Inter Comp Profit Center Level 7 ID HH] = S.[Profit Center Level 7 ID HH] ,
	T.[Inter Comp Profit Center Level 7 Description HH] = S.[Profit Center Level 7 Description HH] ,
	T.[Inter Comp Profit Center Level 8 ID HH] = S.[Profit Center Level 8 ID HH] ,
	T.[Inter Comp Profit Center Level 8 Description HH] = S.[Profit Center Level 8 Description HH] ,
	T.[Inter Comp Profit Center Level 9 ID HH] = S.[Profit Center Level 9 ID HH] ,
	T.[Inter Comp Profit Center Level 9 Description HH] = S.[Profit Center Level 9 Description HH]            
  FROM [GLOBAL_EDW].[Propelis].[INTER_COMPANY_PC_HH]  T 
  INNER JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] S 
  ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];
  
   -- Insert new records
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMPANY_PC_HH] 
    (
		[PROFT_CNTR_KEY],
		[Profit Center Controlling Area ID HH],
		[Profit Center Leaf Level HH],
		[Profit Center Level 0 ID HH],
		[Profit Center Level 0 Description HH],
		[Inter Comp Profit Center Level 1 ID HH],
		[Inter Comp Profit Center Level 1 Description HH],
		[Inter Comp Profit Center Level 2 ID HH],
		[Inter Comp Profit Center Level 2 Description HH],
		[Inter Comp Profit Center Level 3 ID HH],
		[Inter Comp Profit Center Level 3 Description HH],
		[Inter Comp Profit Center Level 4 ID HH],
		[Inter Comp Profit Center Level 4 Description HH],
		[Inter Comp Profit Center Level 5 ID HH],
		[Inter Comp Profit Center Level 5 Description HH],
		[Inter Comp Profit Center Level 6 ID HH],
		[Inter Comp Profit Center Level 6 Description HH],
		[Inter Comp Profit Center Level 7 ID HH],
		[Inter Comp Profit Center Level 7 Description HH],
		[Inter Comp Profit Center Level 8 ID HH],
		[Inter Comp Profit Center Level 8 Description HH],
		[Inter Comp Profit Center Level 9 ID HH],
		[Inter Comp Profit Center Level 9 Description HH]
)

SELECT 
		S.[PROFT_CNTR_KEY] ,
		S.[Profit Center Controlling Area ID HH] ,
		S.[Profit Center Leaf Level HH] ,
		S.[Profit Center Level 0 ID HH],
		S.[Profit Center Level 0 Description HH] ,
		S.[Profit Center Level 1 ID HH] ,
		S.[Profit Center Level 1 Description HH],
		S.[Profit Center Level 2 ID HH] ,
		S.[Profit Center Level 2 Description HH] ,
		S.[Profit Center Level 3 ID HH] ,
		S.[Profit Center Level 3 Description HH],
		S.[Profit Center Level 4 ID HH],
		S.[Profit Center Level 4 Description HH] ,
		S.[Profit Center Level 5 ID HH] ,
		S.[Profit Center Level 5 Description HH],
		S.[Profit Center Level 6 ID HH],
		S.[Profit Center Level 6 Description HH],
		S.[Profit Center Level 7 ID HH] ,
		S.[Profit Center Level 7 Description HH] ,
		S.[Profit Center Level 8 ID HH] ,
		S.[Profit Center Level 8 Description HH] ,
		S.[Profit Center Level 9 ID HH] ,
		S.[Profit Center Level 9 Description HH]          
 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] S
 LEFT JOIN  [GLOBAL_EDW].[Propelis].[INTER_COMPANY_PC_HH] T
 ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
 WHERE T.[PROFT_CNTR_KEY] IS NULL;
 END;
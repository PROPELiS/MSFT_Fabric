CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_HY_COSTCHZ_CUR_D]
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
	
    TRUNCATE TABLE [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D];
 
    -- Step 2: Load data from Mirrored Database table into Warehouse target table
	
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D]
    (
[COST_CNTR_KEY],
[CONTROLLING_AREA_ID],
[Cost Center ID Leaf level HH],
[LVL_0_ID],
[LVL_0_DESC],
[Cost Center Level 1 ID HH],
[Cost Center Level 1 Description HH],
[Cost Center Level 2 ID HH],
[Cost Center Level 2 Description HH],
[Cost Center Level 3 ID HH],
[Cost Center Level 3 Description HH],
[Cost Center Level 4 ID HH],
[Cost Center Level 4 Description HH],
[Cost Center Level 5 ID HH],
[Cost Center Level 5 Description HH],
[Cost Center Level 6 ID HH],
[Cost Center Level 6 Description HH],
[Cost Center Level 7 ID HH],
[Cost Center Level 7 Description HH],
[Cost Center Level 8 ID HH],
[Cost Center Level 8 Description HH]

)
SELECT 

[COST_CNTR_KEY],
[CONTROLLING_AREA_ID],
[LEAF_LVL],
[LVL_0_ID],
[LVL_0_DESC],
[LVL_1_ID],
[LVL_1_DESC],
[LVL_2_ID],
[LVL_2_DESC],
[LVL_3_ID],
[LVL_3_DESC],
[LVL_4_ID],
[LVL_4_DESC],
[LVL_5_ID],
[LVL_5_DESC],
[LVL_6_ID],
[LVL_6_DESC],
[LVL_7_ID],
[LVL_7_DESC],
[LVL_8_ID],
[LVL_8_DESC]

FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D];
 
-- Step 3: Return success message
    PRINT 'Succeeded';
END;
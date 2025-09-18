CREATE   PROCEDURE [dbo].[Proc_EDW_T_R_MST_SGK_STNDRD_TXT_CATG_CUR_D]
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
	
    TRUNCATE TABLE [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_STNDRD_TXT_CATG_CUR_D];
 
    -- Step 2: Load data from Mirrored Database table into Warehouse target table
	
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_STNDRD_TXT_CATG_CUR_D]
    (
        [Operation Description],
        [STANDARD_TEXT_KEY]  ,  
        [SSP Preferred Task],   
        [Activity Group]   ,    
        [CSR Tasks]        ,    
        [Activity With Status] ,
        [FY 2023 SS Tasks],     
        [All Tasks]   ,         
        [Automation Groups]  
    )
SELECT 
[OPERATION_DESCRIPTION] ,
[STANDARD_TEXT_KEY] 	,
[FY2023_SS_TASKS] 		,
[SSP_PREFERRED_TASK] 	,
[CSR_TASKS] 			,
[ALL_TASKS] 			,
[ACTIVITY_GROUP] 		,
[ACTIVITY_WITH_STATUS] 	,
[AUTOMATION_GROUPS] 	



FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_R_MST_SGK_STNDRD_TXT_CATG_CUR_D];
 
-- Step 3: Return success message
    PRINT 'Succeeded';
END;
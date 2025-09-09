CREATE   PROCEDURE dbo.Proc_EDW_T_R_SGK_OPERREG_CUR_D
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D]
    (
        [Plant Business Function],
        [Plant Region],
        [PLANT]
    )
    SELECT
        [BUSINESS_FUNCTION],
        [REGION],
        [PLANT]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
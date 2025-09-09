CREATE   PROCEDURE [Propelis].[Proc_OPERATING_PLANT_REGION]
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[OPERATING_PLANT_REGION];

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[OPERATING_PLANT_REGION]
    (
        [Operating Plant Business Function],
        [Operating Plant Region],
        [Operating Plant]
    )
    SELECT
        [BUSINESS_FUNCTION],
        [REGION],
        [PLANT]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
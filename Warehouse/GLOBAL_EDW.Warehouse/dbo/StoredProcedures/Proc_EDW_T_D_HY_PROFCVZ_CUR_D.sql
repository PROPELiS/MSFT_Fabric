CREATE     PROCEDURE dbo.Proc_EDW_T_D_HY_PROFCVZ_CUR_D
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE dbo.EDW_T_D_HY_PROFCVZ_CUR_D;

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    INSERT INTO dbo.EDW_T_D_HY_PROFCVZ_CUR_D
    (
        [PROFT_CNTR_KEY],
        [Profit Center Controlling Area ID VH],
        [Profit Center Parent ID VH],
        [Profit Center Parent Description VH],
        [Profit Center Child ID VH],
        [Profit Center Child ID Description VH],
        [Profit Center Level VH]
    )
    SELECT
        PROFT_CNTR_KEY,
        CONTROLLING_AREA_ID,
        PARNT_ID,
        PARNT_DESC,
        CHILD_ID,
        CHILD_DESC,
        LVL
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCVZ_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
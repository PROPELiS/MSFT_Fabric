CREATE     PROCEDURE dbo.Proc_EDW_T_D_HY_WORKCVZ_CUR_D
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE dbo.EDW_T_D_HY_WORKCVZ_CUR_D;

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    INSERT INTO dbo.EDW_T_D_HY_WORKCVZ_CUR_D
    (
        [WRK_CNTR_KEY],
        [Work Center Plant ID],
        [Work Center Parent ID VH],
        [Work Center Parent Description VH],
        [Work Center Child ID VH],
        [Work Center Child Description VH],
        [Work Center Level VH]
    )
    SELECT
        WRK_CNTR_KEY,
        PLNT_ID,
        PARNT_ID,
        PARNT_DESC,
        CHILD_ID,
        CHILD_DESC,
        LVL
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_WORKCVZ_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
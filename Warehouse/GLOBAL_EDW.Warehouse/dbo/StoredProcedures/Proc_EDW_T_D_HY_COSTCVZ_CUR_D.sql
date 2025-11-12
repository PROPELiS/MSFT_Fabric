CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_HY_COSTCVZ_CUR_D]
AS
BEGIN
    -- Step 1: Remove existing data from the Warehouse target table
    TRUNCATE TABLE [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D];

    -- Step 2: Load data from the Lakehouse/Mirroring source table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D]
    (
        [COST_CNTR_KEY],
        [Cost Center Controlling Area ID VH],
        [Cost Center Parent ID VH],
        [Cost Center Parent Description VH],
        [Cost Center Child ID VH],
        [Cost Center Child Description VH],
        [Cost Center Hierarchical Level VH]
    )
    SELECT
        [COST_CNTR_KEY],
        [CONTROLLING_AREA_ID],
        [PARNT_ID],
        [PARNT_DESC],
        [CHILD_ID],
        [CHILD_DESC],
        [LVL]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTCVZ_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded: Data loaded into Warehouse table [EDW_T_D_HY_COSTCVZ_CUR_D]';
END;
CREATE   PROCEDURE dbo.Proc_EDW_T_D_HY_WORKCHZ_CUR_D
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE dbo.EDW_T_D_HY_WORKCHZ_CUR_D;

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    INSERT INTO dbo.EDW_T_D_HY_WORKCHZ_CUR_D
    (
        [WRK_CNTR_KEY],
        [Work Center Plant ID],
        [Work Center Leaf Level HH],
        [Work Center Level 0 ID HH],
        [Work Center Level 0 Description HH],
        [Work Center Level 1 ID HH],
        [Work Center Level 1 Description HH],
        [Work Center Level 2 ID HH],
        [Work Center Level 2 Description HH],
        [Work Center Level 3 ID HH],
        [Work Center Level 3 Description HH],
        [Work Center Level 4 ID HH],
        [Work Center Level 4 Description HH],
        [Work Center Level 5 ID HH],
        [Work Center Level 5 Description HH],
        [Work Center Level 6 ID HH],
        [Work Center Level 6 Description HH],
        [Work Center Level 7 ID HH],
        [Work Center Level 7 Description HH],
        [Work Center Level 8 ID HH],
        [Work Center Level 8 Description HH]
    )
    SELECT
        WRK_CNTR_KEY,
        PLNT_ID,
        LEAF_LVL,
        LVL_0_ID,
        LVL_0_DESC,
        LVL_1_ID,
        LVL_1_DESC,
        LVL_2_ID,
        LVL_2_DESC,
        LVL_3_ID,
        LVL_3_DESC,
        LVL_4_ID,
        LVL_4_DESC,
        LVL_5_ID,
        LVL_5_DESC,
        LVL_6_ID,
        LVL_6_DESC,
        LVL_7_ID,
        LVL_7_DESC,
        LVL_8_ID,
        LVL_8_DESC
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_HY_WORKCHZ_CUR_D];

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
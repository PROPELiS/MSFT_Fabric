CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_HY_PROFCHZ_CUR_D-Clone]
AS
BEGIN
    -- Step 1: Remove all existing data from Warehouse target table
    TRUNCATE TABLE [dbo].[EDW_T_D_HY_PROFCHZ_CUR_D-Clone];

    -- Step 2: Load data from Lakehouse source table into Warehouse target table
    ;WITH RankedData AS
    (
        SELECT
            PROFT_CNTR_KEY,
            CONTROLLING_AREA_ID,
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
            LVL_8_DESC,
            LVL_9_ID,
            LVL_9_DESC,
            ROW_NUMBER() OVER (
                PARTITION BY PROFT_CNTR_KEY
                ORDER BY LEAF_LVL DESC
            ) AS rn
        FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_PROFCHZ_CUR_D]
    )
    INSERT INTO [dbo].[EDW_T_D_HY_PROFCHZ_CUR_D-Clone]
    (
        [PROFT_CNTR_KEY],
        [Profit Center Controlling Area ID HH],
        [Profit Center Leaf Level HH],
        [Profit Center Level 0 ID HH],
        [Profit Center Level 0 Description HH],
        [Profit Center Level 1 ID HH],
        [Profit Center Level 1 Description HH],
        [Profit Center Level 2 ID HH],
        [Profit Center Level 2 Description HH],
        [Profit Center Level 3 ID HH],
        [Profit Center Level 3 Description HH],
        [Profit Center Level 4 ID HH],
        [Profit Center Level 4 Description HH],
        [Profit Center Level 5 ID HH],
        [Profit Center Level 5 Description HH],
        [Profit Center Level 6 ID HH],
        [Profit Center Level 6 Description HH],
        [Profit Center Level 7 ID HH],
        [Profit Center Level 7 Description HH],
        [Profit Center Level 8 ID HH],
        [Profit Center Level 8 Description HH],
        [Profit Center Level 9 ID HH],
        [Profit Center Level 9 Description HH]
    )
    SELECT
        PROFT_CNTR_KEY,
        CONTROLLING_AREA_ID,
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
        LVL_8_DESC,
        LVL_9_ID,
        LVL_9_DESC
    FROM RankedData
    WHERE rn = 1;  -- keep only the row with max LEAF_LVL per PROFT_CNTR_KEY

    -- Step 3: Return success message
    PRINT 'Succeeded';
END;
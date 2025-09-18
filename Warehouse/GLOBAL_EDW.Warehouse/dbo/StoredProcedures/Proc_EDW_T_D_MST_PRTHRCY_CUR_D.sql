CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_PRTHRCY_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Product Hierarchy]                             = S.[PRDT_HIERCHY_CD],
        T.[Product Hierarchy Description]                 = S.[PRDT_HIERCHY_DESC],
        T.[Product Hier Level6 Product Category Code]     = S.[LEVEL6_PRDT_CTGRY_CD],
        T.[Product Hier Level6 Product Category Description] = S.[LEVEL6_PRDT_CTGRY_DESC],
        T.[Product Hier Level5 Product Category Code]     = S.[LEVEL5_PRDT_CTGRY_CD],
        T.[Product Hier Level5 Product Category Description] = S.[LEVEL5_PRDT_CTGRY_DESC],
        T.[Product Hier Level4 Product Category Code]     = S.[LEVEL4_PRDT_CTGRY_CD],
        T.[Product Hier Level4 Product Category Description] = S.[LEVEL4_PRDT_CTGRY_DESC],
        T.[Product Hier Level3 Sub Family Code]           = S.[LEVEL3_SUB_FAMILY_CD],
        T.[Product Hier Level3 Sub Family Description]    = S.[LEVEL3_SUB_FAMILY_DESC],
        T.[Product Hier Level2 Family Code]               = S.[LEVEL2_FAMILY_CD],
        T.[Product Hier Level2 Family Description]        = S.[LEVEL2_FAMILY_DESC],
        T.[Product Hier Level1 Class Code]                = S.[LEVEL1_CLASS_CD],
        T.[Product Hier Level1 Class Description]         = S.[LEVEL1_CLASS_DESC],
        T.[ETL_SRC_SYS_CD]                                = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                        = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                          = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                              = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                                = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                                  = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] S
        ON T.[Product Hierarchy] = S.[PRDT_HIERCHY_CD];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] (
        [Product Hierarchy],
        [Product Hierarchy Description],
        [Product Hier Level6 Product Category Code],
        [Product Hier Level6 Product Category Description],
        [Product Hier Level5 Product Category Code],
        [Product Hier Level5 Product Category Description],
        [Product Hier Level4 Product Category Code],
        [Product Hier Level4 Product Category Description],
        [Product Hier Level3 Sub Family Code],
        [Product Hier Level3 Sub Family Description],
        [Product Hier Level2 Family Code],
        [Product Hier Level2 Family Description],
        [Product Hier Level1 Class Code],
        [Product Hier Level1 Class Description],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[PRDT_HIERCHY_CD],
        S.[PRDT_HIERCHY_DESC],
        S.[LEVEL6_PRDT_CTGRY_CD],
        S.[LEVEL6_PRDT_CTGRY_DESC],
        S.[LEVEL5_PRDT_CTGRY_CD],
        S.[LEVEL5_PRDT_CTGRY_DESC],
        S.[LEVEL4_PRDT_CTGRY_CD],
        S.[LEVEL4_PRDT_CTGRY_DESC],
        S.[LEVEL3_SUB_FAMILY_CD],
        S.[LEVEL3_SUB_FAMILY_DESC],
        S.[LEVEL2_FAMILY_CD],
        S.[LEVEL2_FAMILY_DESC],
        S.[LEVEL1_CLASS_CD],
        S.[LEVEL1_CLASS_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] T
        ON T.[Product Hierarchy] = S.[PRDT_HIERCHY_CD]
    WHERE T.[Product Hierarchy] IS NULL;
END
CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_MST_ACTTYPE_CUR_D]
AS
BEGIN  

    -- Step 1: Update existing records
    UPDATE T
    SET
        T.[Activity Type ID]                      = S.[ACTIVITY_TYP_ID],
        T.[Activity Type Description]             = S.[ACTIVITY_TYP_DESC],
        T.[Activity Type Description 2]           = S.[ACTIVITY_TYP_DESC_2],
        T.[Activity Type Controlling Area]        = S.[CONTROLLING_AREA],
        T.[Activity Type Unit]                    = S.[ACTIVITY_UNIT],
        T.[Activity Type Category]                = S.[ACTIVITY_TYP_CTGRY],
        T.[ETL_SRC_SYS_CD]                        = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                  = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                      = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                        = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                          = S.[ETL_UPDTD_TS],
        T.[Activity Type Controlling Area Description] = S.[CONTROLLING_AREA_DESC],
        T.[Activity Type Category Description]    = S.[ACTIVITY_TYP_CTGRY_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_ACTTYPE_CUR_D] S
        ON T.[ACTIVITY_TYP_KEY] = S.[ACTIVITY_TYP_KEY];

    -- Step 2: Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] (
        [ACTIVITY_TYP_KEY],
        [Activity Type ID],
        [Activity Type Description],
        [Activity Type Description 2],
        [Activity Type Controlling Area],
        [Activity Type Unit],
        [Activity Type Category],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Activity Type Controlling Area Description],
        [Activity Type Category Description]
    )
    SELECT
        S.[ACTIVITY_TYP_KEY],
        S.[ACTIVITY_TYP_ID],
        S.[ACTIVITY_TYP_DESC],
        S.[ACTIVITY_TYP_DESC_2],
        S.[CONTROLLING_AREA],
        S.[ACTIVITY_UNIT],
        S.[ACTIVITY_TYP_CTGRY],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[CONTROLLING_AREA_DESC],
        S.[ACTIVITY_TYP_CTGRY_DESC]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_ACTTYPE_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] T
        ON T.[ACTIVITY_TYP_KEY] = S.[ACTIVITY_TYP_KEY]
    WHERE T.[ACTIVITY_TYP_KEY] IS NULL;
END
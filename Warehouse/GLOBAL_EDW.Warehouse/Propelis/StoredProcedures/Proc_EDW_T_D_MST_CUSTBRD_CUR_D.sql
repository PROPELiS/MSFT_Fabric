CREATE   PROCEDURE [Propelis].[Proc_EDW_T_D_MST_CUSTBRD_CUR_D]
AS
BEGIN

    ----------------------------------------------------------------------
    -- Step 1: Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET 
        T.[CUST_ID] = S.[CUST_ID],
        T.[CTGRY_CD] = S.[CTGRY_CD],
        T.[CTGRY_DESC] = S.[CTGRY_DESC],
        T.[BRAND_ID] = S.[BRAND_ID],
        T.[BRAND_DESC] = S.[BRAND_DESC],
        T.[SUB_BRAND_ID] = S.[SUB_BRAND_ID],
        T.[SUB_BRAND_DESC] = S.[SUB_BRAND_DESC],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[EDW_T_D_MST_CUSTBRD_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR_OLD].[dbo].[EDW_T_D_MST_CUSTBRD_CUR_D] S
        ON T.[CUST_BRAND_HIERCHY_KEY] = S.[CUST_BRAND_HIERCHY_KEY];

    ----------------------------------------------------------------------
    -- Step 2: Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_D_MST_CUSTBRD_CUR_D] 
    (
        [CUST_BRAND_HIERCHY_KEY],
        [CUST_ID],
        [CTGRY_CD],
        [CTGRY_DESC],
        [BRAND_ID],
        [BRAND_DESC],
        [SUB_BRAND_ID],
        [SUB_BRAND_DESC],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT 
        S.[CUST_BRAND_HIERCHY_KEY],
        S.[CUST_ID],
        S.[CTGRY_CD],
        S.[CTGRY_DESC],
        S.[BRAND_ID],
        S.[BRAND_DESC],
        S.[SUB_BRAND_ID],
        S.[SUB_BRAND_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_MIRROR_OLD].[dbo].[EDW_T_D_MST_CUSTBRD_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[EDW_T_D_MST_CUSTBRD_CUR_D] T
        ON T.[CUST_BRAND_HIERCHY_KEY] = S.[CUST_BRAND_HIERCHY_KEY]
    WHERE T.[CUST_BRAND_HIERCHY_KEY] IS NULL;

END
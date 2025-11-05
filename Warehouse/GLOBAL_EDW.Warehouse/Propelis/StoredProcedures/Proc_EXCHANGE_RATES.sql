CREATE   PROCEDURE [Propelis].[Proc_EXCHANGE_RATES]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records in target table
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[EXCHG_RT_TYP_DESC] = S.[EXCHG_RT_TYP_DESC],
        T.[From Currency] = S.[FROM_CRNCY],
        T.[TO_CRNCY] = S.[TO_CRNCY],
        T.[Valid From Date] = S.[VALID_FROM_DATE],
        T.[Exchange Rate] = S.[EXCHG_RT],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[EXCHANGE_RATES] T
    INNER JOIN [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_EXCHGRT_CUR_D] S
        ON T.[EXCHG_RT_TYP] = S.[EXCHG_RT_TYP];

    ----------------------------------------------------------------------
    -- Insert new records into target table
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[EXCHANGE_RATES] (
        [EXCHG_RT_TYP],
        [EXCHG_RT_TYP_DESC],
        [From Currency],
        [TO_CRNCY],
        [Valid From Date],
        [Exchange Rate],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[EXCHG_RT_TYP],
        S.[EXCHG_RT_TYP_DESC],
        S.[FROM_CRNCY],
        S.[TO_CRNCY],
        S.[VALID_FROM_DATE],
        S.[EXCHG_RT],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_EXCHGRT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[EXCHANGE_RATES] T
        ON T.[EXCHG_RT_TYP] = S.[EXCHG_RT_TYP]
    WHERE T.[EXCHG_RT_TYP] IS NULL;
END
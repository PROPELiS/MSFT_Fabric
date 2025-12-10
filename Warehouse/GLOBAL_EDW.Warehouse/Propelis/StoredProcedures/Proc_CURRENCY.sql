CREATE   PROCEDURE [Propelis].[Proc_CURRENCY]
AS
BEGIN
    

    UPDATE T
    SET 
	      T.[CRNCY_KEY]                       = S.[CRNCY_KEY],
          T.[Currency Code]                   = S.[CRNCY_CD],
          T.[ETL_SRC_SYS_CD]                  = S.[ETL_SRC_SYS_CD],
          T.[ETL_EFFECTV_BEGIN_DATE]          = S.[ETL_EFFECTV_BEGIN_DATE],
          T.[ETL_EFFECTV_END_DATE]            = S.[ETL_EFFECTV_END_DATE],
          T.[ETL_CURR_RCD_IND]                = S.[ETL_CURR_RCD_IND],
          T.[ETL_CREATED_TS]                  = S.[ETL_CREATED_TS],
          T.[ETL_UPDTD_TS]                    = S.[ETL_UPDTD_TS],
          T.[Currency Short Description]      = S.[SHORT_DESC],
          T.[Currency Long Description]       = S.[LONG_DESC]
    FROM [GLOBAL_EDW].[Propelis].[CURRENCY] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CRNCY_CUR_D] AS S
        ON T.[CRNCY_KEY] = S.[CRNCY_KEY];

    INSERT INTO [GLOBAL_EDW].[Propelis].[CURRENCY]
    (
        [CRNCY_KEY],
        [Currency Code],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Currency Short Description],
        [Currency Long Description]
    )
    SELECT
        S.[CRNCY_KEY],
        S.[CRNCY_CD],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[SHORT_DESC],
        S.[LONG_DESC]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CRNCY_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CURRENCY] AS T
        ON T.[CRNCY_KEY] = S.[CRNCY_KEY]
    WHERE T.[CRNCY_KEY] IS NULL;

END;
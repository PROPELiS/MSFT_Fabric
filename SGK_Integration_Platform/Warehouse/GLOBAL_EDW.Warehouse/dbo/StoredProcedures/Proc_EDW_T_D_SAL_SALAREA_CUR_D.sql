CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_SAL_SALAREA_CUR_D]
AS
BEGIN
    -- Step 1: Update existing records in target table
    UPDATE T
    SET
        T.[Sales Area Sales Organization]   = S.[SALES_ORGZTN],
        T.[Sales Area Sales Organization Name] = S.[SALES_ORGZTN_NAME],
        T.[Sales Area Distribution Channel] = S.[DISTRBN_CHNL],
        T.[Distribution Channel Name]       = S.[DISTRBN_CHNL_NAME],
        T.[Sales Area Division]             = S.[DIVISION],
        T.[Sales Area Division Name]        = S.[DIVISION_NAME],
        T.[Sales Area Created Date]         = S.[CREATED_DATE],
        T.[Sales Area Created User ID]      = S.[CREATED_USR_ID],
        T.[Sales Area Updated Date]         = S.[UPDTD_DATE],
        T.[Sales Area Updated User ID]      = S.[UPDTD_USR_ID],
        T.[ETL_SRC_SYS_CD]                  = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]          = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]            = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                  = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                    = S.[ETL_UPDTD_TS],
        T.[BIZ_SEG]                         = S.[BIZ_SEG]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS S
        ON T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY];

    -- Step 2: Insert new records from source into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] (
        [SALES_AREA_KEY],
        [Sales Area Sales Organization],
        [Sales Area Sales Organization Name],
        [Sales Area Distribution Channel],
        [Distribution Channel Name],
        [Sales Area Division],
        [Sales Area Division Name],
        [Sales Area Created Date],
        [Sales Area Created User ID],
        [Sales Area Updated Date],
        [Sales Area Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [BIZ_SEG]
    )
    SELECT
        S.[SALES_AREA_KEY],
        S.[SALES_ORGZTN],
        S.[SALES_ORGZTN_NAME],
        S.[DISTRBN_CHNL],
        S.[DISTRBN_CHNL_NAME],
        S.[DIVISION],
        S.[DIVISION_NAME],
        S.[CREATED_DATE],
        S.[CREATED_USR_ID],
        S.[UPDTD_DATE],
        S.[UPDTD_USR_ID],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[BIZ_SEG]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS T
        ON T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY]
    WHERE T.[SALES_AREA_KEY] IS NULL;
END
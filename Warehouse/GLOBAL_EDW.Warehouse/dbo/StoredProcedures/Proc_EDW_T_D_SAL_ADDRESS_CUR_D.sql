CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_SAL_ADDRESS_CUR_D]
AS
BEGIN

    UPDATE T
    SET
        T.[Address Number]                   = S.[ADDR_NUM],
        T.[Valid From Date]                  = S.[VALID_FROM_DATE],
        T.[International Address Version ID] = S.[INTERNATIONAL_ADDR_VRSN_ID],
        T.[Customer Name]                    = S.[NAME_1],
        T.[Cust Street]                      = S.[STR],
        T.[Cust City]                        = S.[CITY],
        T.[Order for Cust Region]            = S.[REGN],
        T.[Cust City Postal Code]            = S.[CITY_POSTAL_CD],
        T.[Cust Country Key]                 = S.[CNTRY_KEY],
        T.[Cust Language Key]                = S.[LANG_KEY],
        T.[Cust Telephone Number]            = S.[FIRST_TEL_NO],
        T.[ETL_SRC_SYS_CD]                   = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]           = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]             = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                 = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                   = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                     = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_SAL_ADDRESS_CUR_D] AS S
        ON T.[ADDR_KEY] = S.[ADDR_KEY];

    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D]
    (
        [ADDR_KEY],
        [Address Number],
        [Valid From Date],
        [International Address Version ID],
        [Customer Name],
        [Cust Street],
        [Cust City],
        [Order for Cust Region],
        [Cust City Postal Code],
        [Cust Country Key],
        [Cust Language Key],
        [Cust Telephone Number],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[ADDR_KEY],
        S.[ADDR_NUM],
        S.[VALID_FROM_DATE],
        S.[INTERNATIONAL_ADDR_VRSN_ID],
        S.[NAME_1],
        S.[STR],
        S.[CITY],
        S.[REGN],
        S.[CITY_POSTAL_CD],
        S.[CNTRY_KEY],
        S.[LANG_KEY],
        S.[FIRST_TEL_NO],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_SAL_ADDRESS_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] AS T
        ON T.[ADDR_KEY] = S.[ADDR_KEY]
    WHERE T.[ADDR_KEY] IS NULL;

END
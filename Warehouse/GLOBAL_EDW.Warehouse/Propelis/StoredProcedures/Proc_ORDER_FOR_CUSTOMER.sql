CREATE   PROCEDURE [Propelis].[Proc_ORDER_FOR_CUSTOMER]
AS
BEGIN
    -- Step 1: Update existing records in target
    UPDATE T
    SET 
        [Order for Cust Address Number]              = S.[ADDR_NUM],
        [Order for Cust Valid From Date]             = S.[VALID_FROM_DATE],
        [Order for Cust International Address Version ID] = S.[INTERNATIONAL_ADDR_VRSN_ID],
        [Order for Customer Name]                    = S.[NAME_1],
        [Order for Cust Street]                      = S.[STR],
        [Order for Cust City]                        = S.[CITY],
        [Order for Cust Region]                      = S.[REGN],
        [Order for Cust City Postal Code]            = S.[CITY_POSTAL_CD],
        [Order for Cust Country Key]                 = S.[CNTRY_KEY],
        [Order for Cust Language Key]                = S.[LANG_KEY],
        [Order for Cust Telephone Number]            = S.[FIRST_TEL_NO],
        [ETL_SRC_SYS_CD]                             = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]                     = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]                       = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]                           = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]                             = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]                               = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[ORDER_FOR_CUSTOMER] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] AS S
        ON T.[ADDR_KEY] = S.[ADDR_KEY];

    -- Step 2: Insert new records that do not exist in target
    INSERT INTO [GLOBAL_EDW].[Propelis].[ORDER_FOR_CUSTOMER]
    (
        [ADDR_KEY],
        [Order for Cust Address Number],
        [Order for Cust Valid From Date],
        [Order for Cust International Address Version ID],
        [Order for Customer Name],
        [Order for Cust Street],
        [Order for Cust City],
        [Order for Cust Region],
        [Order for Cust City Postal Code],
        [Order for Cust Country Key],
        [Order for Cust Language Key],
        [Order for Cust Telephone Number],
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ORDER_FOR_CUSTOMER] AS T
        ON T.[ADDR_KEY] = S.[ADDR_KEY]
    WHERE T.[ADDR_KEY] IS NULL;

END
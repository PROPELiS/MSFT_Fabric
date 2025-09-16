CREATE   PROCEDURE [Propelis].[Proc_COMPANY_INFO]
AS
BEGIN
    -----------------------------------------------------------------
    -- Step 1: Update existing records in target strictly by given columns
    -----------------------------------------------------------------
    UPDATE T
    SET
        T.[Company Code]                  = S.[CMPNY_CD],
        T.[Legacy Company Code]           = S.[LGCY_CMPNY_CD],
        T.[Company Name]                  = S.[CMPNY_NAME],
        T.[Line Of Business]              = S.[LINE_OF_BIZ_CD],
        T.[Line Of Business Description]  = S.[LINE_OF_BIZ_CD_DESC],
        T.[Currency Code]                 = S.[CRNCY_CD],
        T.[Language Type Code]            = S.[LANG_TYP_CD],
        T.[Chart Of Accounts ID]          = S.[CHART_OF_ACCTS_ID],
        T.[Entity Address Line 1]         = S.[ENTTY_ADDR_LINE_1],
        T.[Entity Address Line 2]         = S.[ENTTY_ADDR_LINE_2],
        T.[Entity City Name]              = S.[ENTTY_CITY_NAME],
        T.[Entity Zip Code]               = S.[ENTTY_ZIP_CD],
        T.[Entity Country Code]           = S.[ENTTY_CNTRY_CD],
        T.[Entity Telephone1]             = S.[ENTTY_TELEPHONE1],
        T.[Entity Telephone 2]            = S.[ENTTY_TELEPHONE2],
        T.[Fax Number]                    = S.[FAX_NUM],
        T.[Tax Jurisdiction]              = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD]                = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]        = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]          = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]              = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                  = S.[ETL_UPDTD_TS],
        T.[Fiscal Year Variant]           = S.[FISCAL_YR_VARIANT],
        T.[Credit Control Area]           = S.[CREDIT_CTRL_AREA],
        T.[Input Tax Code]                = S.[INPUT_TAX_CD],
        T.[Output Tax Code]               = S.[OTPUT_TAX_CD],
        T.[Currency Description]          = S.[CRNCY_DESC],
        T.[Chart Of Accounts Description] = S.[CHART_OF_ACCTS_DESC],
        T.[Country Description]           = S.[CNTRY_DESC],
        T.[Fiscal Year Variant Description] = S.[FISCAL_YR_VARIANT_DESC],
        T.[Credit Control Area Description] = S.[CREDIT_CTRL_AREA_DESC]
    FROM [GLOBAL_EDW].[Propelis].[COMPANY_INFO] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];

    -----------------------------------------------------------------
    -- Step 2: Insert new records strictly with the same columns
    -----------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[COMPANY_INFO] (
        [CMPNY_KEY],
        [Company Code],
        [Legacy Company Code],
        [Company Name],
        [Line Of Business],
        [Line Of Business Description],
        [Currency Code],
        [Language Type Code],
        [Chart Of Accounts ID],
        [Entity Address Line 1],
        [Entity Address Line 2],
        [Entity City Name],
        [Entity Zip Code],
        [Entity Country Code],
        [Entity Telephone1],
        [Entity Telephone 2],
        [Fax Number],
        [Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Fiscal Year Variant],
        [Credit Control Area],
        [Input Tax Code],
        [Output Tax Code],
        [Currency Description],
        [Chart Of Accounts Description],
        [Country Description],
        [Fiscal Year Variant Description],
        [Credit Control Area Description]
    )
    SELECT
        S.[CMPNY_KEY],
        S.[CMPNY_CD],
        S.[LGCY_CMPNY_CD],
        S.[CMPNY_NAME],
        S.[LINE_OF_BIZ_CD],
        S.[LINE_OF_BIZ_CD_DESC],
        S.[CRNCY_CD],
        S.[LANG_TYP_CD],
        S.[CHART_OF_ACCTS_ID],
        S.[ENTTY_ADDR_LINE_1],
        S.[ENTTY_ADDR_LINE_2],
        S.[ENTTY_CITY_NAME],
        S.[ENTTY_ZIP_CD],
        S.[ENTTY_CNTRY_CD],
        S.[ENTTY_TELEPHONE1],
        S.[ENTTY_TELEPHONE2],
        S.[FAX_NUM],
        S.[TAX_JRSDCTN],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[FISCAL_YR_VARIANT],
        S.[CREDIT_CTRL_AREA],
        S.[INPUT_TAX_CD],
        S.[OTPUT_TAX_CD],
        S.[CRNCY_DESC],
        S.[CHART_OF_ACCTS_DESC],
        S.[CNTRY_DESC],
        S.[FISCAL_YR_VARIANT_DESC],
        S.[CREDIT_CTRL_AREA_DESC]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_INFO] AS T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;
END
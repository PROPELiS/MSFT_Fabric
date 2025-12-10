CREATE   PROCEDURE [Propelis].[Proc_INTER_COMPANY] 
AS
BEGIN

    ----------------------------------------------------------
    -- UPDATE EXISTING RECORDS
    ----------------------------------------------------------
    UPDATE T
    SET  
        T.[CMPNY_KEY] = S.[CMPNY_KEY],
        T.[Inter Company Code] = S.[CMPNY_CD],
        T.[Inter Company Legacy Code] = S.[LGCY_CMPNY_CD],
        T.[Inter Company Name] = S.[CMPNY_NAME],
        T.[Inter Company Line Of Business Code] = S.[LINE_OF_BIZ_CD],
        T.[Inter Company Line Of Business Description] = S.[LINE_OF_BIZ_CD_DESC],
        T.[Inter Company Currency Code] = S.[CRNCY_CD],
        T.[Inter Company Language Type Code] = S.[LANG_TYP_CD],
        T.[Inter Company Chart Of Accounts ID] = S.[CHART_OF_ACCTS_ID],
        T.[Inter Company Address Line 1] = S.[ENTTY_ADDR_LINE_1],
        T.[Inter Company Address Line 2] = S.[ENTTY_ADDR_LINE_2],
        T.[Inter Company City Name] = S.[ENTTY_CITY_NAME],
        T.[Inter Company Zip Code] = S.[ENTTY_ZIP_CD],
        T.[Inter Company Country Code] = S.[ENTTY_CNTRY_CD],
        T.[Company Telephone 1] = S.[ENTTY_TELEPHONE1],
        T.[Company Telephone 2] = S.[ENTTY_TELEPHONE2],
        T.[Inter Company Fax Number] = S.[FAX_NUM],
        T.[Inter Company Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Inter Company Fiscal Year Variant] = S.[FISCAL_YR_VARIANT],
        T.[Inter Company Credit Control Area] = S.[CREDIT_CTRL_AREA],
        T.[Inter Company Input Tax Code] = S.[INPUT_TAX_CD],
        T.[Inter Company Output Tax Code] = S.[OTPUT_TAX_CD],
        T.[Inter Company Currency Description] = S.[CRNCY_DESC],
        T.[Inter Company Chart Of Accounts Description] = S.[CHART_OF_ACCTS_DESC],
        T.[Inter Company Country Description] = S.[CNTRY_DESC],
        T.[Inter Company Fiscal Year Variant Description] = S.[FISCAL_YR_VARIANT_DESC],
        T.[Inter Company Credit Control Area Description] = S.[CREDIT_CTRL_AREA_DESC]
    FROM [GLOBAL_EDW].[Propelis].[INTER_COMPANY] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];


    ----------------------------------------------------------
    -- INSERT NEW RECORDS
    ----------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMPANY]
    (
        [CMPNY_KEY],
        [Inter Company Code],
        [Inter Company Legacy Code],
        [Inter Company Name],
        [Inter Company Line Of Business Code],
        [Inter Company Line Of Business Description],
        [Inter Company Currency Code],
        [Inter Company Language Type Code],
        [Inter Company Chart Of Accounts ID],
        [Inter Company Address Line 1],
        [Inter Company Address Line 2],
        [Inter Company City Name],
        [Inter Company Zip Code],
        [Inter Company Country Code],
        [Company Telephone 1],
        [Company Telephone 2],
        [Inter Company Fax Number],
        [Inter Company Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Inter Company Fiscal Year Variant],
        [Inter Company Credit Control Area],
        [Inter Company Input Tax Code],
        [Inter Company Output Tax Code],
        [Inter Company Currency Description],
        [Inter Company Chart Of Accounts Description],
        [Inter Company Country Description],
        [Inter Company Fiscal Year Variant Description],
        [Inter Company Credit Control Area Description]
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[INTER_COMPANY] AS T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;

END;
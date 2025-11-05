CREATE   PROCEDURE [Propelis].[Proc_Trading_Partner_Company]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Trading Partner Company Code] = S.[CMPNY_CD],
        T.[Legacy Trading Partner Company Code] = S.[LGCY_CMPNY_CD],
        T.[Trading Partner Company Name] = S.[CMPNY_NAME],
        T.[Trading Partner Company Line Of Business Code] = S.[LINE_OF_BIZ_CD],
        T.[Trading Partner Company Line Of Business Description] = S.[LINE_OF_BIZ_CD_DESC],
        T.[Trading Partner Company Currency Code] = S.[CRNCY_CD],
        T.[Trading Partner Company Language Type Code] = S.[LANG_TYP_CD],
        T.[Trading Partner Company Chart Of Accounts ID] = S.[CHART_OF_ACCTS_ID],
        T.[Trading Partner Company Address Line 1] = S.[ENTTY_ADDR_LINE_1],
        T.[Trading Partner Company Address Line 2] = S.[ENTTY_ADDR_LINE_2],
        T.[Trading Partner Company City Name] = S.[ENTTY_CITY_NAME],
        T.[Trading Partner Company Zip Code] = S.[ENTTY_ZIP_CD],
        T.[Trading Partner Company Country Code] = S.[ENTTY_CNTRY_CD],
        T.[Trading Partner Company Telephone 1] = S.[ENTTY_TELEPHONE1],
        T.[Trading Partner Company Telephone 2] = S.[ENTTY_TELEPHONE2],
        T.[Trading Partner Company Fax Number] = S.[FAX_NUM],
        T.[Trading Partner Company Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Trading Partner Company Fiscal Year Variant] = S.[FISCAL_YR_VARIANT],
        T.[Trading Partner Company Credit Control Area] = S.[CREDIT_CTRL_AREA],
        T.[Trading Partner Company Input Tax Code] = S.[INPUT_TAX_CD],
        T.[Trading Partner Company Output Tax Code] = S.[OTPUT_TAX_CD],
        T.[Trading Partner Company Currency Description] = S.[CRNCY_DESC],
        T.[Trading Partner Company Chart Of Accounts Description] = S.[CHART_OF_ACCTS_DESC],
        T.[Trading Partner Company Country Description] = S.[CNTRY_DESC],
        T.[Trading Partner Company Fiscal Year Variant Description] = S.[FISCAL_YR_VARIANT_DESC],
        T.[Trading Partner Company Credit Control Area Description] = S.[CREDIT_CTRL_AREA_DESC]
    FROM [GLOBAL_EDW].[Propelis].[Trading_Partner_Company] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[Trading_Partner_Company] (
        [CMPNY_KEY],
        [Trading Partner Company Code],
        [Legacy Trading Partner Company Code],
        [Trading Partner Company Name],
        [Trading Partner Company Line Of Business Code],
        [Trading Partner Company Line Of Business Description],
        [Trading Partner Company Currency Code],
        [Trading Partner Company Language Type Code],
        [Trading Partner Company Chart Of Accounts ID],
        [Trading Partner Company Address Line 1],
        [Trading Partner Company Address Line 2],
        [Trading Partner Company City Name],
        [Trading Partner Company Zip Code],
        [Trading Partner Company Country Code],
        [Trading Partner Company Telephone 1],
        [Trading Partner Company Telephone 2],
        [Trading Partner Company Fax Number],
        [Trading Partner Company Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Trading Partner Company Fiscal Year Variant],
        [Trading Partner Company Credit Control Area],
        [Trading Partner Company Input Tax Code],
        [Trading Partner Company Output Tax Code],
        [Trading Partner Company Currency Description],
        [Trading Partner Company Chart Of Accounts Description],
        [Trading Partner Company Country Description],
        [Trading Partner Company Fiscal Year Variant Description],
        [Trading Partner Company Credit Control Area Description]
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
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Trading_Partner_Company] T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;
END
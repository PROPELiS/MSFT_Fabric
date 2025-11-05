CREATE   PROCEDURE [Propelis].[Proc_Entity_Company]
AS
BEGIN
    -- ============================================
    -- Step 1: Update existing records
    -- ============================================
    UPDATE T
    SET
        T.[Entity Company Code] = S.[Company Code],
        T.[Entity Legacy Company Code] = S.[Legacy Company Code],
        T.[Entity Company Name] = S.[Company Name],
        T.[Entity Company Line Of Business Code] = S.[Company Line Of Business],
        T.[Entity Company Line Of Business Code Description] = S.[Company Line Of Business Description],
        T.[Entity Company Currency Code] = S.[Company Currency Code],
        T.[Entity Company Language Type Code] = S.[Company Language Type Code],
        T.[Entity Company Chart Of Accounts ID] = S.[Company Chart Of Accounts ID],
        T.[Entity Company Entity Address Line 1] = S.[Company Address Line 1],
        T.[Entity Company Entity Address Line 2] = S.[Company Address Line 2],
        T.[Entity Company Entity City Name] = S.[Company City Name],
        T.[Entity Company Entity Zip Code] = S.[Company Zip Code],
        T.[Entity Company Entity Country Code] = S.[Company Country Code],
        T.[Entity Company Entity Telephone 1] = S.[Company Telephone 1],
        T.[Entity Company Entity Telephone 2] = S.[Company Telephone 2],
        T.[Entity Company Fax Number] = S.[Company Fax Number],
        T.[Entity Company Tax Jurisdiction] = S.[Company Tax Jurisdiction],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Entity Company Fiscal Year Variant] = S.[Company Fiscal Year Variant],
        T.[Entity Company Credit Control Area] = S.[Company Credit Control Area],
        T.[Entity Company Input Tax Code] = S.[Company Input Tax Code],
        T.[Entity Company Output Tax Code] = S.[Company Output Tax Code],
        T.[Entity Company Currency Description] = S.[Company Currency Description],
        T.[Entity Company Chart Of Accounts Description] = S.[Company Chart Of Accounts Description],
        T.[Entity Company Country Description] = S.[Company Country Description],
        T.[Entity Company Fiscal Year Variant Description] = S.[Company Fiscal Year Variant Description],
        T.[Entity Company Credit Control Area Description] = S.[Company Credit Control Area Description]
    FROM [GLOBAL_EDW].[Propelis].[Entity_Company] AS T
    INNER JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];

    -- ============================================
    -- Step 2: Insert new records
    -- ============================================
    INSERT INTO [GLOBAL_EDW].[Propelis].[Entity_Company]
    (
        [CMPNY_KEY],
        [Entity Company Code],
        [Entity Legacy Company Code],
        [Entity Company Name],
        [Entity Company Line Of Business Code],
        [Entity Company Line Of Business Code Description],
        [Entity Company Currency Code],
        [Entity Company Language Type Code],
        [Entity Company Chart Of Accounts ID],
        [Entity Company Entity Address Line 1],
        [Entity Company Entity Address Line 2],
        [Entity Company Entity City Name],
        [Entity Company Entity Zip Code],
        [Entity Company Entity Country Code],
        [Entity Company Entity Telephone 1],
        [Entity Company Entity Telephone 2],
        [Entity Company Fax Number],
        [Entity Company Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Entity Company Fiscal Year Variant],
        [Entity Company Credit Control Area],
        [Entity Company Input Tax Code],
        [Entity Company Output Tax Code],
        [Entity Company Currency Description],
        [Entity Company Chart Of Accounts Description],
        [Entity Company Country Description],
        [Entity Company Fiscal Year Variant Description],
        [Entity Company Credit Control Area Description]
    )
    SELECT
        S.[CMPNY_KEY],
        S.[Company Code],
        S.[Legacy Company Code],
        S.[Company Name],
        S.[Company Line Of Business],
        S.[Company Line Of Business Description],
        S.[Company Currency Code],
        S.[Company Language Type Code],
        S.[Company Chart Of Accounts ID],
        S.[Company Address Line 1],
        S.[Company Address Line 2],
        S.[Company City Name],
        S.[Company Zip Code],
        S.[Company Country Code],
        S.[Company Telephone 1],
        S.[Company Telephone 2],
        S.[Company Fax Number],
        S.[Company Tax Jurisdiction],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[Company Fiscal Year Variant],
        S.[Company Credit Control Area],
        S.[Company Input Tax Code],
        S.[Company Output Tax Code],
        S.[Company Currency Description],
        S.[Company Chart Of Accounts Description],
        S.[Company Country Description],
        S.[Company Fiscal Year Variant Description],
        S.[Company Credit Control Area Description]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Entity_Company] AS T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;

END;
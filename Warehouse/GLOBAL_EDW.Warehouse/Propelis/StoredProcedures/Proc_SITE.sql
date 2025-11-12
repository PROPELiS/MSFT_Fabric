CREATE     PROCEDURE [Propelis].[Proc_SITE]
AS
BEGIN

    ----------------------------------------------------------------------
    -- STEP 1: Update existing records
    ----------------------------------------------------------------------

    UPDATE T
    SET
        T.[PLNT_KEY] = S.[PLNT_KEY],
        T.[Site ID] = S.[PLNT_ID],
        T.[Site Legacy Plant ID] = S.[LGCY_PLNT_ID],
        T.[Site Name] = S.[PLNT_NAME],
        T.[Site Company Code] = S.[CMPNY_CD],
        T.[Site Sales Organization] = S.[SALES_ORGZTN],
        T.[Site Purchasing Organization] = S.[PURNG_ORGZTN],
        T.[Site Valuation Area] = S.[VALTN_AREA],
        T.[Site Customer Number Of Plant] = S.[CUST_NUM_OF_PLNT],
        T.[Site Vendor Number Of Plant] = S.[VNDR_NUM_OF_PLNT],
        T.[Site Factory Calendar Key] = S.[FACTORY_CLNDR_KEY],
        T.[Site Line 1 Description] = S.[PLNT_LINE_1_DESC],
        T.[Site Line 2 Description] = S.[PLNT_LINE_2_DESC],
        T.[Site City Name] = S.[PLNT_CITY_NAME],
        T.[Site Zip Code] = S.[PLNT_ZIP_CD],
        T.[Site Country Code] = S.[PLNT_CNTRY_CD],
        T.[Site Country Name] = S.[PLNT_CNTRY_NAME],
        T.[Site Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Site Is P001] = S.[IS_P001],
        T.[Site Is P002] = S.[IS_P002],
        T.[Site Company Description] = S.[CMPNY_DESC],
        T.[Site Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[Site Factory Calendar Description] = S.[FACTORY_CLNDR_DESC],
        T.[Site Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[Site Tax Indicator] = S.[PLNT_TAX_IND],
        T.[Site Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[Site Shipping Receiving Point] = S.[SHIPG_RECVNG_PNT],
        T.[Site Shipping Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[SITE] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    ----------------------------------------------------------------------
    -- STEP 2: Insert new records
    ----------------------------------------------------------------------

    INSERT INTO [GLOBAL_EDW].[Propelis].[SITE]
    (
        [PLNT_KEY],
        [Site ID],
        [Site Legacy Plant ID],
        [Site Name],
        [Site Company Code],
        [Site Sales Organization],
        [Site Purchasing Organization],
        [Site Valuation Area],
        [Site Customer Number Of Plant],
        [Site Vendor Number Of Plant],
        [Site Factory Calendar Key],
        [Site Line 1 Description],
        [Site Line 2 Description],
        [Site City Name],
        [Site Zip Code],
        [Site Country Code],
        [Site Country Name],
        [Site Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Site Is P001],
        [Site Is P002],
        [Site Company Description],
        [Site Sales Organization Description],
        [Site Factory Calendar Description],
        [Site Purchasing Organization Description],
        [Site Tax Indicator],
        [Site Tax Indicator Description],
        [Site Shipping Receiving Point],
        [Site Shipping Receiving Point Description]
    )
    SELECT
        S.[PLNT_KEY],
        S.[PLNT_ID],
        S.[LGCY_PLNT_ID],
        S.[PLNT_NAME],
        S.[CMPNY_CD],
        S.[SALES_ORGZTN],
        S.[PURNG_ORGZTN],
        S.[VALTN_AREA],
        S.[CUST_NUM_OF_PLNT],
        S.[VNDR_NUM_OF_PLNT],
        S.[FACTORY_CLNDR_KEY],
        S.[PLNT_LINE_1_DESC],
        S.[PLNT_LINE_2_DESC],
        S.[PLNT_CITY_NAME],
        S.[PLNT_ZIP_CD],
        S.[PLNT_CNTRY_CD],
        S.[PLNT_CNTRY_NAME],
        S.[TAX_JRSDCTN],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[IS_P001],
        S.[IS_P002],
        S.[CMPNY_DESC],
        S.[SALES_ORG_DESC],
        S.[FACTORY_CLNDR_DESC],
        S.[PURNG_ORGZTN_DESC],
        S.[PLNT_TAX_IND],
        S.[PLNT_TAX_IND_DESC],
        S.[SHIPG_RECVNG_PNT],
        S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SITE] T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;

END;
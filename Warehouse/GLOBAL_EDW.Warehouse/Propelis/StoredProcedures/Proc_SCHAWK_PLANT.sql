CREATE   PROCEDURE [Propelis].[Proc_SCHAWK_PLANT]
AS
BEGIN

    ----------------------------------------------------------------------
    -- STEP 1: Update existing records
    ----------------------------------------------------------------------

    UPDATE T
    SET
        T.[PLNT_KEY] = S.[PLNT_KEY],
        T.[Schawk Plant ID] = S.[PLNT_ID],
        T.[Schawk Legacy Plant ID] = S.[LGCY_PLNT_ID],
        T.[Schawk Plant Name] = S.[PLNT_NAME],
        T.[Schawk Plant Company Code] = S.[CMPNY_CD],
        T.[Schawk Plant Sales Organization] = S.[SALES_ORGZTN],
        T.[Schawk Plant Purchasing Organization] = S.[PURNG_ORGZTN],
        T.[Schawk Plant Valuation Area] = S.[VALTN_AREA],
        T.[Schawk Plant Customer Number Of Plant] = S.[CUST_NUM_OF_PLNT],
        T.[Schawk Plant Vendor Number Of Plant] = S.[VNDR_NUM_OF_PLNT],
        T.[Plant Factory Calendar Key] = S.[FACTORY_CLNDR_KEY],
        T.[Schawk Plant Line 1 Description] = S.[PLNT_LINE_1_DESC],
        T.[Schawk Plant Line 2 Description] = S.[PLNT_LINE_2_DESC],
        T.[Schawk Plant City Name] = S.[PLNT_CITY_NAME],
        T.[Schawk Plant Zip Code] = S.[PLNT_ZIP_CD],
        T.[Schawk Plant Country Code] = S.[PLNT_CNTRY_CD],
        T.[Schawk Plant Country Name] = S.[PLNT_CNTRY_NAME],
        T.[Schawk Plant Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Schawk Plant Is P001] = S.[IS_P001],
        T.[Schawk Plant Is P002] = S.[IS_P002],
        T.[Schawk Plant Company Description] = S.[CMPNY_DESC],
        T.[Schawk Plant Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[Schawk Plant Factory Calendar Description] = S.[FACTORY_CLNDR_DESC],
        T.[Schawk Plant Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[Schawk Plant Tax Indicator] = S.[PLNT_TAX_IND],
        T.[Schawk Plant Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[Schawk Plant Shipping Receiving Point] = S.[SHIPG_RECVNG_PNT],
        T.[Schawk Plant Shipping Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[Schawk_PLANT] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    ----------------------------------------------------------------------
    -- STEP 2: Insert new records
    ----------------------------------------------------------------------

    INSERT INTO [GLOBAL_EDW].[Propelis].[Schawk_PLANT]
    (
        [PLNT_KEY],
        [Schawk Plant ID],
        [Schawk Legacy Plant ID],
        [Schawk Plant Name],
        [Schawk Plant Company Code],
        [Schawk Plant Sales Organization],
        [Schawk Plant Purchasing Organization],
        [Schawk Plant Valuation Area],
        [Schawk Plant Customer Number Of Plant],
        [Schawk Plant Vendor Number Of Plant],
        [Plant Factory Calendar Key],
        [Schawk Plant Line 1 Description],
        [Schawk Plant Line 2 Description],
        [Schawk Plant City Name],
        [Schawk Plant Zip Code],
        [Schawk Plant Country Code],
        [Schawk Plant Country Name],
        [Schawk Plant Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Schawk Plant Is P001],
        [Schawk Plant Is P002],
        [Schawk Plant Company Description],
        [Schawk Plant Sales Organization Description],
        [Schawk Plant Factory Calendar Description],
        [Schawk Plant Purchasing Organization Description],
        [Schawk Plant Tax Indicator],
        [Schawk Plant Tax Indicator Description],
        [Schawk Plant Shipping Receiving Point],
        [Schawk Plant Shipping Receiving Point Description]
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
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[Schawk_PLANT] T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;

END;
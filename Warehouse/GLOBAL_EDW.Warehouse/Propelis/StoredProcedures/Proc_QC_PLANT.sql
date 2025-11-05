CREATE   PROCEDURE [Propelis].[Proc_QC_PLANT]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[PLNT_KEY] = S.[PLNT_KEY],
        T.[QC Plant ID] = S.[PLNT_ID],
        T.[QC Legacy Plant ID] = S.[LGCY_PLNT_ID],
        T.[QC Plant Name] = S.[PLNT_NAME],
        T.[QC Company Code] = S.[CMPNY_CD],
        T.[QC Sales Organization] = S.[SALES_ORGZTN],
        T.[QC Purchasing Organization] = S.[PURNG_ORGZTN],
        T.[QC Valuation Area] = S.[VALTN_AREA],
        T.[QC Customer Number Of Plant] = S.[CUST_NUM_OF_PLNT],
        T.[QC Vendor Number Of Plant] = S.[VNDR_NUM_OF_PLNT],
        T.[Plant Factory Calendar Key] = S.[FACTORY_CLNDR_KEY],
        T.[QC Plant Line 1 Description] = S.[PLNT_LINE_1_DESC],
        T.[QC Plant Line 2 Description] = S.[PLNT_LINE_2_DESC],
        T.[QC Plant City Name] = S.[PLNT_CITY_NAME],
        T.[QC Plant Zip Code] = S.[PLNT_ZIP_CD],
        T.[QC Plant Country Code] = S.[PLNT_CNTRY_CD],
        T.[QC Plant Country Name] = S.[PLNT_CNTRY_NAME],
        T.[QC Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[QC Is P001] = S.[IS_P001],
        T.[QC Is P002] = S.[IS_P002],
        T.[QC Company Description] = S.[CMPNY_DESC],
        T.[QC Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[QC Factory Calendar Description] = S.[FACTORY_CLNDR_DESC],
        T.[QC Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[QC Plant Tax Indicator] = S.[PLNT_TAX_IND],
        T.[QC Plant Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[QC Shipping Receiving Point] = S.[SHIPG_RECVNG_PNT],
        T.[QC Shipping Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[QC_PLANT] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[QC_PLANT] (
        [PLNT_KEY],
        [QC Plant ID],
        [QC Legacy Plant ID],
        [QC Plant Name],
        [QC Company Code],
        [QC Sales Organization],
        [QC Purchasing Organization],
        [QC Valuation Area],
        [QC Customer Number Of Plant],
        [QC Vendor Number Of Plant],
        [Plant Factory Calendar Key],
        [QC Plant Line 1 Description],
        [QC Plant Line 2 Description],
        [QC Plant City Name],
        [QC Plant Zip Code],
        [QC Plant Country Code],
        [QC Plant Country Name],
        [QC Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [QC Is P001],
        [QC Is P002],
        [QC Company Description],
        [QC Sales Organization Description],
        [QC Factory Calendar Description],
        [QC Purchasing Organization Description],
        [QC Plant Tax Indicator],
        [QC Plant Tax Indicator Description],
        [QC Shipping Receiving Point],
        [QC Shipping Receiving Point Description]
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[QC_PLANT] T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;
END
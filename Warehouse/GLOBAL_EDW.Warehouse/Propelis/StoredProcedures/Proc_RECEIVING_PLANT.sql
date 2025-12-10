CREATE     PROCEDURE [Propelis].[Proc_RECEIVING_PLANT]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records in target table
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Receiving Plant ID] = S.[PLNT_ID],
        T.[Receiving Legacy Plant ID] = S.[LGCY_PLNT_ID],
        T.[Receiving Plant Name] = S.[PLNT_NAME],
        T.[Receiving Plant Company Code] = S.[CMPNY_CD],
        T.[Receiving Plant Sales Organization] = S.[SALES_ORGZTN],
        T.[Receiving Plant Purchasing Organization] = S.[PURNG_ORGZTN],
        T.[Receiving Plant Valuation Area] = S.[VALTN_AREA],
        T.[Receiving Customer Number Of Plant] = S.[CUST_NUM_OF_PLNT],
        T.[Receiving Vendor Number Of Plant] = S.[VNDR_NUM_OF_PLNT],
        T.[FACTORY_CLNDR_KEY] = S.[FACTORY_CLNDR_KEY],
        T.[Receiving Plant Line 1 Description] = S.[PLNT_LINE_1_DESC],
        T.[Receiving Plant Line 2 Description] = S.[PLNT_LINE_2_DESC],
        T.[Receiving Plant City Name] = S.[PLNT_CITY_NAME],
        T.[Receiving Plant Zip Code] = S.[PLNT_ZIP_CD],
        T.[Receiving Plant Country Code] = S.[PLNT_CNTRY_CD],
        T.[Receiving Plant Country Name] = S.[PLNT_CNTRY_NAME],
        T.[Receiving Plant Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Receiving Plant Is P001] = S.[IS_P001],
        T.[Receiving Plant Is P002] = S.[IS_P002],
        T.[Receiving Plant Company Description] = S.[CMPNY_DESC],
        T.[Receiving Plant Sales Organiztion Description] = S.[SALES_ORG_DESC],
        T.[Receiving Plant Factory Calendar Description] = S.[FACTORY_CLNDR_DESC],
        T.[Receiving Plant Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[Receiving Plant Tax Indicator] = S.[PLNT_TAX_IND],
        T.[Receiving Plant Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[Receiving Plant Shipping Recvng Point] = S.[SHIPG_RECVNG_PNT],
        T.[Receiving Plant Shipping Recvng Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[RECEIVING_PLANT] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    ----------------------------------------------------------------------
    -- Insert new records into target table
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[RECEIVING_PLANT] (
        [PLNT_KEY],
        [Receiving Plant ID],
        [Receiving Legacy Plant ID],
        [Receiving Plant Name],
        [Receiving Plant Company Code],
        [Receiving Plant Sales Organization],
        [Receiving Plant Purchasing Organization],
        [Receiving Plant Valuation Area],
        [Receiving Customer Number Of Plant],
        [Receiving Vendor Number Of Plant],
        [FACTORY_CLNDR_KEY],
        [Receiving Plant Line 1 Description],
        [Receiving Plant Line 2 Description],
        [Receiving Plant City Name],
        [Receiving Plant Zip Code],
        [Receiving Plant Country Code],
        [Receiving Plant Country Name],
        [Receiving Plant Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Receiving Plant Is P001],
        [Receiving Plant Is P002],
        [Receiving Plant Company Description],
        [Receiving Plant Sales Organiztion Description],
        [Receiving Plant Factory Calendar Description],
        [Receiving Plant Purchasing Organization Description],
        [Receiving Plant Tax Indicator],
        [Receiving Plant Tax Indicator Description],
        [Receiving Plant Shipping Recvng Point],
        [Receiving Plant Shipping Recvng Point Description]
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[RECEIVING_PLANT] T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;
END
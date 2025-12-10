CREATE   PROCEDURE [Propelis].[Proc_LOCATION_PLANT]
AS
BEGIN
   
    UPDATE T
    SET 
        T.[PLNT_KEY]                              = S.[PLNT_KEY],
        T.[Location Plant ID]                      = S.[PLNT_ID],
        T.[Location Legacy Plant ID]               = S.[LGCY_PLNT_ID],
        T.[Location Plant Name]                    = S.[PLNT_NAME],
        T.[Location Plant Company Code]            = S.[CMPNY_CD],
        T.[Location Plant Sales Organization]      = S.[SALES_ORGZTN],
        T.[Location Plant Purchasing Organization] = S.[PURNG_ORGZTN],
        T.[Location Plant Valuation Area]          = S.[VALTN_AREA],
        T.[Location Plant Customer Number]         = S.[CUST_NUM_OF_PLNT],
        T.[Location Plant Vendor Number]           = S.[VNDR_NUM_OF_PLNT],
        T.[Location Plant Factory Calendar Key]    = S.[FACTORY_CLNDR_KEY],
        T.[Location Plant Line 1 Description]      = S.[PLNT_LINE_1_DESC],
        T.[Location Plant Line 2 Description]      = S.[PLNT_LINE_2_DESC],
        T.[Location Plant City Name]               = S.[PLNT_CITY_NAME],
        T.[Location Plant Zip Code]                = S.[PLNT_ZIP_CD],
        T.[Location Plant Country Code]            = S.[PLNT_CNTRY_CD],
        T.[Location Plant Country Name]            = S.[PLNT_CNTRY_NAME],
        T.[Location Plant Tax Jurisdiction]        = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD]                         = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                 = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                   = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                       = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                         = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                           = S.[ETL_UPDTD_TS],
        T.[Location Plant Is P001]                 = S.[IS_P001],
        T.[Location Plant Is P002]                 = S.[IS_P002],
        T.[Location Plant Company Description]     = S.[CMPNY_DESC],
        T.[Location Plant Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[Location Plant Factory Calendar Description]   = S.[FACTORY_CLNDR_DESC],
        T.[Location Plant Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[Location Plant Tax Indicator]           = S.[PLNT_TAX_IND],
        T.[Location Plant Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[Location Plant Shipping/Receiving Point] = S.[SHIPG_RECVNG_PNT],
        T.[Location Plant Shipping/Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[LOCATION_PLANT] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] AS S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    --------------------------------------------------------
    -- INSERT new rows
    --------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[LOCATION_PLANT]
    (
        [PLNT_KEY],
        [Location Plant ID],
        [Location Legacy Plant ID],
        [Location Plant Name],
        [Location Plant Company Code],
        [Location Plant Sales Organization],
        [Location Plant Purchasing Organization],
        [Location Plant Valuation Area],
        [Location Plant Customer Number],
        [Location Plant Vendor Number],
        [Location Plant Factory Calendar Key],
        [Location Plant Line 1 Description],
        [Location Plant Line 2 Description],
        [Location Plant City Name],
        [Location Plant Zip Code],
        [Location Plant Country Code],
        [Location Plant Country Name],
        [Location Plant Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Location Plant Is P001],
        [Location Plant Is P002],
        [Location Plant Company Description],
        [Location Plant Sales Organization Description],
        [Location Plant Factory Calendar Description],
        [Location Plant Purchasing Organization Description],
        [Location Plant Tax Indicator],
        [Location Plant Tax Indicator Description],
        [Location Plant Shipping/Receiving Point],
        [Location Plant Shipping/Receiving Point Description]
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
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[LOCATION_PLANT] AS T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;

END;
CREATE   PROCEDURE [Propelis].[Proc_PRODUCING_PLANT]
AS
BEGIN
    
    --------------------------------------------------------
    -- UPDATE existing records
    --------------------------------------------------------
    UPDATE T
    SET 
        T.[PLNT_KEY]                                      = S.[PLNT_KEY],
        T.[Producing Plant ID]                             = S.[PLNT_ID],
        T.[Producing Legacy Plant ID]                      = S.[LGCY_PLNT_ID],
        T.[Producing Plant Name]                           = S.[PLNT_NAME],
        T.[Producing Plant Company Code]                   = S.[CMPNY_CD],
        T.[Producing Plant Sales Organization]             = S.[SALES_ORGZTN],
        T.[Producing Plant Purchasing Organization]        = S.[PURNG_ORGZTN],
        T.[Producing Plant Valuation Area]                 = S.[VALTN_AREA],
        T.[Producing Plant Customer Number]                = S.[CUST_NUM_OF_PLNT],
        T.[Producing Plant Vendor Number]                  = S.[VNDR_NUM_OF_PLNT],
        T.[Producing Plant Factory Calendar Key]           = S.[FACTORY_CLNDR_KEY],
        T.[Producing Plant Line 1 Description]             = S.[PLNT_LINE_1_DESC],
        T.[Producing Plant Line 2 Description]             = S.[PLNT_LINE_2_DESC],
        T.[Producing Plant City Name]                      = S.[PLNT_CITY_NAME],
        T.[Producing Plant Zip Code]                       = S.[PLNT_ZIP_CD],
        T.[Producing Plant Country Code]                   = S.[PLNT_CNTRY_CD],
        T.[Producing Plant Country Name]                   = S.[PLNT_CNTRY_NAME],
        T.[Producing Plant Tax Jurisdiction]               = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD]                                 = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                         = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                           = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                               = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                                 = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                                   = S.[ETL_UPDTD_TS],
        T.[Producing Plant Is P001]                        = S.[IS_P001],
        T.[Producing Plant Is P002]                        = S.[IS_P002],
        T.[Producing Plant Company Description]            = S.[CMPNY_DESC],
        T.[Producing Plant Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[Producing Plant Factory Calendar Description]   = S.[FACTORY_CLNDR_DESC],
        T.[Producing Plant Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[Producing Plant Tax Indicator]                  = S.[PLNT_TAX_IND],
        T.[Producing Plant Tax Indicator Description]       = S.[PLNT_TAX_IND_DESC],
        T.[Producing Plant Shipping/Receiving Point]        = S.[SHIPG_RECVNG_PNT],
        T.[Producing Plant Shipping/Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PRODUCING_PLANT] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] AS S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    --------------------------------------------------------
    -- INSERT new rows
    --------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PRODUCING_PLANT]
    (
        [PLNT_KEY],
        [Producing Plant ID],
        [Producing Legacy Plant ID],
        [Producing Plant Name],
        [Producing Plant Company Code],
        [Producing Plant Sales Organization],
        [Producing Plant Purchasing Organization],
        [Producing Plant Valuation Area],
        [Producing Plant Customer Number],
        [Producing Plant Vendor Number],
        [Producing Plant Factory Calendar Key],
        [Producing Plant Line 1 Description],
        [Producing Plant Line 2 Description],
        [Producing Plant City Name],
        [Producing Plant Zip Code],
        [Producing Plant Country Code],
        [Producing Plant Country Name],
        [Producing Plant Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Producing Plant Is P001],
        [Producing Plant Is P002],
        [Producing Plant Company Description],
        [Producing Plant Sales Organization Description],
        [Producing Plant Factory Calendar Description],
        [Producing Plant Purchasing Organization Description],
        [Producing Plant Tax Indicator],
        [Producing Plant Tax Indicator Description],
        [Producing Plant Shipping/Receiving Point],
        [Producing Plant Shipping/Receiving Point Description]
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PRODUCING_PLANT] AS T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;
END;
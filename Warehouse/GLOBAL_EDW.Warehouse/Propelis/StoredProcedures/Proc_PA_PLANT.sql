CREATE     PROCEDURE [Propelis].[Proc_PA_PLANT]
AS
BEGIN  

    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[PA Plant ID]                        = S.[PLNT_ID],
        T.[PA Legacy Plant ID]                 = S.[LGCY_PLNT_ID],
        T.[PA Plant Name]                      = S.[PLNT_NAME],
        T.[PA Plant Company Code]              = S.[CMPNY_CD],
        T.[PA Plant Sales Organization]        = S.[SALES_ORGZTN],
        T.[PA Plant Purchasing Organization]   = S.[PURNG_ORGZTN],
        T.[PA Plant Valuation Area]            = S.[VALTN_AREA],
        T.[PA Plant Customer Number Of Plant]  = S.[CUST_NUM_OF_PLNT],
        T.[PA Plant Vendor Number Of Plant]    = S.[VNDR_NUM_OF_PLNT],
        T.[PA Plant Factory Calendar Key]      = S.[FACTORY_CLNDR_KEY],
        T.[PA Plant Line 1 Description]        = S.[PLNT_LINE_1_DESC],
        T.[PA Plant Line 2 Description]        = S.[PLNT_LINE_2_DESC],
        T.[PA Plant City Name]                 = S.[PLNT_CITY_NAME],
        T.[PA Plant Zip Code]                  = S.[PLNT_ZIP_CD],
        T.[PA Plant Country Code]              = S.[PLNT_CNTRY_CD],
        T.[PA Plant Country Name]              = S.[PLNT_CNTRY_NAME],
        T.[PA Plant Tax Jurisdiction]          = S.[TAX_JRSDCTN],
        T.[ETL_Source_System_Code]             = S.[ETL_SRC_SYS_CD],
        T.[Current Record Indicator Of PA Plant] = S.[ETL_CURR_RCD_IND],
        T.[PA Plant Is P001]                   = S.[IS_P001],
        T.[PA Plant Is P002]                   = S.[IS_P002],
        T.[PA Plant Company Description]       = S.[CMPNY_DESC],
        T.[PA Plant Sales Organization Description] = S.[SALES_ORG_DESC],
        T.[PA Plant Factory Calendar Description]   = S.[FACTORY_CLNDR_DESC],
        T.[PA Plant Purchasing Organization Description] = S.[PURNG_ORGZTN_DESC],
        T.[PA Plant Tax Indicator]             = S.[PLNT_TAX_IND],
        T.[PA Plant Tax Indicator Description] = S.[PLNT_TAX_IND_DESC],
        T.[PA Plant Shipping Receiving Point]  = S.[SHIPG_RECVNG_PNT],
        T.[PA Plant Shipping Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC]

    FROM [GLOBAL_EDW].[Propelis].[PA_PLANT] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PA_PLANT] (
        [PLNT_KEY],
        [PA Plant ID],
        [PA Legacy Plant ID],
        [PA Plant Name],
        [PA Plant Company Code],
        [PA Plant Sales Organization],
        [PA Plant Purchasing Organization],
        [PA Plant Valuation Area],
        [PA Plant Customer Number Of Plant],
        [PA Plant Vendor Number Of Plant],
        [PA Plant Factory Calendar Key],
        [PA Plant Line 1 Description],
        [PA Plant Line 2 Description],
        [PA Plant City Name],
        [PA Plant Zip Code],
        [PA Plant Country Code],
        [PA Plant Country Name],
        [PA Plant Tax Jurisdiction],
        [ETL_Source_System_Code],
        [Current Record Indicator Of PA Plant],
        [PA Plant Is P001],
        [PA Plant Is P002],
        [PA Plant Company Description],
        [PA Plant Sales Organization Description],
        [PA Plant Factory Calendar Description],
        [PA Plant Purchasing Organization Description],
        [PA Plant Tax Indicator],
        [PA Plant Tax Indicator Description],
        [PA Plant Shipping Receiving Point],
        [PA Plant Shipping Receiving Point Description]
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
        S.[ETL_CURR_RCD_IND],
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PA_PLANT] T
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE T.[PLNT_KEY] IS NULL;

END;
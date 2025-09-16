CREATE   PROCEDURE Propelis.Proc_LOCATION_PLANT
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

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
       S.[SHIPG_RECVNG_PNT_DESC],

        HASHBYTES('SHA2_256', 
            CONCAT(
                 ISNULL(S.[PLNT_KEY], ''), '|',
                 ISNULL(S.[PLNT_ID], ''), '|',
                 ISNULL(S.[LGCY_PLNT_ID], ''), '|',
                 ISNULL(S.[PLNT_NAME], ''), '|',
                 ISNULL(S.[CMPNY_CD], ''), '|',
                 ISNULL(S.[SALES_ORGZTN], ''), '|',
                 ISNULL(S.[PURNG_ORGZTN], ''), '|',
                 ISNULL(S.[VALTN_AREA], ''), '|',
                 ISNULL(S.[CUST_NUM_OF_PLNT], ''), '|',
                 ISNULL(S.[VNDR_NUM_OF_PLNT], ''), '|',
                 ISNULL(S.[FACTORY_CLNDR_KEY], ''), '|',
                 ISNULL(S.[PLNT_LINE_1_DESC], ''), '|',
                 ISNULL(S.[PLNT_LINE_2_DESC], ''), '|',
                 ISNULL(S.[PLNT_CITY_NAME], ''), '|',
                 ISNULL(S.[PLNT_ZIP_CD], ''), '|',
                 ISNULL(S.[PLNT_CNTRY_CD], ''), '|',
                 ISNULL(S.[PLNT_CNTRY_NAME], ''), '|',
                 ISNULL(S.[TAX_JRSDCTN], ''), '|',
                 ISNULL(S.[ETL_SRC_SYS_CD], ''), '|',
                 ISNULL(S.[ETL_EFFECTV_BEGIN_DATE], ''), '|',
                 ISNULL(S.[ETL_EFFECTV_END_DATE], ''), '|',
                 ISNULL(S.[ETL_CURR_RCD_IND], ''), '|',
                 ISNULL(S.[ETL_CREATED_TS], ''), '|',
                 ISNULL(S.[ETL_UPDTD_TS], ''), '|',
                 ISNULL(S.[IS_P001], ''), '|',
                 ISNULL(S.[IS_P002], ''), '|',
                 ISNULL(S.[CMPNY_DESC], ''), '|',
                 ISNULL(S.[SALES_ORG_DESC], ''), '|',
                 ISNULL(S.[FACTORY_CLNDR_DESC], ''), '|',
                 ISNULL(S.[PURNG_ORGZTN_DESC], ''), '|',
                 ISNULL(S.[PLNT_TAX_IND], ''), '|',
                 ISNULL(S.[PLNT_TAX_IND_DESC], ''), '|',
                 ISNULL(S.[SHIPG_RECVNG_PNT], ''), '|',
                 ISNULL(S.[SHIPG_RECVNG_PNT_DESC], '')

            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS S;

    -- Step 2: Update changed rows in target (hash mismatch)
    UPDATE T
    SET 
        [PLNT_KEY]	= S.[PLNT_KEY],
        [Location Plant ID]	= S.[PLNT_ID],
        [Location Legacy Plant ID]	= S.[LGCY_PLNT_ID],
        [Location Plant Name]	= S.[PLNT_NAME],
        [Location Plant Company Code]	= S.[CMPNY_CD],
        [Location Plant Sales Organization]	= S.[SALES_ORGZTN],
        [Location Plant Purchasing Organization]	= S.[PURNG_ORGZTN],
        [Location Plant Valuation Area]	= S.[VALTN_AREA],
        [Location Plant Customer Number]	= S.[CUST_NUM_OF_PLNT],
        [Location Plant Vendor Number]	= S.[VNDR_NUM_OF_PLNT],
        [Location Plant Factory Calendar Key]	= S.[FACTORY_CLNDR_KEY],
        [Location Plant Line 1 Description]	= S.[PLNT_LINE_1_DESC],
        [Location Plant Line 2 Description]	= S.[PLNT_LINE_2_DESC],
        [Location Plant City Name]	= S.[PLNT_CITY_NAME],
        [Location Plant Zip Code]	= S.[PLNT_ZIP_CD],
        [Location Plant Country Code]	= S.[PLNT_CNTRY_CD],
        [Location Plant Country Name]	= S.[PLNT_CNTRY_NAME],
        [Location Plant Tax Jurisdiction]	= S.[TAX_JRSDCTN],
        [ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
        [Location Plant Is P001]	= S.[IS_P001],
        [Location Plant Is P002]	= S.[IS_P002],
        [Location Plant Company Description]	= S.[CMPNY_DESC],
        [Location Plant Sales Organization Description]	= S.[SALES_ORG_DESC],
        [Location Plant Factory Calendar Description]	= S.[FACTORY_CLNDR_DESC],
        [Location Plant Purchasing Organization Description]	= S.[PURNG_ORGZTN_DESC],
        [Location Plant Tax Indicator]	= S.[PLNT_TAX_IND],
        [Location Plant Tax Indicator Description]	= S.[PLNT_TAX_IND_DESC],
        [Location Plant Shipping/Receiving Point]	= S.[SHIPG_RECVNG_PNT],
        [Location Plant Shipping/Receiving Point Description]	= S.[SHIPG_RECVNG_PNT_DESC]


    FROM [GLOBAL_EDW].[Propelis].[LOCATION_PLANT] AS T
    INNER JOIN #SourceData AS S
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
				ISNULL(T.[PLNT_KEY], ''), '|',
                ISNULL(T.[Location Plant ID], ''), '|',
                ISNULL(T.[Location Legacy Plant ID], ''), '|',
                ISNULL(T.[Location Plant Name], ''), '|',
                ISNULL(T.[Location Plant Company Code], ''), '|',
                ISNULL(T.[Location Plant Sales Organization], ''), '|',
                ISNULL(T.[Location Plant Purchasing Organization], ''), '|',
                ISNULL(T.[Location Plant Valuation Area], ''), '|',
                ISNULL(T.[Location Plant Customer Number], ''), '|',
                ISNULL(T.[Location Plant Vendor Number], ''), '|',
                ISNULL(T.[Location Plant Factory Calendar Key], ''), '|',
                ISNULL(T.[Location Plant Line 1 Description], ''), '|',
                ISNULL(T.[Location Plant Line 2 Description], ''), '|',
                ISNULL(T.[Location Plant City Name], ''), '|',
                ISNULL(T.[Location Plant Zip Code], ''), '|',
                ISNULL(T.[Location Plant Country Code], ''), '|',
                ISNULL(T.[Location Plant Country Name], ''), '|',
                ISNULL(T.[Location Plant Tax Jurisdiction], ''), '|',
                ISNULL(T.[ETL_SRC_SYS_CD], ''), '|',
                ISNULL(T.[ETL_EFFECTV_BEGIN_DATE], ''), '|',
                ISNULL(T.[ETL_EFFECTV_END_DATE], ''), '|',
                ISNULL(T.[ETL_CURR_RCD_IND], ''), '|',
                ISNULL(T.[ETL_CREATED_TS], ''), '|',
                ISNULL(T.[ETL_UPDTD_TS], ''), '|',
                ISNULL(T.[Location Plant Is P001], ''), '|',
                ISNULL(T.[Location Plant Is P002], ''), '|',
                ISNULL(T.[Location Plant Company Description], ''), '|',
                ISNULL(T.[Location Plant Sales Organization Description], ''), '|',
                ISNULL(T.[Location Plant Factory Calendar Description], ''), '|',
                ISNULL(T.[Location Plant Purchasing Organization Description], ''), '|',
                ISNULL(T.[Location Plant Tax Indicator], ''), '|',
                ISNULL(T.[Location Plant Tax Indicator Description], ''), '|',
                ISNULL(T.[Location Plant Shipping/Receiving Point], ''), '|',
                ISNULL(T.[Location Plant Shipping/Receiving Point Description], '')


            )
        ) <> S.HashKey;

    -- Step 3: Insert only new rows not already in target
	
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

    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[LOCATION_PLANT] AS T
        WHERE T.[PLNT_KEY] = S.[PLNT_KEY]
    );

END;
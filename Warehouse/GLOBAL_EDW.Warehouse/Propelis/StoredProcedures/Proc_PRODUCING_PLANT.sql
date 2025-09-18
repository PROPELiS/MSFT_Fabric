CREATE   PROCEDURE Propelis.Proc_PRODUCING_PLANT
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
        [Producing Plant ID]	= S.[PLNT_ID],
        [Producing Legacy Plant ID]	= S.[LGCY_PLNT_ID],
        [Producing Plant Name]	= S.[PLNT_NAME],
        [Producing Plant Company Code]	= S.[CMPNY_CD],
        [Producing Plant Sales Organization]	= S.[SALES_ORGZTN],
        [Producing Plant Purchasing Organization]	= S.[PURNG_ORGZTN],
        [Producing Plant Valuation Area]	= S.[VALTN_AREA],
        [Producing Plant Customer Number]	= S.[CUST_NUM_OF_PLNT],
        [Producing Plant Vendor Number]	= S.[VNDR_NUM_OF_PLNT],
        [Producing Plant Factory Calendar Key]	= S.[FACTORY_CLNDR_KEY],
        [Producing Plant Line 1 Description]	= S.[PLNT_LINE_1_DESC],
        [Producing Plant Line 2 Description]	= S.[PLNT_LINE_2_DESC],
        [Producing Plant City Name]	= S.[PLNT_CITY_NAME],
        [Producing Plant Zip Code]	= S.[PLNT_ZIP_CD],
        [Producing Plant Country Code]	= S.[PLNT_CNTRY_CD],
        [Producing Plant Country Name]	= S.[PLNT_CNTRY_NAME],
        [Producing Plant Tax Jurisdiction]	= S.[TAX_JRSDCTN],
        [ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
        [Producing Plant Is P001]	= S.[IS_P001],
        [Producing Plant Is P002]	= S.[IS_P002],
        [Producing Plant Company Description]	= S.[CMPNY_DESC],
        [Producing Plant Sales Organization Description]	= S.[SALES_ORG_DESC],
        [Producing Plant Factory Calendar Description]	= S.[FACTORY_CLNDR_DESC],
        [Producing Plant Purchasing Organization Description]	= S.[PURNG_ORGZTN_DESC],
        [Producing Plant Tax Indicator]	= S.[PLNT_TAX_IND],
        [Producing Plant Tax Indicator Description]	= S.[PLNT_TAX_IND_DESC],
        [Producing Plant Shipping/Receiving Point]	= S.[SHIPG_RECVNG_PNT],
        [Producing Plant Shipping/Receiving Point Description]	= S.[SHIPG_RECVNG_PNT_DESC]


    FROM [GLOBAL_EDW].[Propelis].[PRODUCING_PLANT] AS T
    INNER JOIN #SourceData AS S
        ON T.[PLNT_KEY] = S.[PLNT_KEY]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
				ISNULL(T.[PLNT_KEY], ''), '|',
                ISNULL(T.[Producing Plant ID], ''), '|',
                ISNULL(T.[Producing Legacy Plant ID], ''), '|',
                ISNULL(T.[Producing Plant Name], ''), '|',
                ISNULL(T.[Producing Plant Company Code], ''), '|',
                ISNULL(T.[Producing Plant Sales Organization], ''), '|',
                ISNULL(T.[Producing Plant Purchasing Organization], ''), '|',
                ISNULL(T.[Producing Plant Valuation Area], ''), '|',
                ISNULL(T.[Producing Plant Customer Number], ''), '|',
                ISNULL(T.[Producing Plant Vendor Number], ''), '|',
                ISNULL(T.[Producing Plant Factory Calendar Key], ''), '|',
                ISNULL(T.[Producing Plant Line 1 Description], ''), '|',
                ISNULL(T.[Producing Plant Line 2 Description], ''), '|',
                ISNULL(T.[Producing Plant City Name], ''), '|',
                ISNULL(T.[Producing Plant Zip Code], ''), '|',
                ISNULL(T.[Producing Plant Country Code], ''), '|',
                ISNULL(T.[Producing Plant Country Name], ''), '|',
                ISNULL(T.[Producing Plant Tax Jurisdiction], ''), '|',
                ISNULL(T.[ETL_SRC_SYS_CD], ''), '|',
                ISNULL(T.[ETL_EFFECTV_BEGIN_DATE], ''), '|',
                ISNULL(T.[ETL_EFFECTV_END_DATE], ''), '|',
                ISNULL(T.[ETL_CURR_RCD_IND], ''), '|',
                ISNULL(T.[ETL_CREATED_TS], ''), '|',
                ISNULL(T.[ETL_UPDTD_TS], ''), '|',
                ISNULL(T.[Producing Plant Is P001], ''), '|',
                ISNULL(T.[Producing Plant Is P002], ''), '|',
                ISNULL(T.[Producing Plant Company Description], ''), '|',
                ISNULL(T.[Producing Plant Sales Organization Description], ''), '|',
                ISNULL(T.[Producing Plant Factory Calendar Description], ''), '|',
                ISNULL(T.[Producing Plant Purchasing Organization Description], ''), '|',
                ISNULL(T.[Producing Plant Tax Indicator], ''), '|',
                ISNULL(T.[Producing Plant Tax Indicator Description], ''), '|',
                ISNULL(T.[Producing Plant Shipping/Receiving Point], ''), '|',
                ISNULL(T.[Producing Plant Shipping/Receiving Point Description], '')


            )
        ) <> S.HashKey;

    -- Step 3: Insert only new rows not already in target
	
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

    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PRODUCING_PLANT] AS T
        WHERE T.[PLNT_KEY] = S.[PLNT_KEY]
    );

END;
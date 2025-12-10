CREATE      PROCEDURE [Propelis].[Proc_PRINTER_CUST]
AS
BEGIN
    -- Step 1: Update existing records in target table
    UPDATE T
    SET
         T.[CUST_KEY] = S.[CUST_KEY],
         T.[Printer Customer ID] = S.[CUST_ID],
         T.[Printer Legacy Customer ID] = S.[LGCY_CUST_ID],
         T.[Printer Customer Name] = S.[CUST_NAME],
         T.[Printer Cust Line 2 Description] = S.[CUST_LINE_2_DESC],
         T.[Printer Cust Country Code] = S.[CNTRY_CD],
         T.[Printer Cust Plant ID] = S.[PLNT_ID],
         T.[Printer Cust Segment ID] = S.[SEG_ID],
         T.[Printer Cust Industry] = S.[INDUS],
         T.[Printer Cust Vendor ID] = S.[VNDR_ID],
         T.[Printer Cust Customer Group Code] = S.[CUST_GROUP_CD],
         T.[Printer Cust Account Group Code] = S.[CUST_ACCT_GROUP_CD],
         T.[Printer Cust Title] = S.[TITLE],
         T.[Printer Cust Search Term] = S.[SEARCH_TERM],
         T.[Printer Cust Street] = S.[STR],
         T.[Printer Cust City] = S.[CITY_NAME],
         T.[Printer Cust Zip Code] = S.[ZIP_CD],
         T.[Printer Cust State Code] = S.[ST_CD],
         T.[Printer Cust Region Code] = S.[REGN_CD],
         T.[Printer Cust Telephone Number 1] = S.[TEL_NUM_1_CD],
         T.[Printer Cust Telephone Number 2] = S.[TEL_NUM_2_CD],
         T.[Printer Cust Fax Number Code] = S.[FAX_NUM_CD],
         T.[Printer Cust Tax Number 1 Code] = S.[TAX_NUM_1_CD],
         T.[Printer Cust Tax Number 2 Code] = S.[TAX_NUM_2_CD],
         T.[Printer Cust VAT Number Code] = S.[VAT_NUM_CD],
         T.[Printer Cust Credit Limit] = S.[CREDIT_LMT],
         T.[Printer Cust Bonus Rate] = S.[BONUS_RT],
         T.[Printer Cust Express Station] = S.[EXPRESS_STATION],
         T.[Printer Cust Transportation Zone] = S.[TRANSTN_ZONE],
         T.[Printer Cust Printer Location Characteristics] = S.[PRINTER_LOC_CHARS],
         T.[Printer Cust Printer Region Characteristics] = S.[PRINTER_REGN_CHARS],
         T.[Printer Cust Printer Site Characteristics] = S.[PRINTER_SITE_CHARS],
         T.[Printer Cust Customer Created Date] = S.[CREATED_DATE],
         T.[Printer Cust Created User ID] = S.[CREATED_USR_ID],
         T.[Printer Cust Customer Updated Date] = S.[UPDTD_DATE],
         T.[Printer Cust Updated User ID] = S.[UPDTD_USR_ID],
         T.[Printer Cust Payment Terms Code] = S.[PYMT_TRMS_CD],
         T.[Printer Cust Sales Distribution] = S.[SALES_DISTR],
         T.[Printer Cust Order Block For Sales Area] = S.[ORDER_BLK_FOR_SALES_AREA],
         T.[Printer Cust Delivery Block For Sales Area] = S.[DLVRY_BLK_FOR_SALES_AREA],
         T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
         T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
         T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
         T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
         T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
         T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
         T.[Printer Cust Corporate Group] = S.[CORPORATE_GROUP],
         T.[Printer Cust Classification] = S.[CUST_CLSFTN],
         T.[Printer Cust Classification Description] = S.[CUST_CLSFTN_DESC],
         T.[Printer Cust Printer Location] = S.[PRINTER_LOC],
         T.[Printer Cust Printer Site] = S.[PRINTER_SITE],
         T.[Printer Cust Printer Region] = S.[PRINTER_REGN],
         T.[Printer Cust Specific Field Label 01] = S.[CUST_SPECIFIC_FLD_LBL_01],
         T.[Printer Cust Specific Field Label 02] = S.[CUST_SPECIFIC_FLD_LBL_02],
         T.[Printer Cust Specific Field Label 03] = S.[CUST_SPECIFIC_FLD_LBL_03],
         T.[Printer Cust Specific Field Label 04] = S.[CUST_SPECIFIC_FLD_LBL_04],
         T.[Printer Cust Specific Field Label 05] = S.[CUST_SPECIFIC_FLD_LBL_05],
         T.[Printer Cust Specific Field Label 06] = S.[CUST_SPECIFIC_FLD_LBL_06],
         T.[Printer Cust Specific Field Label 07] = S.[CUST_SPECIFIC_FLD_LBL_07],
         T.[Printer Cust Specific Field Label 08] = S.[CUST_SPECIFIC_FLD_LBL_08],
         T.[Printer Cust Specific Field Label 09] = S.[CUST_SPECIFIC_FLD_LBL_09],
         T.[Printer Cust Specific Field Label 10] = S.[CUST_SPECIFIC_FLD_LBL_10],
         T.[Printer Cust Specific Field Label 11] = S.[CUST_SPECIFIC_FLD_LBL_11],
         T.[Printer Cust Specific Field Label 12] = S.[CUST_SPECIFIC_FLD_LBL_12],
         T.[Printer Cust Specific Field Label 13] = S.[CUST_SPECIFIC_FLD_LBL_13],
         T.[Printer Cust Specific Field Label 14] = S.[CUST_SPECIFIC_FLD_LBL_14],
         T.[Printer Cust Specific Field Label 15] = S.[CUST_SPECIFIC_FLD_LBL_15],
         T.[Printer Cust Specific Field Label 16] = S.[CUST_SPECIFIC_FLD_LBL_16],
         T.[Printer Cust Specific Field Label 17] = S.[CUST_SPECIFIC_FLD_LBL_17],
         T.[Printer Cust Specific Field Label 18] = S.[CUST_SPECIFIC_FLD_LBL_18],
         T.[Printer Cust Specific Field Label 19] = S.[CUST_SPECIFIC_FLD_LBL_19],
         T.[Printer Cust Specific Field Label 20] = S.[CUST_SPECIFIC_FLD_LBL_20],
         T.[Printer Cust Condition Group 1] = S.[CUST_COND_GROUP_1],
         T.[Printer Cust Industry Code 1] = S.[INDUS_CD_1],
         T.[Printer Cust Industry Code 2] = S.[INDUS_CD_2],
         T.[Printer Cust Industry Description] = S.[INDUS_DESC],
         T.[Printer Cust Industry Code 1 Description] = S.[INDUS_CD_1_DESC],
         T.[Printer Cust Industry Code 2 Description] = S.[INDUS_CD_2_DESC],
         T.[Printer Cust Assignment to Hierarchy] = S.[ASSIGN_TO_HIERCHY],
         T.[Printer Cust Account Group Description] = S.[CUST_ACCT_GROUP_DESC],
         T.[Printer Cust Region Description] = S.[REGN_DESC],
         T.[Printer Cust Country Description] = S.[CNTRY_DESC],
         T.[Printer Cust Transportation Zone Description] = S.[TRANSTN_ZONE_DESC],
         T.[Printer Cust Plant Description] = S.[PLNT_DESC],
         T.[Printer Cust Condition Group Description] = S.[CUST_COND_GROUP_DESC],
         T.[Printer Cust Cental Billing Block] = S.[CENTRAL_BILLING_BLK],
         T.[Printer Cust Cental Billing Block Description] = S.[CENTRAL_BILLING_BLK_DESC],
         T.[Printer Cust Central Delivery Block] = S.[CENTRAL_DLVRY_BLK],
         T.[Printer Cust Central Delivery Block Description] = S.[CENTRAL_DLVRY_BLK_DESC],
         T.[Printer Cust Deletion Indicator] = S.[DELETION_IND],
         T.[Printer Cust Central Posting Block] = S.[CENTRAL_PSTNG_BLK],
         T.[Printer Cust Tax Jurisdiction] = S.[TAX_JRSDCTN],
         T.[Printer Cust Central Sales Block] = S.[CENTRAL_SALES_BLK],
         T.[Printer Cust Central Deletion Indicator] = S.[CENTRAL_DELETION_IND],
         T.[Printer Cust Language Key] = S.[LANG_KEY],
         T.[Printer Cust Trading Partner] = S.[TRADING_PRTNR],
         T.[Printer Cust Trading Partner Description] = S.[TRADING_PRTNR_DESC],
         T.[Printer Cust Annual Sales] = S.[ANNUAL_SALES],
         T.[Printer Cust Annual Sales Year] = S.[ANNUAL_SALES_YR],
         T.[Printer Cust Sales Currency] = S.[SALES_CRNCY],
         T.[Printer Cust Sales Currency Description] = S.[SALES_CRNCY_DESC],
         T.[Printer Cust One Time Account Group] = S.[ONE_TM_CUST_ACCT_GROUP],
         T.[Printer Cust One Time Account Group Description] = S.[ONE_TM_CUST_ACCT_GROUP_DESC],
         T.[Printer Cust One Time Indicator] = S.[ONE_TM_CUST_IND],
         T.[Printer Cust Industry Type] = S.[INDUS_TYP],
         T.[Printer Cust Business Type] = S.[BIZ_TYP],
         T.[Printer Cust DUNS Number] = S.[DUNS_NUM],
         T.[Printer Cust DUNS Plus 4] = S.[DUNS_PLUS_4],
         T.[Printer Cust City Coordinates] = S.[CITY_COORDINATES],
         T.[Printer Cust Tax Number 4 Code] = S.[TAX_NUM_4_CD],
         T.[Printer Cust Attribute 3] = S.[ATTRIB_3],
         T.[Printer Cust Attribute 3 Description] = S.[ATTRIB_3_DESC],
         T.[Printer Cust E-Mail Address] = S.[E_MAIL_ADDR],
         T.[Printer Cust Mobile Number] = S.[MOBILE_NUM],
         T.[Printer Cust Search Term 2] = S.[SEARCH_TERM_2],
         T.[Printer Cust Street 2] = S.[STR_2],
         T.[Printer Cust Street 3] = S.[STR_3],
         T.[Printer Cust Street 4] = S.[STR_4],
         T.[Printer Cust Street 5] = S.[STR_5],
         T.[Printer Cust PO Box] = S.[PO_BOX],
         T.[Printer Cust PO Box Postal Code] = S.[PO_BOX_POSTAL_CD],
         T.[Printer Cust PO Box City] = S.[PO_BOX_CITY],
         T.[Printer Cust Region For PO Box] = S.[REGN_FOR_PO_BOX],
         T.[Printer Cust PO Box Country] = S.[PO_BOX_CNTRY],
         T.[Printer Cust Transport Zone] = S.[TRNSPRT_ZONE],
         T.[Printer Cust Address Notes] = S.[ADDR_NOTES],
         T.[Printer Cust Tax Number 3 Code] = S.[TAX_NUM_3],
         T.[Printer Cust Tax Number 5 Code] = S.[TAX_NUM_5],
         T.[Printer Cust Industry Code 3] = S.[INDUS_CD_3],
         T.[Printer Cust Industry Code 3 Description] = S.[INDUS_CD_3_DESC],
         T.[Printer Cust Industry Code 4] = S.[INDUS_CD_4],
         T.[Printer Cust Industry Code 4 Description] = S.[INDUS_CD_4_DESC],
         T.[Printer Cust Industry Code 5] = S.[INDUS_CD_5],
         T.[Printer Cust Industry Code 5 Description] = S.[INDUS_CD_5_DESC],
         T.[Printer Cust Central Order Block For Customer] = S.[CENTRAL_ORDER_BLK_FOR_CUST],
         T.[Printer Cust Payment Block] = S.[PYMT_BLK]

    FROM [GLOBAL_EDW].[Propelis].[PRINTER_CUST] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTOMR_CUR_D] AS S
        ON T.[CUST_KEY] = S.[CUST_KEY];

    -- Step 2: Insert new records from mirror to target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[PRINTER_CUST] (
        [CUST_KEY]
        ,[Printer Customer ID]
        ,[Printer Legacy Customer ID]
        ,[Printer Customer Name]
        ,[Printer Cust Line 2 Description]
        ,[Printer Cust Country Code]
        ,[Printer Cust Plant ID]
        ,[Printer Cust Segment ID]
        ,[Printer Cust Industry]
        ,[Printer Cust Vendor ID]
        ,[Printer Cust Customer Group Code]
        ,[Printer Cust Account Group Code]
        ,[Printer Cust Title]
        ,[Printer Cust Search Term]
        ,[Printer Cust Street]
        ,[Printer Cust City]
        ,[Printer Cust Zip Code]
        ,[Printer Cust State Code]
        ,[Printer Cust Region Code]
        ,[Printer Cust Telephone Number 1]
        ,[Printer Cust Telephone Number 2]
        ,[Printer Cust Fax Number Code]
        ,[Printer Cust Tax Number 1 Code]
        ,[Printer Cust Tax Number 2 Code]
        ,[Printer Cust VAT Number Code]
        ,[Printer Cust Credit Limit]
        ,[Printer Cust Bonus Rate]
        ,[Printer Cust Express Station]
        ,[Printer Cust Transportation Zone]
        ,[Printer Cust Printer Location Characteristics]
        ,[Printer Cust Printer Region Characteristics]
        ,[Printer Cust Printer Site Characteristics]
        ,[Printer Cust Customer Created Date]
        ,[Printer Cust Created User ID]
        ,[Printer Cust Customer Updated Date]
        ,[Printer Cust Updated User ID]
        ,[Printer Cust Payment Terms Code]
        ,[Printer Cust Sales Distribution]
        ,[Printer Cust Order Block For Sales Area]
        ,[Printer Cust Delivery Block For Sales Area]
        ,[ETL_SRC_SYS_CD]
        ,[ETL_EFFECTV_BEGIN_DATE]
        ,[ETL_EFFECTV_END_DATE]
        ,[ETL_CURR_RCD_IND]
        ,[ETL_CREATED_TS]
        ,[ETL_UPDTD_TS]
        ,[Printer Cust Corporate Group]
        ,[Printer Cust Classification]
        ,[Printer Cust Classification Description]
        ,[Printer Cust Printer Location]
        ,[Printer Cust Printer Site]
        ,[Printer Cust Printer Region]
        ,[Printer Cust Specific Field Label 01]
        ,[Printer Cust Specific Field Label 02]
        ,[Printer Cust Specific Field Label 03]
        ,[Printer Cust Specific Field Label 04]
        ,[Printer Cust Specific Field Label 05]
        ,[Printer Cust Specific Field Label 06]
        ,[Printer Cust Specific Field Label 07]
        ,[Printer Cust Specific Field Label 08]
        ,[Printer Cust Specific Field Label 09]
        ,[Printer Cust Specific Field Label 10]
        ,[Printer Cust Specific Field Label 11]
        ,[Printer Cust Specific Field Label 12]
        ,[Printer Cust Specific Field Label 13]
        ,[Printer Cust Specific Field Label 14]
        ,[Printer Cust Specific Field Label 15]
        ,[Printer Cust Specific Field Label 16]
        ,[Printer Cust Specific Field Label 17]
        ,[Printer Cust Specific Field Label 18]
        ,[Printer Cust Specific Field Label 19]
        ,[Printer Cust Specific Field Label 20]
        ,[Printer Cust Condition Group 1]
        ,[Printer Cust Industry Code 1]
        ,[Printer Cust Industry Code 2]
        ,[Printer Cust Industry Description]
        ,[Printer Cust Industry Code 1 Description]
        ,[Printer Cust Industry Code 2 Description]
        ,[Printer Cust Assignment to Hierarchy]
        ,[Printer Cust Account Group Description]
        ,[Printer Cust Region Description]
        ,[Printer Cust Country Description]
        ,[Printer Cust Transportation Zone Description]
        ,[Printer Cust Plant Description]
        ,[Printer Cust Condition Group Description]
        ,[Printer Cust Cental Billing Block]
        ,[Printer Cust Cental Billing Block Description]
        ,[Printer Cust Central Delivery Block]
        ,[Printer Cust Central Delivery Block Description]
        ,[Printer Cust Deletion Indicator]
        ,[Printer Cust Central Posting Block]
        ,[Printer Cust Tax Jurisdiction]
        ,[Printer Cust Central Sales Block]
        ,[Printer Cust Central Deletion Indicator]
        ,[Printer Cust Language Key]
        ,[Printer Cust Trading Partner]
        ,[Printer Cust Trading Partner Description]
        ,[Printer Cust Annual Sales]
        ,[Printer Cust Annual Sales Year]
        ,[Printer Cust Sales Currency]
        ,[Printer Cust Sales Currency Description]
        ,[Printer Cust One Time Account Group]
        ,[Printer Cust One Time Account Group Description]
        ,[Printer Cust One Time Indicator]
        ,[Printer Cust Industry Type]
        ,[Printer Cust Business Type]
        ,[Printer Cust DUNS Number]
        ,[Printer Cust DUNS Plus 4]
        ,[Printer Cust City Coordinates]
        ,[Printer Cust Tax Number 4 Code]
        ,[Printer Cust Attribute 3]
        ,[Printer Cust Attribute 3 Description]
        ,[Printer Cust E-Mail Address]
        ,[Printer Cust Mobile Number]
        ,[Printer Cust Search Term 2]
        ,[Printer Cust Street 2]
        ,[Printer Cust Street 3]
        ,[Printer Cust Street 4]
        ,[Printer Cust Street 5]
        ,[Printer Cust PO Box]
        ,[Printer Cust PO Box Postal Code]
        ,[Printer Cust PO Box City]
        ,[Printer Cust Region For PO Box]
        ,[Printer Cust PO Box Country]
        ,[Printer Cust Transport Zone]
        ,[Printer Cust Address Notes]
        ,[Printer Cust Tax Number 3 Code]
        ,[Printer Cust Tax Number 5 Code]
        ,[Printer Cust Industry Code 3]
        ,[Printer Cust Industry Code 3 Description]
        ,[Printer Cust Industry Code 4]
        ,[Printer Cust Industry Code 4 Description]
        ,[Printer Cust Industry Code 5]
        ,[Printer Cust Industry Code 5 Description]
        ,[Printer Cust Central Order Block For Customer]
        ,[Printer Cust Payment Block]

    )
    SELECT
             S.[CUST_KEY]
            ,S.[CUST_ID]  
            ,S.[LGCY_CUST_ID]  
            ,S.[CUST_NAME]  
            ,S.[CUST_LINE_2_DESC] 
            ,S.[CNTRY_CD] 
            ,S.[PLNT_ID]  
            ,S.[SEG_ID]  
            ,S.[INDUS]  
            ,S.[VNDR_ID]  
            ,S.[CUST_GROUP_CD]  
            ,S.[CUST_ACCT_GROUP_CD]  
            ,S.[TITLE] 
            ,S.[SEARCH_TERM]  
            ,S.[STR]  
            ,S.[CITY_NAME] 
            ,S.[ZIP_CD]  
            ,S.[ST_CD]  
            ,S.[REGN_CD]  
            ,S.[TEL_NUM_1_CD]  
            ,S.[TEL_NUM_2_CD]  
            ,S.[FAX_NUM_CD]  
            ,S.[TAX_NUM_1_CD]  
            ,S.[TAX_NUM_2_CD]  
            ,S.[VAT_NUM_CD] 
            ,S.[CREDIT_LMT] 
            ,S.[BONUS_RT] 
            ,S.[EXPRESS_STATION] 
            ,S.[TRANSTN_ZONE]  
            ,S.[PRINTER_LOC_CHARS]  
            ,S.[PRINTER_REGN_CHARS]  
            ,S.[PRINTER_SITE_CHARS]  
            ,S.[CREATED_DATE]  
            ,S.[CREATED_USR_ID]  
            ,S.[UPDTD_DATE]  
            ,S.[UPDTD_USR_ID]  
            ,S.[PYMT_TRMS_CD]  
            ,S.[SALES_DISTR] 
            ,S.[ORDER_BLK_FOR_SALES_AREA]  
            ,S.[DLVRY_BLK_FOR_SALES_AREA]  
            ,S.[ETL_SRC_SYS_CD]  
            ,S.[ETL_EFFECTV_BEGIN_DATE]  
            ,S.[ETL_EFFECTV_END_DATE]  
            ,S.[ETL_CURR_RCD_IND]  
            ,S.[ETL_CREATED_TS] 
            ,S.[ETL_UPDTD_TS] 
            ,S.[CORPORATE_GROUP]  
            ,S.[CUST_CLSFTN]  
            ,S.[CUST_CLSFTN_DESC]  
            ,S.[PRINTER_LOC]  
            ,S.[PRINTER_SITE]  
            ,S.[PRINTER_REGN]  
            ,S.[CUST_SPECIFIC_FLD_LBL_01]  
            ,S.[CUST_SPECIFIC_FLD_LBL_02]  
            ,S.[CUST_SPECIFIC_FLD_LBL_03]  
            ,S.[CUST_SPECIFIC_FLD_LBL_04]  
            ,S.[CUST_SPECIFIC_FLD_LBL_05]  
            ,S.[CUST_SPECIFIC_FLD_LBL_06]  
            ,S.[CUST_SPECIFIC_FLD_LBL_07]  
            ,S.[CUST_SPECIFIC_FLD_LBL_08]  
            ,S.[CUST_SPECIFIC_FLD_LBL_09]  
            ,S.[CUST_SPECIFIC_FLD_LBL_10]  
            ,S.[CUST_SPECIFIC_FLD_LBL_11]  
            ,S.[CUST_SPECIFIC_FLD_LBL_12]  
            ,S.[CUST_SPECIFIC_FLD_LBL_13]  
            ,S.[CUST_SPECIFIC_FLD_LBL_14]  
            ,S.[CUST_SPECIFIC_FLD_LBL_15]  
            ,S.[CUST_SPECIFIC_FLD_LBL_16]  
            ,S.[CUST_SPECIFIC_FLD_LBL_17]  
            ,S.[CUST_SPECIFIC_FLD_LBL_18]  
            ,S.[CUST_SPECIFIC_FLD_LBL_19]  
            ,S.[CUST_SPECIFIC_FLD_LBL_20]  
            ,S.[CUST_COND_GROUP_1]  
            ,S.[INDUS_CD_1]  
            ,S.[INDUS_CD_2]  
            ,S.[INDUS_DESC]  
            ,S.[INDUS_CD_1_DESC]  
            ,S.[INDUS_CD_2_DESC]  
            ,S.[ASSIGN_TO_HIERCHY]  
            ,S.[CUST_ACCT_GROUP_DESC]  
            ,S.[REGN_DESC]  
            ,S.[CNTRY_DESC]  
            ,S.[TRANSTN_ZONE_DESC]  
            ,S.[PLNT_DESC]  
            ,S.[CUST_COND_GROUP_DESC]  
            ,S.[CENTRAL_BILLING_BLK]  
            ,S.[CENTRAL_BILLING_BLK_DESC]  
            ,S.[CENTRAL_DLVRY_BLK]  
            ,S.[CENTRAL_DLVRY_BLK_DESC]  
            ,S.[DELETION_IND]  
            ,S.[CENTRAL_PSTNG_BLK]  
            ,S.[TAX_JRSDCTN]  
            ,S.[CENTRAL_SALES_BLK]  
            ,S.[CENTRAL_DELETION_IND]  
            ,S.[LANG_KEY]  
            ,S.[TRADING_PRTNR]  
            ,S.[TRADING_PRTNR_DESC]  
            ,S.[ANNUAL_SALES] 
            ,S.[ANNUAL_SALES_YR]  
            ,S.[SALES_CRNCY]  
            ,S.[SALES_CRNCY_DESC]  
            ,S.[ONE_TM_CUST_ACCT_GROUP]  
            ,S.[ONE_TM_CUST_ACCT_GROUP_DESC]  
            ,S.[ONE_TM_CUST_IND]  
            ,S.[INDUS_TYP]  
            ,S.[BIZ_TYP]  
            ,S.[DUNS_NUM] 
            ,S.[DUNS_PLUS_4]  
            ,S.[CITY_COORDINATES]  
            ,S.[TAX_NUM_4_CD]  
            ,S.[ATTRIB_3]  
            ,S.[ATTRIB_3_DESC]  
            ,S.[E_MAIL_ADDR]  
            ,S.[MOBILE_NUM]  
            ,S.[SEARCH_TERM_2]  
            ,S.[STR_2]  
            ,S.[STR_3]  
            ,S.[STR_4]  
            ,S.[STR_5]  
            ,S.[PO_BOX]  
            ,S.[PO_BOX_POSTAL_CD]  
            ,S.[PO_BOX_CITY]  
            ,S.[REGN_FOR_PO_BOX]  
            ,S.[PO_BOX_CNTRY]  
            ,S.[TRNSPRT_ZONE]  
            ,S.[ADDR_NOTES]  
            ,S.[TAX_NUM_3] 
            ,S.[TAX_NUM_5]  
            ,S.[INDUS_CD_3]  
            ,S.[INDUS_CD_3_DESC]  
            ,S.[INDUS_CD_4]  
            ,S.[INDUS_CD_4_DESC]  
            ,S.[INDUS_CD_5]  
            ,S.[INDUS_CD_5_DESC]  
            ,S.[CENTRAL_ORDER_BLK_FOR_CUST]  
             ,S.[PYMT_BLK] 
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTOMR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PRINTER_CUST] AS T
        ON T.[CUST_KEY] = S.[CUST_KEY]
    WHERE T.[CUST_KEY] IS NULL;

END
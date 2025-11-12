CREATE   PROCEDURE [Propelis].[Proc_AGENCY_CUST]
AS
BEGIN
    -- Step 1: Update existing records in target table
    UPDATE T
    SET
                T.[CUST_KEY]	= S.[CUST_KEY],
                T.[Agency Customer ID]	= S.[CUST_ID],
                T.[Agency Legacy Customer ID]	= S.[LGCY_CUST_ID],
                T.[Agency Customer Name]	= S.[CUST_NAME],
                T.[Agency Cust Line 2 Description]	= S.[CUST_LINE_2_DESC],
                T.[Agency Cust Country Code]	= S.[CNTRY_CD],
                T.[Agency Cust Plant ID]	= S.[PLNT_ID],
                T.[Agency Cust Segment ID]	= S.[SEG_ID],
                T.[Agency Cust Industry]	= S.[INDUS],
                T.[Agency Cust Vendor ID]	= S.[VNDR_ID],
                T.[Agency Cust Customer Group Code]	= S.[CUST_GROUP_CD],
                T.[Agency Cust Account Group Code]	= S.[CUST_ACCT_GROUP_CD],
                T.[Agency Cust Title]	= S.[TITLE],
                T.[Agency Cust Search Term]	= S.[SEARCH_TERM],
                T.[Agency Cust Street]	= S.[STR],
                T.[Agency Cust City]	= S.[CITY_NAME],
                T.[Agency Cust Zip Code]	= S.[ZIP_CD],
                T.[Agency Cust State Code]	= S.[ST_CD],
                T.[Agency Cust Region Code]	= S.[REGN_CD],
                T.[Agency Cust Telephone Number 1]	= S.[TEL_NUM_1_CD],
                T.[Agency Cust Telephone Number 2]	= S.[TEL_NUM_2_CD],
                T.[Agency Cust Fax Number Code]	= S.[FAX_NUM_CD],
                T.[Agency Cust Tax Number 1 Code]	= S.[TAX_NUM_1_CD],
                T.[Agency Cust Tax Number 2 Code]	= S.[TAX_NUM_2_CD],
                T.[Agency Cust VAT Number Code]	= S.[VAT_NUM_CD],
                T.[Agency Cust Credit Limit]	= S.[CREDIT_LMT],
                T.[Agency Cust Bonus Rate]	= S.[BONUS_RT],
                T.[Agency Cust Express Station]	= S.[EXPRESS_STATION],
                T.[TRANSTN_ZONE]	= S.[TRANSTN_ZONE],
                T.[Agency Cust Printer Location Characteristics]	= S.[PRINTER_LOC_CHARS],
                T.[Agency Cust Printer Region Characteristics]	= S.[PRINTER_REGN_CHARS],
                T.[Agency Cust Printer Site Characteristics]	= S.[PRINTER_SITE_CHARS],
                T.[CREATED_DATE]	= S.[CREATED_DATE],
                T.[Agency Cust Created User ID]	= S.[CREATED_USR_ID],
                T.[UPDTD_DATE]	= S.[UPDTD_DATE],
                T.[Agency Cust Updated User ID]	= S.[UPDTD_USR_ID],
                T.[PYMT_TRMS_CD]	= S.[PYMT_TRMS_CD],
                T.[Agency Cust Sales Distribution]	= S.[SALES_DISTR],
                T.[Agency Cust Order Block For Sales Area]	= S.[ORDER_BLK_FOR_SALES_AREA],
                T.[Agency Cust Delivery Block For Sales Area]	= S.[DLVRY_BLK_FOR_SALES_AREA],
                T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
                T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
                T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
                T.[ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
                T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
                T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
                T.[Agency Cust Corporate Group]	= S.[CORPORATE_GROUP],
                T.[Agency Cust Classification]	= S.[CUST_CLSFTN],
                T.[Agency Cust Classification Description]	= S.[CUST_CLSFTN_DESC],
                T.[Agency Cust Printer Location]	= S.[PRINTER_LOC],
                T.[Agency Cust Printer Site]	= S.[PRINTER_SITE],
                T.[Agency Cust Printer Region]	= S.[PRINTER_REGN],
                T.[Agency Cust Specific Field Label 01]	= S.[CUST_SPECIFIC_FLD_LBL_01],
                T.[Agency Cust Specific Field Label 02]	= S.[CUST_SPECIFIC_FLD_LBL_02],
                T.[Agency Cust Specific Field Label 03]	= S.[CUST_SPECIFIC_FLD_LBL_03],
                T.[Agency Cust Specific Field Label 04]	= S.[CUST_SPECIFIC_FLD_LBL_04],
                T.[Agency Cust Specific Field Label 05]	= S.[CUST_SPECIFIC_FLD_LBL_05],
                T.[Agency Cust Specific Field Label 06]	= S.[CUST_SPECIFIC_FLD_LBL_06],
                T.[Agency Cust Specific Field Label 07]	= S.[CUST_SPECIFIC_FLD_LBL_07],
                T.[Agency Cust Specific Field Label 08]	= S.[CUST_SPECIFIC_FLD_LBL_08],
                T.[Agency Cust Specific Field Label 09]	= S.[CUST_SPECIFIC_FLD_LBL_09],
                T.[Agency Cust Specific Field Label 10]	= S.[CUST_SPECIFIC_FLD_LBL_10],
                T.[Agency Cust Specific Field Label 11]	= S.[CUST_SPECIFIC_FLD_LBL_11],
                T.[Agency Cust Specific Field Label 12]	= S.[CUST_SPECIFIC_FLD_LBL_12],
                T.[Agency Cust Specific Field Label 13]	= S.[CUST_SPECIFIC_FLD_LBL_13],
                T.[Agency Cust Specific Field Label 14]	= S.[CUST_SPECIFIC_FLD_LBL_14],
                T.[Agency Cust Specific Field Label 15]	= S.[CUST_SPECIFIC_FLD_LBL_15],
                T.[Agency Cust Specific Field Label 16]	= S.[CUST_SPECIFIC_FLD_LBL_16],
                T.[Agency Cust Specific Field Label 17]	= S.[CUST_SPECIFIC_FLD_LBL_17],
                T.[Agency Cust Specific Field Label 18]	= S.[CUST_SPECIFIC_FLD_LBL_18],
                T.[Agency Cust Specific Field Label 19]	= S.[CUST_SPECIFIC_FLD_LBL_19],
                T.[Agency Cust Specific Field Label 20]	= S.[CUST_SPECIFIC_FLD_LBL_20],
                T.[Agency Cust Condition Group 1]	= S.[CUST_COND_GROUP_1],
                T.[Agency Cust Industry Code 1]	= S.[INDUS_CD_1],
                T.[Agency Cust Industry Code 2]	= S.[INDUS_CD_2],
                T.[Agency Cust Industry Description]	= S.[INDUS_DESC],
                T.[Agency Cust Industry Code 1 Description]	= S.[INDUS_CD_1_DESC],
                T.[Agency Cust Industry Code 2 Description]	= S.[INDUS_CD_2_DESC],
                T.[Agency Cust Assignment to Hierarchy]	= S.[ASSIGN_TO_HIERCHY],
                T.[Agency Cust Account Group Description]	= S.[CUST_ACCT_GROUP_DESC],
                T.[Agency Cust Region Description]	= S.[REGN_DESC],
                T.[CNTRY_DESC]	= S.[CNTRY_DESC],
                T.[Agency Cust Transportation Zone Description]	= S.[TRANSTN_ZONE_DESC],
                T.[Agency Cust Plant Description]	= S.[PLNT_DESC],
                T.[Agency Cust Condition Group Description]	= S.[CUST_COND_GROUP_DESC],
                T.[Agency Cust Cental Billing Block]	= S.[CENTRAL_BILLING_BLK],
                T.[Agency Cust Cental Billing Block Description]	= S.[CENTRAL_BILLING_BLK_DESC],
                T.[Agency Cust Central Delivery Block]	= S.[CENTRAL_DLVRY_BLK],
                T.[Agency Cust Central Delivery Block Description]	= S.[CENTRAL_DLVRY_BLK_DESC],
                T.[Agency Cust Deletion Indicator]	= S.[DELETION_IND],
                T.[Agency Cust Central Posting Block]	= S.[CENTRAL_PSTNG_BLK],
                T.[Agency Cust Tax Jurisdiction]	= S.[TAX_JRSDCTN],
                T.[Agency Cust Central Sales Block]	= S.[CENTRAL_SALES_BLK],
                T.[Agency Cust Central Deletion Indicator]	= S.[CENTRAL_DELETION_IND],
                T.[Agency Cust Language Key]	= S.[LANG_KEY],
                T.[Agency Cust Trading Partner]	= S.[TRADING_PRTNR],
                T.[Agency Cust Trading Partner Description]	= S.[TRADING_PRTNR_DESC],
                T.[Agency Cust Annual Sales]	= S.[ANNUAL_SALES],
                T.[Agency Cust Annual Sales Year]	= S.[ANNUAL_SALES_YR],
                T.[Agency Cust Sales Currency]	= S.[SALES_CRNCY],
                T.[Agency Cust Sales Currency Description]	= S.[SALES_CRNCY_DESC],
                T.[Agency Cust One Time Account Group]	= S.[ONE_TM_CUST_ACCT_GROUP],
                T.[Agency Cust One Time Account Group Description]	= S.[ONE_TM_CUST_ACCT_GROUP_DESC],
                T.[Agency Cust One Time Indicator]	= S.[ONE_TM_CUST_IND],
                T.[Agency Cust Industry Type]	= S.[INDUS_TYP],
                T.[Agency Cust Business Type]	= S.[BIZ_TYP],
                T.[Agency Cust DUNS Number]	= S.[DUNS_NUM],
                T.[Agency Cust DUNS Plus 4]	= S.[DUNS_PLUS_4],
                T.[Agency City Coordinates]	= S.[CITY_COORDINATES],
                T.[Agency Cust Tax Number 4 Code]	= S.[TAX_NUM_4_CD],
                T.[Agency Cust Attribute 3]	= S.[ATTRIB_3],
                T.[Agency Cust Attribute 3 Description]	= S.[ATTRIB_3_DESC],
                T.[Agency Cust E-Mail Address]	= S.[E_MAIL_ADDR],
                T.[Agency Cust Mobile Number]	= S.[MOBILE_NUM],
                T.[Agency Cust Search Term 2]	= S.[SEARCH_TERM_2],
                T.[Agency Cust Street 2]	= S.[STR_2],
                T.[Agency Cust Street 3]	= S.[STR_3],
                T.[Agency Cust Street 4]	= S.[STR_4],
                T.[Agency Cust Street 5]	= S.[STR_5],
                T.[Agency Cust PO Box]	= S.[PO_BOX],
                T.[Agency Cust PO Box Postal Code]	= S.[PO_BOX_POSTAL_CD],
                T.[Agency Cust PO Box City]	= S.[PO_BOX_CITY],
                T.[Agency Cust Region For PO Box]	= S.[REGN_FOR_PO_BOX],
                T.[Agency Cust PO Box Country]	= S.[PO_BOX_CNTRY],
                T.[Agency Cust Transport Zone]	= S.[TRNSPRT_ZONE],
                T.[Agency Cust Address Notes]	= S.[ADDR_NOTES],
                T.[TAX_NUM_3]	= S.[TAX_NUM_3],
                T.[TAX_NUM_5]	= S.[TAX_NUM_5],
                T.[Agency Cust Industry Code 3]	= S.[INDUS_CD_3],
                T.[Agency Cust Industry Code 3 Description]	= S.[INDUS_CD_3_DESC],
                T.[Agency Cust Industry Code 4]	= S.[INDUS_CD_4],
                T.[Agency Cust Industry Code 4 Description]	= S.[INDUS_CD_4_DESC],
                T.[Agency Cust Industry Code 5]	= S.[INDUS_CD_5],
                T.[Agency Cust Industry Code 5 Description]	= S.[INDUS_CD_5_DESC],
                T.[Agency Cust Central Order Block For Customer]	= S.[CENTRAL_ORDER_BLK_FOR_CUST],
                T.[Agency Cust Payment Block]	= S.[PYMT_BLK]

    FROM [GLOBAL_EDW].[Propelis].[AGENCY_CUST] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTOMR_CUR_D] AS S
        ON T.[CUST_KEY] = S.[CUST_KEY];

    -- Step 2: Insert new records from mirror to target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[AGENCY_CUST] (
                 [CUST_KEY]
                ,[Agency Customer ID]
                ,[Agency Legacy Customer ID]
                ,[Agency Customer Name]
                ,[Agency Cust Line 2 Description]
                ,[Agency Cust Country Code]
                ,[Agency Cust Plant ID]
                ,[Agency Cust Segment ID]
                ,[Agency Cust Industry]
                ,[Agency Cust Vendor ID]
                ,[Agency Cust Customer Group Code]
                ,[Agency Cust Account Group Code]
                ,[Agency Cust Title]
                ,[Agency Cust Search Term]
                ,[Agency Cust Street]
                ,[Agency Cust City]
                ,[Agency Cust Zip Code]
                ,[Agency Cust State Code]
                ,[Agency Cust Region Code]
                ,[Agency Cust Telephone Number 1]
                ,[Agency Cust Telephone Number 2]
                ,[Agency Cust Fax Number Code]
                ,[Agency Cust Tax Number 1 Code]
                ,[Agency Cust Tax Number 2 Code]
                ,[Agency Cust VAT Number Code]
                ,[Agency Cust Credit Limit]
                ,[Agency Cust Bonus Rate]
                ,[Agency Cust Express Station]
                ,[TRANSTN_ZONE]
                ,[Agency Cust Printer Location Characteristics]
                ,[Agency Cust Printer Region Characteristics]
                ,[Agency Cust Printer Site Characteristics]
                ,[CREATED_DATE]
                ,[Agency Cust Created User ID]
                ,[UPDTD_DATE]
                ,[Agency Cust Updated User ID]
                ,[PYMT_TRMS_CD]
                ,[Agency Cust Sales Distribution]
                ,[Agency Cust Order Block For Sales Area]
                ,[Agency Cust Delivery Block For Sales Area]
                ,[ETL_SRC_SYS_CD]
                ,[ETL_EFFECTV_BEGIN_DATE]
                ,[ETL_EFFECTV_END_DATE]
                ,[ETL_CURR_RCD_IND]
                ,[ETL_CREATED_TS]
                ,[ETL_UPDTD_TS]
                ,[Agency Cust Corporate Group]
                ,[Agency Cust Classification]
                ,[Agency Cust Classification Description]
                ,[Agency Cust Printer Location]
                ,[Agency Cust Printer Site]
                ,[Agency Cust Printer Region]
                ,[Agency Cust Specific Field Label 01]
                ,[Agency Cust Specific Field Label 02]
                ,[Agency Cust Specific Field Label 03]
                ,[Agency Cust Specific Field Label 04]
                ,[Agency Cust Specific Field Label 05]
                ,[Agency Cust Specific Field Label 06]
                ,[Agency Cust Specific Field Label 07]
                ,[Agency Cust Specific Field Label 08]
                ,[Agency Cust Specific Field Label 09]
                ,[Agency Cust Specific Field Label 10]
                ,[Agency Cust Specific Field Label 11]
                ,[Agency Cust Specific Field Label 12]
                ,[Agency Cust Specific Field Label 13]
                ,[Agency Cust Specific Field Label 14]
                ,[Agency Cust Specific Field Label 15]
                ,[Agency Cust Specific Field Label 16]
                ,[Agency Cust Specific Field Label 17]
                ,[Agency Cust Specific Field Label 18]
                ,[Agency Cust Specific Field Label 19]
                ,[Agency Cust Specific Field Label 20]
                ,[Agency Cust Condition Group 1]
                ,[Agency Cust Industry Code 1]
                ,[Agency Cust Industry Code 2]
                ,[Agency Cust Industry Description]
                ,[Agency Cust Industry Code 1 Description]
                ,[Agency Cust Industry Code 2 Description]
                ,[Agency Cust Assignment to Hierarchy]
                ,[Agency Cust Account Group Description]
                ,[Agency Cust Region Description]
                ,[CNTRY_DESC]
                ,[Agency Cust Transportation Zone Description]
                ,[Agency Cust Plant Description]
                ,[Agency Cust Condition Group Description]
                ,[Agency Cust Cental Billing Block]
                ,[Agency Cust Cental Billing Block Description]
                ,[Agency Cust Central Delivery Block]
                ,[Agency Cust Central Delivery Block Description]
                ,[Agency Cust Deletion Indicator]
                ,[Agency Cust Central Posting Block]
                ,[Agency Cust Tax Jurisdiction]
                ,[Agency Cust Central Sales Block]
                ,[Agency Cust Central Deletion Indicator]
                ,[Agency Cust Language Key]
                ,[Agency Cust Trading Partner]
                ,[Agency Cust Trading Partner Description]
                ,[Agency Cust Annual Sales]
                ,[Agency Cust Annual Sales Year]
                ,[Agency Cust Sales Currency]
                ,[Agency Cust Sales Currency Description]
                ,[Agency Cust One Time Account Group]
                ,[Agency Cust One Time Account Group Description]
                ,[Agency Cust One Time Indicator]
                ,[Agency Cust Industry Type]
                ,[Agency Cust Business Type]
                ,[Agency Cust DUNS Number]
                ,[Agency Cust DUNS Plus 4]
                ,[Agency City Coordinates]
                ,[Agency Cust Tax Number 4 Code]
                ,[Agency Cust Attribute 3]
                ,[Agency Cust Attribute 3 Description]
                ,[Agency Cust E-Mail Address]
                ,[Agency Cust Mobile Number]
                ,[Agency Cust Search Term 2]
                ,[Agency Cust Street 2]
                ,[Agency Cust Street 3]
                ,[Agency Cust Street 4]
                ,[Agency Cust Street 5]
                ,[Agency Cust PO Box]
                ,[Agency Cust PO Box Postal Code]
                ,[Agency Cust PO Box City]
                ,[Agency Cust Region For PO Box]
                ,[Agency Cust PO Box Country]
                ,[Agency Cust Transport Zone]
                ,[Agency Cust Address Notes]
                ,[TAX_NUM_3]
                ,[TAX_NUM_5]
                ,[Agency Cust Industry Code 3]
                ,[Agency Cust Industry Code 3 Description]
                ,[Agency Cust Industry Code 4]
                ,[Agency Cust Industry Code 4 Description]
                ,[Agency Cust Industry Code 5]
                ,[Agency Cust Industry Code 5 Description]
                ,[Agency Cust Central Order Block For Customer]
                ,[Agency Cust Payment Block]
    )
    SELECT
         S.[CUST_KEY],
         S.[CUST_ID],
         S.[LGCY_CUST_ID],
         S.[CUST_NAME],
         S.[CUST_LINE_2_DESC],
         S.[CNTRY_CD],
         S.[PLNT_ID],
         S.[SEG_ID],
         S.[INDUS],
         S.[VNDR_ID],
         S.[CUST_GROUP_CD],
         S.[CUST_ACCT_GROUP_CD],
         S.[TITLE],
         S.[SEARCH_TERM],
         S.[STR],
         S.[CITY_NAME],
         S.[ZIP_CD],
         S.[ST_CD],
         S.[REGN_CD],
         S.[TEL_NUM_1_CD],
         S.[TEL_NUM_2_CD],
         S.[FAX_NUM_CD],
         S.[TAX_NUM_1_CD],
         S.[TAX_NUM_2_CD],
         S.[VAT_NUM_CD],
         S.[CREDIT_LMT],
         S.[BONUS_RT],
         S.[EXPRESS_STATION],
         S.[TRANSTN_ZONE],
         S.[PRINTER_LOC_CHARS],
         S.[PRINTER_REGN_CHARS],
         S.[PRINTER_SITE_CHARS],
         S.[CREATED_DATE],
         S.[CREATED_USR_ID],
         S.[UPDTD_DATE],
         S.[UPDTD_USR_ID],
         S.[PYMT_TRMS_CD],
         S.[SALES_DISTR],
         S.[ORDER_BLK_FOR_SALES_AREA],
         S.[DLVRY_BLK_FOR_SALES_AREA],
         S.[ETL_SRC_SYS_CD],
         S.[ETL_EFFECTV_BEGIN_DATE],
         S.[ETL_EFFECTV_END_DATE],
         S.[ETL_CURR_RCD_IND],
         S.[ETL_CREATED_TS],
         S.[ETL_UPDTD_TS],
         S.[CORPORATE_GROUP],
         S.[CUST_CLSFTN],
         S.[CUST_CLSFTN_DESC],
         S.[PRINTER_LOC],
         S.[PRINTER_SITE],
         S.[PRINTER_REGN],
         S.[CUST_SPECIFIC_FLD_LBL_01],
         S.[CUST_SPECIFIC_FLD_LBL_02],
         S.[CUST_SPECIFIC_FLD_LBL_03],
         S.[CUST_SPECIFIC_FLD_LBL_04],
         S.[CUST_SPECIFIC_FLD_LBL_05],
         S.[CUST_SPECIFIC_FLD_LBL_06],
         S.[CUST_SPECIFIC_FLD_LBL_07],
         S.[CUST_SPECIFIC_FLD_LBL_08],
         S.[CUST_SPECIFIC_FLD_LBL_09],
         S.[CUST_SPECIFIC_FLD_LBL_10],
         S.[CUST_SPECIFIC_FLD_LBL_11],
         S.[CUST_SPECIFIC_FLD_LBL_12],
         S.[CUST_SPECIFIC_FLD_LBL_13],
         S.[CUST_SPECIFIC_FLD_LBL_14],
         S.[CUST_SPECIFIC_FLD_LBL_15],
         S.[CUST_SPECIFIC_FLD_LBL_16],
         S.[CUST_SPECIFIC_FLD_LBL_17],
         S.[CUST_SPECIFIC_FLD_LBL_18],
         S.[CUST_SPECIFIC_FLD_LBL_19],
         S.[CUST_SPECIFIC_FLD_LBL_20],
         S.[CUST_COND_GROUP_1],
         S.[INDUS_CD_1],
         S.[INDUS_CD_2],
         S.[INDUS_DESC],
         S.[INDUS_CD_1_DESC],
         S.[INDUS_CD_2_DESC],
         S.[ASSIGN_TO_HIERCHY],
         S.[CUST_ACCT_GROUP_DESC],
         S.[REGN_DESC],
         S.[CNTRY_DESC],
         S.[TRANSTN_ZONE_DESC],
         S.[PLNT_DESC],
         S.[CUST_COND_GROUP_DESC],
         S.[CENTRAL_BILLING_BLK],
         S.[CENTRAL_BILLING_BLK_DESC],
         S.[CENTRAL_DLVRY_BLK],
         S.[CENTRAL_DLVRY_BLK_DESC],
         S.[DELETION_IND],
         S.[CENTRAL_PSTNG_BLK],
         S.[TAX_JRSDCTN],
         S.[CENTRAL_SALES_BLK],
         S.[CENTRAL_DELETION_IND],
         S.[LANG_KEY],
         S.[TRADING_PRTNR],
         S.[TRADING_PRTNR_DESC],
         S.[ANNUAL_SALES],
         S.[ANNUAL_SALES_YR],
         S.[SALES_CRNCY],
         S.[SALES_CRNCY_DESC],
         S.[ONE_TM_CUST_ACCT_GROUP],
         S.[ONE_TM_CUST_ACCT_GROUP_DESC],
         S.[ONE_TM_CUST_IND],
         S.[INDUS_TYP],
         S.[BIZ_TYP],
         S.[DUNS_NUM],
         S.[DUNS_PLUS_4],
         S.[CITY_COORDINATES],
         S.[TAX_NUM_4_CD],
         S.[ATTRIB_3],
         S.[ATTRIB_3_DESC],
         S.[E_MAIL_ADDR],
         S.[MOBILE_NUM],
         S.[SEARCH_TERM_2],
         S.[STR_2],
         S.[STR_3],
         S.[STR_4],
         S.[STR_5],
         S.[PO_BOX],
         S.[PO_BOX_POSTAL_CD],
         S.[PO_BOX_CITY],
         S.[REGN_FOR_PO_BOX],
         S.[PO_BOX_CNTRY],
         S.[TRNSPRT_ZONE],
         S.[ADDR_NOTES],
         S.[TAX_NUM_3],
         S.[TAX_NUM_5],
         S.[INDUS_CD_3],
         S.[INDUS_CD_3_DESC],
         S.[INDUS_CD_4],
         S.[INDUS_CD_4_DESC],
         S.[INDUS_CD_5],
         S.[INDUS_CD_5_DESC],
         S.[CENTRAL_ORDER_BLK_FOR_CUST],
         S.[PYMT_BLK]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTOMR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[AGENCY_CUST] AS T
        ON T.[CUST_KEY] = S.[CUST_KEY]
    WHERE T.[CUST_KEY] IS NULL;

END
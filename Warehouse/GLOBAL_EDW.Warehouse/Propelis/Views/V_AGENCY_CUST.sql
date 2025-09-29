-- Auto Generated (Do not modify) 400A808A172B7D799D04DE91E3063E1C38360DCD8A04D62208CBC52AD7BE3A19
CREATE   view [Propelis].[V_AGENCY_CUST] (
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
     AS 
    SELECT 
    [CUST_KEY],
        [Customer ID],
        [Legacy Customer ID],
        [Customer Name],
        [Customer Line 2 Description],
        [Customer Country Code],
        [Customer Plant ID],
        [Customer Segment ID],
        [Customer Industry],
        [Customer Vendor ID],
        [Customer Group Code],
        [Customer Account Group Code],
        [Customer Title],
        [Customer Search Term],
        [Customer Street],
        [Customer City],
        [Customer Zip Code],
        [Customer State Code],
        [Customer Region Code],
        [Customer Telephone Number 1],
        [Customer Telephone Number 2],
        [Customer Fax Number Code],
        [Customer Tax Number 1 Code],
        [Customer Tax Number 2 Code],
        [Customer VAT Number Code],
        [Customer Credit Limit],
        [Customer Bonus Rate],
        [Customer Express Station],
        [Customer Transportation Zone],
        [Customer Printer Location Characteristics],
        [Customer Printer Region Characteristics],
        [Customer Printer Site Characteristics],
        [Customer Created Date],
        [Customer Created User ID],
        [Customer Updated Date],
        [Customer Updated User ID],
        [Customer Payment Terms Code],
        [Customer Sales Distribution],
        [Customer Order Block For Sales Area],
        [Customer Delivery Block For Sales Area],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Customer Corporate Group],
        [Customer Classification],
        [Customer Classification Description],
        [Customer Printer Location],
        [Customer Printer Site],
        [Customer Printer Region],
        [Customer Specific Field Label 01],
        [Customer Specific Field Label 02],
        [Customer Specific Field Label 03],
        [Customer Specific Field Label 04],
        [Customer Specific Field Label 05],
        [Customer Specific Field Label 06],
        [Customer Specific Field Label 07],
        [Customer Specific Field Label 08],
        [Customer Specific Field Label 09],
        [Customer Specific Field Label 10],
        [Customer Specific Field Label 11],
        [Customer Specific Field Label 12],
        [Customer Specific Field Label 13],
        [Customer Specific Field Label 14],
        [Customer Specific Field Label 15],
        [Customer Specific Field Label 16],
        [Customer Specific Field Label 17],
        [Customer Specific Field Label 18],
        [Customer Specific Field Label 19],
        [Customer Specific Field Label 20],
        [Customer Condition Group 1],
        [Customer Industry Code 1],
        [Customer Industry Code 2],
        [Customer Industry Description],
        [Customer Industry Code 1 Description],
        [Customer Industry Code 2 Description],
        [Customer Assignment to Hierarchy],
        [Customer Account Group Description],
        [Customer Region Description],
        [Customer Country Description],
        [Customer Transportation Zone Description],
        [Customer Plant Description],
        [Customer Condition Group Description],
        [Customer Cental Billing Block],
        [Customer Cental Billing Block Description],
        [Customer Central Delivery Block],
        [Customer Central Delivery Block Description],
        [Customer Deletion Indicator],
        [Customer Central Posting Block],
        [Customer Tax Jurisdiction],
        [Customer Central Sales Block],
        [Customer Central Deletion Indicator],
        [Customer Language Key],
        [Customer Trading Partner],
        [Customer Trading Partner Description],
        [Customer Annual Sales],
        [Customer Annual Sales Year],
        [Customer Sales Currency],
        [Customer Sales Currency Description],
        [Customer One Time Account Group],
        [Customer One Time Account Group Description],
        [Customer One Time Indicator],
        [Customer Industry Type],
        [Customer Business Type],
        [Customer DUNS Number],
        [Customer DUNS Plus 4],
        [Customer City Coordinates],
        [Customer Tax Number 4 Code],
        [Customer Attribute 3],
        [Customer Attribute 3 Description],
        [Customer E-Mail Address],
        [Customer Mobile Number],
        [Customer Search Term 2],
        [Customer Street 2],
        [Customer Street 3],
        [Customer Street 4],
        [Customer Street 5],
        [Customer PO Box],
        [Customer PO Box Postal Code],
        [Customer PO Box City],
        [Customer Region For PO Box],
        [Customer PO Box Country],
        [Customer Transport Zone],
        [Customer Address Notes],
        [Customer Tax Number 3 Code],
        [Customer Tax Number 5 Code],
        [Customer Industry Code 3],
        [Customer Industry Code 3 Description],
        [Customer Industry Code 4],
        [Customer Industry Code 4 Description],
        [Customer Industry Code 5],
        [Customer Industry Code 5 Description],
        [Customer Central Order Block For Customer],
        [Customer Payment Block]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D];
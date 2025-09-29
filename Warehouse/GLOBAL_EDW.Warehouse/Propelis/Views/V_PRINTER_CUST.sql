-- Auto Generated (Do not modify) 8EB934324FF45817E3DCB62C37F57F6C01DECA57001889E5A1864F0B96DFBBF8
CREATE   view [Propelis].[V_PRINTER_CUST] (
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
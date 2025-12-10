CREATE       PROCEDURE [Propelis].[Proc_PARTNER_COMPANY]
AS
BEGIN
    UPDATE T
    SET
        T.[CMPNY_KEY]	                        = S.[CMPNY_KEY],
        T.[Partner Company]	                    = S.[CMPNY_CD],
        T.[Partner Legacy Company Code]	        = S.[LGCY_CMPNY_CD],
        T.[Partner Company Name]	            = S.[CMPNY_NAME],
        T.[Partner Company Line Of Business Code]	       = S.[LINE_OF_BIZ_CD],
        T.[Partner Company Line Of Business Code Description]	       = S.[LINE_OF_BIZ_CD_DESC],
        T.[Partner Company Currency Code]	    = S.[CRNCY_CD],
        T.[Partner Company Language Type Code]	= S.[LANG_TYP_CD],
        T.[Partner Company Chart Of Accounts ID] = S.[CHART_OF_ACCTS_ID],
        T.[Partner Company Address Line 1]	       = S.[ENTTY_ADDR_LINE_1],
        T.[Partner Company Address Line 2]	       = S.[ENTTY_ADDR_LINE_2],
        T.[Partner Company City Name]	       = S.[ENTTY_CITY_NAME],
        T.[Partner Company Zip Code]	       = S.[ENTTY_ZIP_CD],
        T.[Partner Company Country Code]	       = S.[ENTTY_CNTRY_CD],
        T.[Partner Company Telephone 1]	       = S.[ENTTY_TELEPHONE1],
        T.[Partner Company Telephone 2]	       = S.[ENTTY_TELEPHONE2],
        T.[Partner Company Fax Number]	       = S.[FAX_NUM],
        T.[Partner Company Tax Jurisdiction]   = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD]	                   = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	       = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	       = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]	       = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	       = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	       = S.[ETL_UPDTD_TS],
        T.[Partner Company Fiscal Year Variant]	       = S.[FISCAL_YR_VARIANT],
        T.[Partner Company Credit Control Area]	       = S.[CREDIT_CTRL_AREA],
        T.[Partner Company Input Tax Cd]	       = S.[INPUT_TAX_CD],
        T.[Partner Company Output Tax Code]	       = S.[OTPUT_TAX_CD],
        T.[Partner Company Currency Description]	       = S.[CRNCY_DESC],
        T.[Partner Company Chart Of Accounts Description]	       = S.[CHART_OF_ACCTS_DESC],
        T.[Partner Company Country Description]	    = S.[CNTRY_DESC],
        T.[Partner Company Fiscal Year Variant Description]	       = S.[FISCAL_YR_VARIANT_DESC],
        T.[Partner Company Credit Control Area Description]	       = S.[CREDIT_CTRL_AREA_DESC]


 FROM [GLOBAL_EDW].[Propelis].[PARTNER_COMPANY] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[PARTNER_COMPANY] (
        [CMPNY_KEY],
        [Partner Company],
        [Partner Legacy Company Code],
        [Partner Company Name],
        [Partner Company Line Of Business Code],
        [Partner Company Line Of Business Code Description],
        [Partner Company Currency Code],
        [Partner Company Language Type Code],
        [Partner Company Chart Of Accounts ID],
        [Partner Company Address Line 1],
        [Partner Company Address Line 2],
        [Partner Company City Name],
        [Partner Company Zip Code],
        [Partner Company Country Code],
        [Partner Company Telephone 1],
        [Partner Company Telephone 2],
        [Partner Company Fax Number],
        [Partner Company Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Partner Company Fiscal Year Variant],
        [Partner Company Credit Control Area],
        [Partner Company Input Tax Cd],
        [Partner Company Output Tax Code],
        [Partner Company Currency Description],
        [Partner Company Chart Of Accounts Description],
        [Partner Company Country Description],
        [Partner Company Fiscal Year Variant Description],
        [Partner Company Credit Control Area Description]
		
)
    SELECT
       S.[CMPNY_KEY],
       S.[CMPNY_CD],
       S.[LGCY_CMPNY_CD],
       S.[CMPNY_NAME],
       S.[LINE_OF_BIZ_CD],
       S.[LINE_OF_BIZ_CD_DESC],
       S.[CRNCY_CD],
       S.[LANG_TYP_CD],
       S.[CHART_OF_ACCTS_ID],
       S.[ENTTY_ADDR_LINE_1],
       S.[ENTTY_ADDR_LINE_2],
       S.[ENTTY_CITY_NAME],
       S.[ENTTY_ZIP_CD],
       S.[ENTTY_CNTRY_CD],
       S.[ENTTY_TELEPHONE1],
       S.[ENTTY_TELEPHONE2],
       S.[FAX_NUM],
       S.[TAX_JRSDCTN],
       S.[ETL_SRC_SYS_CD],
       S.[ETL_EFFECTV_BEGIN_DATE],
       S.[ETL_EFFECTV_END_DATE],
       S.[ETL_CURR_RCD_IND],
       S.[ETL_CREATED_TS],
       S.[ETL_UPDTD_TS],
       S.[FISCAL_YR_VARIANT],
       S.[CREDIT_CTRL_AREA],
       S.[INPUT_TAX_CD],
       S.[OTPUT_TAX_CD],
       S.[CRNCY_DESC],
       S.[CHART_OF_ACCTS_DESC],
       S.[CNTRY_DESC],
       S.[FISCAL_YR_VARIANT_DESC],
       S.[CREDIT_CTRL_AREA_DESC]
	   
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_COMPANY] AS T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;

END
CREATE   PROCEDURE [Propelis].[Proc_COMPANY]
AS
BEGIN
    UPDATE T
    SET
        T.[CMPNY_KEY]	 = S.[CMPNY_KEY],
        T.[Company]	       = S.[CMPNY_CD],
        T.[Company Legacy Code]	       = S.[LGCY_CMPNY_CD],
        T.[Company Name]	       = S.[CMPNY_NAME],
        T.[Company Line Of Business Code]	       = S.[LINE_OF_BIZ_CD],
        T.[Company Line Of Business Code Description]	       = S.[LINE_OF_BIZ_CD_DESC],
        T.[Company Currency Code]	       = S.[CRNCY_CD],
        T.[Company Language Type Code]	       = S.[LANG_TYP_CD],
        T.[Company Chart Of Accounts ID]	       = S.[CHART_OF_ACCTS_ID],
        T.[Company Address Line 1]	       = S.[ENTTY_ADDR_LINE_1],
        T.[Company Address Line 2]	       = S.[ENTTY_ADDR_LINE_2],
        T.[Company City Name]	       = S.[ENTTY_CITY_NAME],
        T.[Company Zip Code]	       = S.[ENTTY_ZIP_CD],
        T.[Company Country Code]	       = S.[ENTTY_CNTRY_CD],
        T.[Company Telephone 1]	       = S.[ENTTY_TELEPHONE1],
        T.[Company Telephone 2]	       = S.[ENTTY_TELEPHONE2],
        T.[Company Fax Number]	       = S.[FAX_NUM],
        T.[Company Tax Jurisdiction]	       = S.[TAX_JRSDCTN],
        T.[ETL_SRC_SYS_CD]	       = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	       = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	       = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]	       = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	       = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	       = S.[ETL_UPDTD_TS],
        T.[Company Fiscal Year Variant]	       = S.[FISCAL_YR_VARIANT],
        T.[Company Credit Control Area]	       = S.[CREDIT_CTRL_AREA],
        T.[Company Input Tax Cd]	       = S.[INPUT_TAX_CD],
        T.[Company Output Tax Code]	       = S.[OTPUT_TAX_CD],
        T.[Company Currency Description]	       = S.[CRNCY_DESC],
        T.[Company Chart Of Accounts Description]	       = S.[CHART_OF_ACCTS_DESC],
        T.[Company Country Description]	       = S.[CNTRY_DESC],
        T.[Company Fiscal Year Variant Description]	       = S.[FISCAL_YR_VARIANT_DESC],
        T.[Company Credit Control Area Description]	       = S.[CREDIT_CTRL_AREA_DESC]

 FROM [GLOBAL_EDW].[Propelis].[COMPANY] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[COMPANY] (
        [CMPNY_KEY]
        ,[Company]
        ,[Company Legacy Code]
        ,[Company Name]
        ,[Company Line Of Business Code]
        ,[Company Line Of Business Code Description]
        ,[Company Currency Code]
        ,[Company Language Type Code]
        ,[Company Chart Of Accounts ID]
        ,[Company Address Line 1]
        ,[Company Address Line 2]
        ,[Company City Name]
        ,[Company Zip Code]
        ,[Company Country Code]
        ,[Company Telephone 1]
        ,[Company Telephone 2]
        ,[Company Fax Number]
        ,[Company Tax Jurisdiction]
        ,[ETL_SRC_SYS_CD]
        ,[ETL_EFFECTV_BEGIN_DATE]
        ,[ETL_EFFECTV_END_DATE]
        ,[ETL_CURR_RCD_IND]
        ,[ETL_CREATED_TS]
        ,[ETL_UPDTD_TS]
        ,[Company Fiscal Year Variant]
        ,[Company Credit Control Area]
        ,[Company Input Tax Cd]
        ,[Company Output Tax Code]
        ,[Company Currency Description]
        ,[Company Chart Of Accounts Description]
        ,[Company Country Description]
        ,[Company Fiscal Year Variant Description]
        ,[Company Credit Control Area Description]
		
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
	   
	FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY] AS T
        ON T.[CMPNY_KEY] = S.[CMPNY_KEY]
    WHERE T.[CMPNY_KEY] IS NULL;
END
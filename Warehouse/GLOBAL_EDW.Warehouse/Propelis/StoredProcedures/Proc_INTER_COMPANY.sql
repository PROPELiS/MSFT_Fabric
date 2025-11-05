CREATE PROCEDURE [Propelis].[Proc_INTER_COMPANY] 
AS
BEGIN

    -- Update existing records
    UPDATE T
    SET  
			T.[CMPNY_KEY] = S.[CMPNY_KEY],
			T.[Inter Company Code] = S.[Company Code] ,
			T.[Inter Company Legacy Code] = S.[Legacy Company Code] ,
			T.[Inter Company Name] = S.[Company Name] ,
			T.[Inter Company Line Of Business Code] = S.[Company Line Of Business],
			T.[Inter Company Line Of Business Description] = S.[Company Line Of Business Description] ,
			T.[Inter Company Currency Code] = S.[Company Currency Code] ,
			T.[Inter Company Language Type Code] = S.[Company Language Type Code] ,
			T.[Inter Company Chart Of Accounts ID] = S.[Company Chart Of Accounts ID],
			T.[Inter Company Address Line 1] = S.[Company Address Line 1] ,
			T.[Inter Company Address Line 2] = S.[Company Address Line 2],
			T.[Inter Company City Name] = S.[Company City Name] ,
			T.[Inter Company Zip Code] = S.[Company Zip Code],
			T.[Inter Company Country Code] = S.[Company Country Code] ,
			T.[Company Telephone 1] = S.[Company Telephone 1] ,
			T.[Company Telephone 2] = S.[Company Telephone 2] ,
			T.[Inter Company Fax Number] = S.[Company Fax Number] ,
			T.[Inter Company Tax Jurisdiction] = S.[Company Tax Jurisdiction] ,
			T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD] ,
			T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE] ,
			T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE] ,
			T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
			T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS] ,
			T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS] ,
			T.[Inter Company Fiscal Year Variant] = S.[Company Fiscal Year Variant] ,
			T.[Inter Company Credit Control Area] = S.[Company Credit Control Area] ,
			T.[Inter Company Input Tax Code] = S.[Company Input Tax Code],
			T.[Inter Company Output Tax Code] = S.[Company Output Tax Code] ,
			T.[Inter Company Currency Description] = S.[Company Currency Description] ,
			T.[Inter Company Chart Of Accounts Description] = S.[Company Chart Of Accounts Description],
			T.[Inter Company Country Description] = S.[Company Country Description] ,
			T.[Inter Company Fiscal Year Variant Description] = S.[Company Fiscal Year Variant Description],
			T.[Inter Company Credit Control Area Description] = S.[Company Credit Control Area Description]

	FROM 
        [GLOBAL_EDW].[Propelis].[INTER_COMPANY] T
    INNER JOIN 
	[GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] S
	ON T.[CMPNY_KEY] =  S.[CMPNY_KEY]
	
	
    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMPANY]
    (
		[CMPNY_KEY],
		[Inter Company Code],
		[Inter Company Legacy Code],
		[Inter Company Name],
		[Inter Company Line Of Business Code],
		[Inter Company Line Of Business Description],
		[Inter Company Currency Code],
		[Inter Company Language Type Code],
		[Inter Company Chart Of Accounts ID],
		[Inter Company Address Line 1],
		[Inter Company Address Line 2],
		[Inter Company City Name],
		[Inter Company Zip Code],
		[Inter Company Country Code],
		[Company Telephone 1],
		[Company Telephone 2],
		[Inter Company Fax Number],
		[Inter Company Tax Jurisdiction],
		[ETL_SRC_SYS_CD],
		[ETL_EFFECTV_BEGIN_DATE],
		[ETL_EFFECTV_END_DATE],
		[ETL_CURR_RCD_IND],
		[ETL_CREATED_TS],
		[ETL_UPDTD_TS],
		[Inter Company Fiscal Year Variant],
		[Inter Company Credit Control Area],
		[Inter Company Input Tax Code],
		[Inter Company Output Tax Code],
		[Inter Company Currency Description],
		[Inter Company Chart Of Accounts Description],
		[Inter Company Country Description],
		[Inter Company Fiscal Year Variant Description],
		[Inter Company Credit Control Area Description]
)
SELECT 
		S.[CMPNY_KEY],
		S.[Company Code] ,
		S.[Legacy Company Code] ,
		S.[Company Name] ,
		S.[Company Line Of Business],
		S.[Company Line Of Business Description] ,
		S.[Company Currency Code] ,
		S.[Company Language Type Code] ,
		S.[Company Chart Of Accounts ID],
		S.[Company Address Line 1] ,
		S.[Company Address Line 2],
		S.[Company City Name] ,
		S.[Company Zip Code],
		S.[Company Country Code] ,
		S.[Company Telephone 1] ,
		S.[Company Telephone 2] ,
		S.[Company Fax Number] ,
		S.[Company Tax Jurisdiction] ,
		S.[ETL_SRC_SYS_CD] ,
		S.[ETL_EFFECTV_BEGIN_DATE] ,
		S.[ETL_EFFECTV_END_DATE] ,
		S.[ETL_CURR_RCD_IND],
		S.[ETL_CREATED_TS] ,
		S.[ETL_UPDTD_TS] ,
		S.[Company Fiscal Year Variant] ,
		S.[Company Credit Control Area] ,
		S.[Company Input Tax Code],
		S.[Company Output Tax Code] ,
		S.[Company Currency Description] ,
		S.[Company Chart Of Accounts Description],
		S.[Company Country Description] ,
		S.[Company Fiscal Year Variant Description],
		S.[Company Credit Control Area Description]
		
		  FROM 
        [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] S
    LEFT JOIN 
       [GLOBAL_EDW].[Propelis].[INTER_COMPANY] T 
        ON T.[CMPNY_KEY] =  S.[CMPNY_KEY]
    WHERE 
        T.[CMPNY_KEY] IS NULL;

END;
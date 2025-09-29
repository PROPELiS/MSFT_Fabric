-- Auto Generated (Do not modify) D6947C447ECD81F6F0E35430B8F60E9A7AE67B16BA60B36FCD8986AE3375FD69
CREATE   VIEW [Propelis].[V_COST_CENTER](
    [COST_CNTR_KEY] 
    ,[Cost Center ID]
    ,[Cost Center Controlling Area ID]
    ,[Cost Center Language Type Code]
    ,[Cost Center Category]
    ,[Cost Center Hierarchy Area]
    ,[Cost Center Company Code]
    ,[Cost Center Plant]
    ,[Cost Center Profit Center ID]
    ,[Cost Center Currency Code]
    ,[Cost Center Address Line 1]
    ,[Cost Center Address Line 2]
    ,[Cost Center Address Line 3]
    ,[Cost Center City Name]
    ,[Cost Center District Name]
    ,[Cost Center Postal Code]
    ,[Cost Center Country]
    ,[Cost Center Region Code]
    ,[Cost Center Person Responsible]
    ,[Cost Center Flag Actual Revenues]
    ,[ETL_SRC_SYS_CD]
    ,[ETL_EFFECTV_BEGIN_DATE]
    ,[ETL_EFFECTV_END_DATE]
    ,[ETL_CURR_RCD_IND]
    ,[ETL_CREATED_TS]
    ,[ETL_UPDTD_TS]
    ,[Cost Center Description]
    ,[Cost Center Actual Primary Postings Lock Indicator]
    ,[Cost Center Plan Primary Costs Lock Indicator]
    ,[Cost Center Business Area]
    ,[Cost Center Planning Revenues Lock Indicator]
    ,[Cost Center PO Box]
    ,[Cost Center Street]
    ,[Cost Center User Responsible]
    ,[Cost Center Short Description]
    ,[Cost Center Controlling Area Description]
    ,[Cost Center Category Description]
    ,[Cost Center Company Description]
    ,[Cost Center Currency Description]
    ,[Cost Center Country Description]
    ,[Cost Center Business Area Description]
    ,[Cost Center Actual Secondary Cost Lock Indicator]
    ,[Cost Center Plan Secondary Cost Lock Indicator]
    ,[Cost Center Functional Area]
    ,[Cost Center Functional Area Description]
    ,[Cost Center Tax Jurisdiction]
    ,[Cost Center Function]
    ,[Cost Center Plant Description]
    ,[Cost Center Created On]
    ,[Cost Center Long Description]
    ,[Cost Center Valid To Date]
    ,[Cost Center Valid From Date]
)AS
SELECT
    [COST_CNTR_KEY],
    [Cost Center ID],
    [Cost Center Long Description],
    [Cost Center Controlling Area ID],
    [Cost Center Language Type Code],
    [Cost Center Category],
    [Cost Center Hierarchy Area],
    [Cost Center Company Code],
    [Cost Center Plant ID],
    [Cost Center Profit Center ID],
    [Cost Center Currency Code],
    [Cost Center Address Line 1],
    [Cost Center Address Line 2],
    [Cost Center Address Line 3],
    [Cost Center City Name],
    [Cost Center District Name],
    [Cost Center Postal Code],
    [Cost Center Country Code],
    [Cost Center Region Code],
    [Cost Center Person Responsible],
    [Cost Center Flag Actual Revenues],
    [ETL_SRC_SYS_CD],
    [ETL_EFFECTV_BEGIN_DATE],
    [ETL_EFFECTV_END_DATE],
    [ETL_CURR_RCD_IND],
    [ETL_CREATED_TS],
    [ETL_UPDTD_TS],
    [Cost Center Actual Primary Postings Lock Indicator],
    [Cost Center Plan Primary Costs Lock Indicator],
    [Cost Center Business Area],
    [Cost Center Planning Revenues Lock Indicator],
    [Cost Center PO Box],
    [Cost Center Street],
    [Cost Center User Responsible],
    [COST_CNTR_NAME],
    [Cost Center Short Description],
    [Cost Center Controlling Area Description],
    [Cost Center Category Description],
    [Cost Center Company Description],
    [Cost Center Currency Description],
    [Cost Center Country Description],
    [Cost Center Business Area Description],
    [Cost Center Actual Secondary Cost Lock Indicator],
    [Cost Center Plan Secondary Cost Lock Indicator],
    [Cost Center Functional Area],
    [Cost Center Functional Area Description],
    [Cost Center Tax Jurisdiction],
    [FUNCTION_OF_COST_CNTR],
    [Cost Center Plant Description],
    [Cost Center Created On],
    [Cost Center Valid To Date],
    [Cost Center Valid From Date]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D];
CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_FI_ACCTRCV2_CUR_D]
AS
BEGIN

UPDATE T 
SET 
T.[AR Dunning Level] = S.[AR Dunning Level],
T.[DUNNING_KEY] = S.[DUNNING_KEY],
T.[AR Dunning Key Description] = S.[AR Dunning Key Description],
T.[AR Expected Customer Sales Organization Description] = S.[AR Expected Customer Sales Organization Description],
T.[AR Expected Sales Organization] = S.[AR Expected Sales Organization],
T.[AR Expected Sales Organization Description] = S.[AR Expected Sales Organization Description],
T.[AR Dunning Block] = S.[AR Dunning Block],
T.[AR Dunning Block Description] = S.[AR Dunning Block Description],
T.[LAST_DUNNING_NOTICE_DATE_KEY] = S.[LAST_DUNNING_NOTICE_DATE_KEY],
T.[AR Controlling Area Description] = S.[AR Controlling Area Description],
T.[AR Ledger Code Description] = S.[AR Ledger Code Description],
T.[AR Item Text] = S.[AR Item Text],
T.[AR Deletion Flag] = S.[AR Deletion Flag],
T.[AR Debit Credit Indicator] = S.[AR Debit Credit Indicator],
T.[AR Expected Customer Sales Organization] = S.[AR Expected Customer Sales Organization],
T.[CUST_SALES_AREA_KEY] = S.[CUST_SALES_AREA_KEY],
T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY],
T.[AR Document Type Description] = S.[AR Document Type Description],
T.[AR Special GL Indicator Description] = S.[AR Special GL Indicator Description],
T.[AR Posting Key Description] = S.[AR Posting Key Description],
T.[AR Payment Terms Description] = S.[AR Payment Terms Description],
T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
T.[AR Controlling Area] = S.[AR Controlling Area],
T.[WBS_ELEMNT_KEY] = S.[WBS_ELEMNT_KEY],
T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY],
T.[AR Days 2] = S.[AR Days 2],
T.[AR Days Net] = S.[AR Days Net],
T.[AR Reference Key for Line Item] = S.[AR Reference Key for Line Item],
T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY],
T.[SEG_KEY] = S.[SEG_KEY],
T.[BILLING_DOCMT_KEY] = S.[BILLING_DOCMT_KEY],
T.[PSTNG_KEY] = S.[PSTNG_KEY],
T.[DEBIT_CREDIT_IND] = S.[DEBIT_CREDIT_IND],
T.[GL_KEY] = S.[GL_KEY],
T.[BSLINE_DUE_DATE_KEY] = S.[BSLINE_DUE_DATE_KEY],
T.[AR Payment Terms] = S.[AR Payment Terms],
T.[AR Days 1] = S.[AR Days 1],
T.[AR Invoice Reference Number] = S.[AR Invoice Reference Number],
T.[AR Reason Code] = S.[AR Reason Code],
T.[AR Reason Code Description] = S.[AR Reason Code Description],
T.[Company Currency Amount] = S.[Company Currency Amount],
T.[Document Currency Amount] = S.[Document Currency Amount],
T.[Group Currency Amount] = S.[Group Currency Amount],
T.[DOCMT_CRNCY_KEY] = S.[DOCMT_CRNCY_KEY],
T.[CMPNY_CRNCY_KEY] = S.[CMPNY_CRNCY_KEY],
T.[GROUP_CRNCY_KEY] = S.[GROUP_CRNCY_KEY],
T.[AR Posting Period] = S.[AR Posting Period],
T.[AR Document Type] = S.[AR Document Type],
T.[AR Reference Number] = S.[AR Reference Number],
T.[CLEARING_DATE_KEY] = S.[CLEARING_DATE_KEY],
T.[AR Assignment Number] = S.[AR Assignment Number],
T.[AR Special GL Transaction Type] = S.[AR Special GL Transaction Type],
T.[AR Special GL Indicator] = S.[AR Special GL Indicator],
T.[DOCMT_DATE_KEY] = S.[DOCMT_DATE_KEY],
T.[PSTNG_DATE_KEY] = S.[PSTNG_DATE_KEY],
T.[Baseline Due Fiscal Year] = S.[Baseline Due Fiscal Year],
T.[AR Ledger Code] = S.[AR Ledger Code],
T.[AR Ledger Posting Item Number] = S.[AR Ledger Posting Item Number],
T.[PAYER_CUST_KEY] = S.[PAYER_CUST_KEY],
T.[AR Document Line Item Number] = S.[AR Document Line Item Number],
T.[AR Clearing Document Number] = S.[AR Clearing Document Number],
T.[CMPNY_KEY] = S.[CMPNY_KEY],
T.[AR Document Header Number] = S.[AR Document Header Number]


FROM [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_ACCTRCV2_CUR_D] T 
INNER JOIN [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_ACCTRCV2_CUR_D]S
ON T.[DOCMT_DATE_KEY] = S.[DOCMT_DATE_KEY]
AND T.[PSTNG_DATE_KEY] = S.[PSTNG_DATE_KEY]
AND T.[CMPNY_KEY] = S.[CMPNY_KEY]
AND T.[PAYER_CUST_KEY] = S.[PAYER_CUST_KEY]
AND T.[DOCMT_CRNCY_KEY] = S.[DOCMT_CRNCY_KEY]
AND T.[GROUP_CRNCY_KEY] = S.[GROUP_CRNCY_KEY]
AND T.[CMPNY_CRNCY_KEY] = S.[CMPNY_CRNCY_KEY]
AND T.[CLEARING_DATE_KEY] = S.[CLEARING_DATE_KEY]
AND T.[BSLINE_DUE_DATE_KEY] = S.[BSLINE_DUE_DATE_KEY]
AND T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
AND T.[BILLING_DOCMT_KEY] = S.[BILLING_DOCMT_KEY]
AND T.[WBS_ELEMNT_KEY] = S.[WBS_ELEMNT_KEY]
AND T.[GL_KEY] = S.[GL_KEY]
AND T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY]
AND T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY]
AND T.[CUST_SALES_AREA_KEY] = S.[CUST_SALES_AREA_KEY]
AND T.[SEG_KEY] = S.[SEG_KEY]
AND T.[LAST_DUNNING_NOTICE_DATE_KEY] = S.[LAST_DUNNING_NOTICE_DATE_KEY];


INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_ACCTRCV2_CUR_D]
(
[AR Dunning Level],
[DUNNING_KEY],
[AR Dunning Key Description],
[AR Expected Customer Sales Organization Description],
[AR Expected Sales Organization],
[AR Expected Sales Organization Description],
[AR Dunning Block],
[AR Dunning Block Description],
[LAST_DUNNING_NOTICE_DATE_KEY],
[AR Controlling Area Description],
[AR Ledger Code Description],
[AR Item Text],
[AR Deletion Flag],
[AR Debit Credit Indicator],
[AR Expected Customer Sales Organization],
[CUST_SALES_AREA_KEY],
[CUST_CREDIT_KEY],
[AR Document Type Description],
[AR Special GL Indicator Description],
[AR Posting Key Description],
[AR Payment Terms Description],
[ETL_SRC_SYS_CD],
[ETL_CREATED_TS],
[ETL_UPDTD_TS],
[AR Controlling Area],
[WBS_ELEMNT_KEY],
[SALES_AREA_KEY],
[AR Days 2],
[AR Days Net],
[AR Reference Key for Line Item],
[PROFT_CNTR_KEY],
[SEG_KEY],
[BILLING_DOCMT_KEY],
[PSTNG_KEY],
[DEBIT_CREDIT_IND],
[GL_KEY],
[BSLINE_DUE_DATE_KEY],
[AR Payment Terms],
[AR Days 1],
[AR Invoice Reference Number],
[AR Reason Code],
[AR Reason Code Description],
[Company Currency Amount],
[Document Currency Amount],
[Group Currency Amount],
[DOCMT_CRNCY_KEY],
[CMPNY_CRNCY_KEY],
[GROUP_CRNCY_KEY],
[AR Posting Period],
[AR Document Type],
[AR Reference Number],
[CLEARING_DATE_KEY],
[AR Assignment Number],
[AR Special GL Transaction Type],
[AR Special GL Indicator],
[DOCMT_DATE_KEY],
[PSTNG_DATE_KEY],
[Baseline Due Fiscal Year],
[AR Ledger Code],
[AR Ledger Posting Item Number],
[PAYER_CUST_KEY],
[AR Document Line Item Number],
[AR Clearing Document Number],
[CMPNY_KEY],
[AR Document Header Number]
)

SELECT 

S.[AR Dunning Level],
S.[DUNNING_KEY],
S.[AR Dunning Key Description],
S.[AR Expected Customer Sales Organization Description],
S.[AR Expected Sales Organization],
S.[AR Expected Sales Organization Description],
S.[AR Dunning Block],
S.[AR Dunning Block Description],
S.[LAST_DUNNING_NOTICE_DATE_KEY],
S.[AR Controlling Area Description],
S.[AR Ledger Code Description],
S.[AR Item Text],
S.[AR Deletion Flag],
S.[AR Debit Credit Indicator],
S.[AR Expected Customer Sales Organization],
S.[CUST_SALES_AREA_KEY],
S.[CUST_CREDIT_KEY],
S.[AR Document Type Description],
S.[AR Special GL Indicator Description],
S.[AR Posting Key Description],
S.[AR Payment Terms Description],
S.[ETL_SRC_SYS_CD],
S.[ETL_CREATED_TS],
S.[ETL_UPDTD_TS],
S.[AR Controlling Area],
S.[WBS_ELEMNT_KEY],
S.[SALES_AREA_KEY],
S.[AR Days 2],
S.[AR Days Net],
S.[AR Reference Key for Line Item],
S.[PROFT_CNTR_KEY],
S.[SEG_KEY],
S.[BILLING_DOCMT_KEY],
S.[PSTNG_KEY],
S.[DEBIT_CREDIT_IND],
S.[GL_KEY],
S.[BSLINE_DUE_DATE_KEY],
S.[AR Payment Terms],
S.[AR Days 1],
S.[AR Invoice Reference Number],
S.[AR Reason Code],
S.[AR Reason Code Description],
S.[Company Currency Amount],
S.[Document Currency Amount],
S.[Group Currency Amount],
S.[DOCMT_CRNCY_KEY],
S.[CMPNY_CRNCY_KEY],
S.[GROUP_CRNCY_KEY],
S.[AR Posting Period],
S.[AR Document Type],
S.[AR Reference Number],
S.[CLEARING_DATE_KEY],
S.[AR Assignment Number],
S.[AR Special GL Transaction Type],
S.[AR Special GL Indicator],
S.[DOCMT_DATE_KEY],
S.[PSTNG_DATE_KEY],
S.[Baseline Due Fiscal Year],
S.[AR Ledger Code],
S.[AR Ledger Posting Item Number],
S.[PAYER_CUST_KEY],
S.[AR Document Line Item Number],
S.[AR Clearing Document Number],
S.[CMPNY_KEY],
S.[AR Document Header Number]

FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_ACCTRCV2_CUR_D] S 
LEFT JOIN [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_ACCTRCV2_CUR_D] T 
ON T.[DOCMT_DATE_KEY] = S.[DOCMT_DATE_KEY]
AND T.[PSTNG_DATE_KEY] = S.[PSTNG_DATE_KEY]
AND T.[CMPNY_KEY] = S.[CMPNY_KEY]
AND T.[PAYER_CUST_KEY] = S.[PAYER_CUST_KEY]
AND T.[DOCMT_CRNCY_KEY] = S.[DOCMT_CRNCY_KEY]
AND T.[GROUP_CRNCY_KEY] = S.[GROUP_CRNCY_KEY]
AND T.[CMPNY_CRNCY_KEY] = S.[CMPNY_CRNCY_KEY]
AND T.[CLEARING_DATE_KEY] = S.[CLEARING_DATE_KEY]
AND T.[BSLINE_DUE_DATE_KEY] = S.[BSLINE_DUE_DATE_KEY]
AND T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
AND T.[BILLING_DOCMT_KEY] = S.[BILLING_DOCMT_KEY]
AND T.[WBS_ELEMNT_KEY] = S.[WBS_ELEMNT_KEY]
AND T.[GL_KEY] = S.[GL_KEY]
AND T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY]
AND T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY]
AND T.[CUST_SALES_AREA_KEY] = S.[CUST_SALES_AREA_KEY]
AND T.[SEG_KEY] = S.[SEG_KEY]
AND T.[LAST_DUNNING_NOTICE_DATE_KEY] = S.[LAST_DUNNING_NOTICE_DATE_KEY];
END;
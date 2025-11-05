CREATE PROCEDURE [Propelis].[Proc_TEMP_EDW_T_FACT_FI_ACCTRCV2_CUR_D]
AS
BEGIN

DELETE FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_ACCTRCV2_CUR_D];


INSERT INTO [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_ACCTRCV2_CUR_D]
(
[CMPNY_KEY],
[AR Document Header Number],
[Baseline Due Fiscal Year],
[AR Ledger Code],
[AR Ledger Posting Item Number],
[PAYER_CUST_KEY],
[AR Document Line Item Number],
[AR Clearing Document Number],
[CLEARING_DATE_KEY],
[AR Assignment Number],
[AR Special GL Transaction Type],
[AR Special GL Indicator],
[DOCMT_DATE_KEY],
[PSTNG_DATE_KEY],
[DOCMT_CRNCY_KEY],
[CMPNY_CRNCY_KEY],
[GROUP_CRNCY_KEY],
[AR Posting Period],
[AR Document Type],
[AR Reference Number],
[AR Invoice Reference Number],
[AR Reason Code],
[AR Reason Code Description],
[Company Currency Amount],
[Document Currency Amount],
[Group Currency Amount],
[PSTNG_KEY],
[DEBIT_CREDIT_IND],
[GL_KEY],
[BSLINE_DUE_DATE_KEY],
[AR Payment Terms],
[AR Days 1],
[AR Days 2],
[AR Days Net],
[AR Reference Key for Line Item],
[PROFT_CNTR_KEY],
[SEG_KEY],
[BILLING_DOCMT_KEY],
[ETL_SRC_SYS_CD],
[ETL_CREATED_TS],
[ETL_UPDTD_TS],
[AR Controlling Area],
[WBS_ELEMNT_KEY],
[SALES_AREA_KEY],
[CUST_SALES_AREA_KEY],
[CUST_CREDIT_KEY],
[AR Document Type Description],
[AR Special GL Indicator Description],
[AR Posting Key Description],
[AR Payment Terms Description],
[AR Controlling Area Description],
[AR Ledger Code Description],
[AR Item Text],
[AR Deletion Flag],
[AR Debit Credit Indicator],
[AR Expected Customer Sales Organization],
[AR Expected Customer Sales Organization Description],
[AR Expected Sales Organization],
[AR Expected Sales Organization Description],
[AR Dunning Block],
[AR Dunning Block Description],
[LAST_DUNNING_NOTICE_DATE_KEY],
[AR Dunning Level],
[DUNNING_KEY],
[AR Dunning Key Description]
)

SELECT 
Fact.[CMPNY_KEY],
Fact.[DOCMT_HDR_NUM],
Fact.[FISCAL_YR],
Fact.[LDGR_CD],
Fact.[LDGR_PSTNG_ITM_NUM],
Fact.[PAYER_CUST_KEY],
Fact.[DOCMT_LINE_ITM_NUM],
Fact.[CLEARING_DOCMT_NUM],
Fact.[CLEARING_DATE_KEY],
Fact.[ASSIGN_NUM],
Fact.[SPCL_GL_TRNSCTN_TYP],
Fact.[SPCL_GL_IND],
Fact.[DOCMT_DATE_KEY],
Fact.[PSTNG_DATE_KEY],
Fact.[DOCMT_CRNCY_KEY],
Fact.[CMPNY_CRNCY_KEY],
Fact.[GROUP_CRNCY_KEY],
Fact.[PSTNG_PER],
Fact.[DOCMT_TYP],
Fact.[REF_NUM],
Fact.[INVC_REF_NUM],
Fact.[RSN_CD],
Fact.[RSN_CD_DESC],
Fact.[CMPNY_CRNCY_AMT],
Fact.[DOCMT_CRNCY_AMT],
Fact.[GROUP_CRNCY_AMT],
Fact.[PSTNG_KEY],
Fact.[DEBIT_CREDIT_IND],
Fact.[GL_KEY],
Fact.[BSLINE_DUE_DATE_KEY],
Fact.[PYMT_TRMS],
Fact.[CASH_DISCOUNTS_DAY_1],
Fact.[CASH_DISCOUNTS_DAY_2],
Fact.[NET_PAYMENTS_TERM_PER],
Fact.[REF_KEY_FOR_LINE_ITM],
Fact.[PROFT_CNTR_KEY],
Fact.[SEG_KEY],
Fact.[BILLING_DOCMT_KEY],
Fact.[ETL_SRC_SYS_CD],
Fact.[ETL_CREATED_TS],
Fact.[ETL_UPDTD_TS],
Fact.[CONTROLLING_AREA],
Fact.[WBS_ELEMNT_KEY],
Fact.[SALES_AREA_KEY],
Fact.[CUST_SALES_AREA_KEY],
Fact.[CUST_CREDIT_KEY],
Fact.[DOCMT_TYP_DESC],
Fact.[SPCL_GL_IND_DESC],
Fact.[PSTNG_KEY_DESC],
Fact.[PYMT_TRMS_DESC],
Fact.[CONTROLLING_AREA_DESC],
Fact.[LDGR_CD_DESC],
Fact.[ITM_TEXT],
Fact.[DELETION_FLG],
Fact.[DEBIT_CREDIT_IND_BSEG],
Fact.[EXPECTED_CUST_SALES_ORGZTN],
Fact.[EXPECTED_CUST_SALES_ORGZTN_DESC],
Fact.[EXPECTED_SALES_ORGZTN],
Fact.[EXPECTED_SALES_ORGZTN_DESC],
Fact.[DUNNING_BLK],
Fact.[DUNNING_BLK_DESC],
Fact.[LAST_DUNNING_NOTICE_DATE_KEY],
Fact.[DUNNING_LVL],
Fact.[DUNNING_KEY],
Fact.[DUNNING_KEY_DESC]
FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_F_FI_ACCTRCV2_CUR_D] Fact
LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] a
ON Fact.[DOCMT_DATE_KEY] = a.[DATE_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] b
ON Fact.[PSTNG_DATE_KEY] = b.[DATE_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] c
ON Fact.[CMPNY_KEY] = c.[CMPNY_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D] d
ON Fact.[PAYER_CUST_KEY] = d.[CUST_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] e 
ON Fact.[DOCMT_CRNCY_KEY] = e.[CRNCY_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] f 
ON Fact.[GROUP_CRNCY_KEY] = f.[CRNCY_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] g
ON Fact.[CMPNY_CRNCY_KEY] = g.[CRNCY_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[CLEARING_DATE] h
ON Fact.[CLEARING_DATE_KEY] = h.[DATE_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[BASELINE_DUE_DATE] i
ON Fact.[BSLINE_DUE_DATE_KEY] = i.[DATE_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] j
ON Fact.[PROFT_CNTR_KEY] = j.[PROFT_CNTR_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_INVOICE_CUR_D] k
ON Fact.[BILLING_DOCMT_KEY] = k.[INVC_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRS_WBSELEM_CUR_D] l
ON Fact.[WBS_ELEMNT_KEY] = l.[WBS_ELEMNT_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] m
ON Fact.[GL_KEY] = m.[GL_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSCRED_CUR_D] n
ON Fact.[CUST_CREDIT_KEY] = n.[CUST_CREDIT_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] o
ON Fact.[SALES_AREA_KEY] = o.[SALES_AREA_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTSAL_CUR_D] p
ON Fact.[CUST_SALES_AREA_KEY] = p.[CUST_SALES_AREA_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D] q
ON Fact.[SEG_KEY] = q.[SEG_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCHZ_CUR_D] r
ON j.[PROFT_CNTR_KEY] = r.[PROFT_CNTR_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_PROFCVZ_CUR_D] s
ON j.[PROFT_CNTR_KEY] = s.[PROFT_CNTR_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[LAST_DUNNING_NOTICE_DATE] t
ON Fact.[LAST_DUNNING_NOTICE_DATE_KEY] = t.[DATE_KEY];
END;
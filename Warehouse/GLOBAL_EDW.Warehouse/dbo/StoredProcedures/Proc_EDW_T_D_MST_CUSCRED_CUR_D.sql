CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSCRED_CUR_D]
	AS
	BEGIN  
		-- Update existing records
		UPDATE T
		SET
			T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY],
			T.[Customer Credit Number] = S.[CUST_NUM],
			T.[Customer Credit Control Area] = S.[CREDIT_CTRL_AREA],
			T.[Customer Credit Limit] = S.[CREDIT_LMT],
			T.[Customer Credit Total Receivables] = S.[TOTAL_RECEIVABLES],
			T.[Customer Credit Account] = S.[CREDIT_ACCT],
			T.[Customer Credit Group Description] = S.[CUST_CREDIT_GROUP_DESC],
			T.[Customer Credit Currency Description] = S.[CRNCY_DESC],
			T.[Current Record Indicator of Customer Credit] = S.[ETL_CURR_RCD_IND],
			T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
			T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
			T.[Cust Sales Area Credit Control Area Description] = S.[CREDIT_CTRL_AREA_DESC],
			T.[Customer Credit Rep Group Description] = S.[CREDIT_REP_GROUP_DESC],
			T.[Customer Credit Risk Category Description] = S.[RISK_CTGRY_DESC],
			T.[Customer Credit Recommended Credit Limit] = S.[RECOMMENDED_CREDIT_LMT],
			T.[Customer Credit Changed By] = S.[CHANGED_BY],
			T.[Customer Credit Changed On] = S.[CHANGED_ON],
			T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
			T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
			T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
			T.[Customer Credit Group] = S.[CUST_CREDIT_GROUP],
			T.[Customer Credit Last Payment Amount] = S.[LAST_PYMT_AMT],
			T.[Customer Credit Last Payment Date] = S.[LAST_PYMT_DATE],
			T.[Customer Credit Currency] = S.[CRNCY],
			T.[Customer Credit Payment Index] = S.[PYMT_IDX],
			T.[Customer Credit Rating] = S.[RTG],
			T.[Customer Credit Rep Group] = S.[CREDIT_REP_GROUP],
			T.[Customer Credit Exceeded On Date] = S.[EXCEEDED_ON_DATE],
			T.[Customer Credit Blocked] = S.[BLCKD],
			T.[Customer Credit Risk Category] = S.[RISK_CTGRY],
			T.[Customer Credit Info Number] = S.[CREDIT_INFO_NUM],
			T.[Customer Credit Last External Review Date] = S.[LAST_EXTNL_REVIEW_DATE]
	FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSCRED_CUR_D] AS T
	INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSCRED_CUR_D] AS S
	ON T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY];


    -- =============================
    -- Step 2: Insert new records
    -- =============================
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSCRED_CUR_D](
    [CUST_CREDIT_KEY],
    [Customer Credit Number],
    [Customer Credit Control Area],
    [Customer Credit Limit],
    [Customer Credit Total Receivables],
    [Customer Credit Account],
    [Customer Credit Group Description],
    [Customer Credit Currency Description],
    [Current Record Indicator of Customer Credit],
    [ETL_CREATED_TS],
	[ETL_UPDTD_TS],									 
    [Cust Sales Area Credit Control Area Description],
    [Customer Credit Rep Group Description],
    [Customer Credit Risk Category Description],
    [Customer Credit Recommended Credit Limit],
    [Customer Credit Changed By],
    [Customer Credit Changed On],
    [ETL_SRC_SYS_CD],
    [ETL_EFFECTV_BEGIN_DATE],                         
    [ETL_EFFECTV_END_DATE],                      
    [Customer Credit Group],
    [Customer Credit Last Payment Amount],
    [Customer Credit Last Payment Date],
    [Customer Credit Currency],
    [Customer Credit Payment Index],
    [Customer Credit Rating],
    [Customer Credit Rep Group],
    [Customer Credit Exceeded On Date],
    [Customer Credit Blocked],
    [Customer Credit Risk Category],
    [Customer Credit Info Number],
    [Customer Credit Last External Review Date]      
)
SELECT

		S.[CUST_CREDIT_KEY],
		S.[CUST_NUM],
		S.[CREDIT_CTRL_AREA],
		S.[CREDIT_LMT],
		S.[TOTAL_RECEIVABLES],
		S.[CREDIT_ACCT],
		S.[CUST_CREDIT_GROUP_DESC],
		S.[CRNCY_DESC],
		S.[ETL_CURR_RCD_IND],
		S.[ETL_CREATED_TS],
		S.[ETL_UPDTD_TS],
		S.[CREDIT_CTRL_AREA_DESC],
		S.[CREDIT_REP_GROUP_DESC],
		S.[RISK_CTGRY_DESC],
		S.[RECOMMENDED_CREDIT_LMT],
		S.[CHANGED_BY],
		S.[CHANGED_ON],
		S.[ETL_SRC_SYS_CD],
		S.[ETL_EFFECTV_BEGIN_DATE],
		S.[ETL_EFFECTV_END_DATE],
		S.[CUST_CREDIT_GROUP],
		S.[LAST_PYMT_AMT],
		S.[LAST_PYMT_DATE],
		S.[CRNCY],
		S.[PYMT_IDX],
		S.[RTG],
		S.[CREDIT_REP_GROUP],
		S.[EXCEEDED_ON_DATE],
		S.[BLCKD],
		S.[RISK_CTGRY],
		S.[CREDIT_INFO_NUM],
		S.[LAST_EXTNL_REVIEW_DATE]
 FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSCRED_CUR_D] AS S
 LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSCRED_CUR_D] AS T
 ON T.[CUST_CREDIT_KEY] = S.[CUST_CREDIT_KEY]
     WHERE T.[CUST_CREDIT_KEY] IS NULL;
END;
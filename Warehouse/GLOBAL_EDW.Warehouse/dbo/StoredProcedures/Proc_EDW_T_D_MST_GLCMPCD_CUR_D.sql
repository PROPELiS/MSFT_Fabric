CREATE       PROCEDURE [dbo].[Proc_EDW_T_D_MST_GLCMPCD_CUR_D]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records in target table
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[GL_CD] = S.[GL_CD],
        T.[CMPNY_CD] = S.[CMPNY_CD],
        T.[CONTROLLING_AREA] = S.[CONTROLLING_AREA],
        T.[CONTROLLING_AREA_DESC] = S.[CONTROLLING_AREA_DESC],
        T.[CNTRY_KEY] = S.[CNTRY_KEY],
        T.[CREATED_DATE] = S.[CREATED_DATE],
        T.[CREATED_BY] = S.[CREATED_BY],
        T.[MRKD_FOR_DELETION_IND] = S.[MRKD_FOR_DELETION_IND],
        T.[ACCT_CRNCY] = S.[ACCT_CRNCY],
        T.[ACCT_CRNCY_DESC] = S.[ACCT_CRNCY_DESC],
        T.[FLD_STATUS_GROUP] = S.[FLD_STATUS_GROUP],
        T.[FLD_STATUS_GROUP_DESC] = S.[FLD_STATUS_GROUP_DESC],
        T.[LINE_ITMS_DISPLAY_IND] = S.[LINE_ITMS_DISPLAY_IND],
        T.[BALANCES_IN_LCL_CRNCY_IND] = S.[BALANCES_IN_LCL_CRNCY_IND],
        T.[OPEN_ITM_MGMNT_IND] = S.[OPEN_ITM_MGMNT_IND],
        T.[NO_TAX_CD_REQUIRED_IND] = S.[NO_TAX_CD_REQUIRED_IND],
        T.[CASH_RCPT_DISBURSEMENT_IND] = S.[CASH_RCPT_DISBURSEMENT_IND],
        T.[ACCT_TAX_CTGRY] = S.[ACCT_TAX_CTGRY],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GLCMPCD_CUR_D] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_GLCMPCD_CUR_D] S
        ON T.[GL_CMPNY_KEY] = S.[GL_CMPNY_KEY];

    ----------------------------------------------------------------------
    -- Insert new records into target table
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GLCMPCD_CUR_D] (
        [GL_CMPNY_KEY],
        [GL_CD],
        [CMPNY_CD],
        [CONTROLLING_AREA],
        [CONTROLLING_AREA_DESC],
        [CNTRY_KEY],
        [CREATED_DATE],
        [CREATED_BY],
        [MRKD_FOR_DELETION_IND],
        [ACCT_CRNCY],
        [ACCT_CRNCY_DESC],
        [FLD_STATUS_GROUP],
        [FLD_STATUS_GROUP_DESC],
        [LINE_ITMS_DISPLAY_IND],
        [BALANCES_IN_LCL_CRNCY_IND],
        [OPEN_ITM_MGMNT_IND],
        [NO_TAX_CD_REQUIRED_IND],
        [CASH_RCPT_DISBURSEMENT_IND],
        [ACCT_TAX_CTGRY],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[GL_CMPNY_KEY],
        S.[GL_CD],
        S.[CMPNY_CD],
        S.[CONTROLLING_AREA],
        S.[CONTROLLING_AREA_DESC],
        S.[CNTRY_KEY],
        S.[CREATED_DATE],
        S.[CREATED_BY],
        S.[MRKD_FOR_DELETION_IND],
        S.[ACCT_CRNCY],
        S.[ACCT_CRNCY_DESC],
        S.[FLD_STATUS_GROUP],
        S.[FLD_STATUS_GROUP_DESC],
        S.[LINE_ITMS_DISPLAY_IND],
        S.[BALANCES_IN_LCL_CRNCY_IND],
        S.[OPEN_ITM_MGMNT_IND],
        S.[NO_TAX_CD_REQUIRED_IND],
        S.[CASH_RCPT_DISBURSEMENT_IND],
        S.[ACCT_TAX_CTGRY],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_GLCMPCD_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GLCMPCD_CUR_D] T
        ON T.[GL_CMPNY_KEY] = S.[GL_CMPNY_KEY]
    WHERE T.[GL_CMPNY_KEY] IS NULL;
END
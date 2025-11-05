CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_GENLDGR_CUR_D]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[General Ledger Account] = S.[GL_CD],
        T.[Chart Of General Ledger Account] = S.[CHART_OF_ACCTS],
        T.[General Ledger Account Description] = S.[GL_DESC],
        T.[General Ledger Account Is Balance Sheet] = S.[IS_BAL_SHEET_ACCT],
        T.[General Ledger Account Created Date] = S.[CREATED_DATE],
        T.[General Ledger Account Created By] = S.[CREATED_BY],
        T.[General Ledger Account Group] = S.[ACCT_GROUP],
        T.[General Ledger Account Marked For Deletion] = S.[MRKD_FOR_DELETION],
        T.[General Ledger Account Blocked For Creation] = S.[BLCKD_FOR_CREATION],
        T.[General Ledger Account Blocked For Posting] = S.[BLCKD_FOR_PSTNG],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[General Ledger Chart Of Accounts Description] = S.[CHART_OF_ACCTS_DESC],
        T.[General Ledger Account Group Description] = S.[ACCT_GROUP_DESC],
        T.[General Ledger Functional Area] = S.[FUNCTNL_AREA],
        T.[General Ledger Functional Area Description] = S.[FUNCTNL_AREA_DESC],
        T.[General Ledger P L Statement Account Type] = S.[P_L_STMT_ACCT_TYP],
        T.[General Ledger Group Account Number] = S.[GROUP_ACCT_NUM],
        T.[General Ledger Search Matchcode] = S.[SEARCH_MATCHCODE],
        T.[General Ledger Short Description] = S.[GL_SHORT_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] S
        ON T.[GL_KEY] = S.[GL_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] (
        [GL_KEY],
        [General Ledger Account],
        [Chart Of General Ledger Account],
        [General Ledger Account Description],
        [General Ledger Account Is Balance Sheet],
        [General Ledger Account Created Date],
        [General Ledger Account Created By],
        [General Ledger Account Group],
        [General Ledger Account Marked For Deletion],
        [General Ledger Account Blocked For Creation],
        [General Ledger Account Blocked For Posting],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [General Ledger Chart Of Accounts Description],
        [General Ledger Account Group Description],
        [General Ledger Functional Area],
        [General Ledger Functional Area Description],
        [General Ledger P L Statement Account Type],
        [General Ledger Group Account Number],
        [General Ledger Search Matchcode],
        [General Ledger Short Description]
    )
    SELECT
        S.[GL_KEY],
        S.[GL_CD],
        S.[CHART_OF_ACCTS],
        S.[GL_DESC],
        S.[IS_BAL_SHEET_ACCT],
        S.[CREATED_DATE],
        S.[CREATED_BY],
        S.[ACCT_GROUP],
        S.[MRKD_FOR_DELETION],
        S.[BLCKD_FOR_CREATION],
        S.[BLCKD_FOR_PSTNG],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[CHART_OF_ACCTS_DESC],
        S.[ACCT_GROUP_DESC],
        S.[FUNCTNL_AREA],
        S.[FUNCTNL_AREA_DESC],
        S.[P_L_STMT_ACCT_TYP],
        S.[GROUP_ACCT_NUM],
        S.[SEARCH_MATCHCODE],
        S.[GL_SHORT_DESC]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] T
        ON T.[GL_KEY] = S.[GL_KEY]
    WHERE T.[GL_KEY] IS NULL;
END
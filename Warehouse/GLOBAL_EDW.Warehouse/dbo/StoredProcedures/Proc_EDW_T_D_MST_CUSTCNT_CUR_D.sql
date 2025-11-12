CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSTCNT_CUR_D]
AS
BEGIN  
    UPDATE T SET
        T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY],
        T.[Customer Contact Person ID] = S.[CONTACT_PERSON_ID],
        T.[Customer Contact Customer ID] = S.[CUST_ID],
        T.[Customer Contact First Name] = S.[FIRST_NAME],
        T.[Customer Contact Name] = S.[NAME],
        T.[Customer Contact Contact Person Dept] = S.[CONTACT_PERSON_DEPT],
        T.[Customer Contact Contact Person Dept Name] = S.[CONTACT_PERSON_DEPT_NAME],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]

    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTCNT_CUR_D] S
        ON T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY];
 
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D] (
        [CUST_CONTACT_KEY],
        [Customer Contact Person ID],
        [Customer Contact Customer ID],
        [Customer Contact First Name],
        [Customer Contact Name],
        [Customer Contact Contact Person Dept],
        [Customer Contact Contact Person Dept Name],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
            
    )
    SELECT
        S.[CUST_CONTACT_KEY],
        S.[CONTACT_PERSON_ID],
        S.[CUST_ID],
        S.[FIRST_NAME],
        S.[NAME],
        S.[CONTACT_PERSON_DEPT],
        S.[CONTACT_PERSON_DEPT_NAME],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
       
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTCNT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D] T
        ON T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY]
    WHERE T.[CUST_CONTACT_KEY] IS NULL;
 
END
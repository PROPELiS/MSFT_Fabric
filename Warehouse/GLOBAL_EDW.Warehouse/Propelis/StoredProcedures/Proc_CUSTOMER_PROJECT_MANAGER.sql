CREATE   PROCEDURE [Propelis].[Proc_CUSTOMER_PROJECT_MANAGER]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Customer Project Manager Contact Person ID] = S.[CONTACT_PERSON_ID],
        T.[Customer Project Manager Customer ID] = S.[CUST_ID],
        T.[Customer Project Manager First Name] = S.[FIRST_NAME],
        T.[Customer Project Manager Name] = S.[NAME],
        T.[Customer Project Manager Contact Person Dept] = S.[CONTACT_PERSON_DEPT],
        T.[Customer Project Manager Contact Person Dept Name] = S.[CONTACT_PERSON_DEPT_NAME],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[CUSTOMER_PROJECT_MANAGER] T
    INNER JOIN [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D] S
        ON T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[CUSTOMER_PROJECT_MANAGER] (
        [CUST_CONTACT_KEY],
        [Customer Project Manager Contact Person ID],
        [Customer Project Manager Customer ID],
        [Customer Project Manager First Name],
        [Customer Project Manager Name],
        [Customer Project Manager Contact Person Dept],
        [Customer Project Manager Contact Person Dept Name],
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
    FROM [GLOBAL_EDW_Mirror].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CUSTOMER_PROJECT_MANAGER] T
        ON T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY]
    WHERE T.[CUST_CONTACT_KEY] IS NULL;
END
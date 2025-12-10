CREATE     PROCEDURE [Propelis].[Proc_SALES_PERSON_PERSONEL]
AS
BEGIN
    -- Step 1: Update existing records in target table
    UPDATE T
    SET
             T.[PERSONEL_KEY]	= S.[PERSONEL_KEY],
             T.[Personnel Number]	= S.[PERSONEL_NUM],
             T.[Company Code]	= S.[CMPNY_CD],
             T.[Plant ID]	= S.[PLNT_ID],
             T.[Cost Center ID]	= S.[COST_CNTR_ID],
             T.[Success Factors ID]	= S.[SUCCESS_FACTORS_ID],
             T.[Employee First Name]	= S.[EMPL_FIRST_NAME],
             T.[Employee Last Name]	= S.[EMPL_LAST_NAME],
             T.[Employee Full Name]	= S.[EMPL_FULL_NAME],
             T.[Employee Status lndicator]	= S.[EMPL_STATUS_IND],
             T.[Employment Start Date]	= S.[EMPLNT_START_DATE],
             T.[Employment End Date]	= S.[EMPLNT_END_DATE],
             T.[Employee Group Id]	= S.[EMPL_GROUP_ID],
             T.[Employee Group Description]	= S.[EMPL_GROUP_DESC],
             T.[Employee Subgroup ID]	= S.[EMPL_SUBGRP_ID],
             T.[Employee Subgroup Description]	= S.[EMPL_SUBGRP_DESC],
             T.[Employee Telephone 1]	= S.[EMPL_TEL_1],
             T.[Employee Telephone 2]	= S.[EMPL_TEL_2],
             T.[Email Address]	= S.[EMAIL_ADDR],
             T.[Created Date]	= S.[CREATED_DATE],
             T.[Created User ID]	= S.[CREATED_USR_ID],
             T.[Updated Date]	= S.[UPDTD_DATE],
             T.[Updated User ID]	= S.[UPDTD_USR_ID],
             T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
             T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
             T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
             T.[ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
             T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
             T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
             T.[Company Description]	= S.[CMPNY_DESC],
             T.[Plant Description]	= S.[PLNT_DESC]


    FROM [GLOBAL_EDW].[Propelis].[SALES_PERSON_PERSONEL] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    -- Step 2: Insert new records from mirror to target table
    INSERT INTO [GLOBAL_EDW].[Propelis].[SALES_PERSON_PERSONEL] (
                [PERSONEL_KEY],
                [Personnel Number],
                [Company Code],
                [Plant ID],
                [Cost Center ID],
                [Success Factors ID],
                [Employee First Name],
                [Employee Last Name],
                [Employee Full Name],
                [Employee Status lndicator],
                [Employment Start Date],
                [Employment End Date],
                [Employee Group Id],
                [Employee Group Description],
                [Employee Subgroup ID],
                [Employee Subgroup Description],
                [Employee Telephone 1],
                [Employee Telephone 2],
                [Email Address],
                [Created Date],
                [Created User ID],
                [Updated Date],
                [Updated User ID],
                [ETL_SRC_SYS_CD],
                [ETL_EFFECTV_BEGIN_DATE],
                [ETL_EFFECTV_END_DATE],
                [ETL_CURR_RCD_IND],
                [ETL_CREATED_TS],
                [ETL_UPDTD_TS],
                [Company Description],
                [Plant Description]

				
            )
    SELECT
        S.[PERSONEL_KEY],
        S.[PERSONEL_NUM],
        S.[CMPNY_CD],
        S.[PLNT_ID],
        S.[COST_CNTR_ID],
        S.[SUCCESS_FACTORS_ID],
        S.[EMPL_FIRST_NAME],
        S.[EMPL_LAST_NAME],
        S.[EMPL_FULL_NAME],
        S.[EMPL_STATUS_IND],
        S.[EMPLNT_START_DATE],
        S.[EMPLNT_END_DATE],
        S.[EMPL_GROUP_ID],
        S.[EMPL_GROUP_DESC],
        S.[EMPL_SUBGRP_ID],
        S.[EMPL_SUBGRP_DESC],
        S.[EMPL_TEL_1],
        S.[EMPL_TEL_2],
        S.[EMAIL_ADDR],
        S.[CREATED_DATE],
        S.[CREATED_USR_ID],
        S.[UPDTD_DATE],
        S.[UPDTD_USR_ID],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[CMPNY_DESC],
        S.[PLNT_DESC]
   
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERSONL_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SALES_PERSON_PERSONEL] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;

END
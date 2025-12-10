CREATE     PROCEDURE [Propelis].[Proc_CUSTOMER_SERVICE_REP]
AS
BEGIN
    -- Step 1: Update existing records in target
    UPDATE T
    SET
        [CSR Personnel Number]          = S.[PERSONEL_NUM],
        [CSR Company Code]              = S.[CMPNY_CD],
        [CSR Plant ID]                  = S.[PLNT_ID],
        [CSR Cost Center ID]            = S.[COST_CNTR_ID],
        [CSR Success Factors ID]        = S.[SUCCESS_FACTORS_ID],
        [CSR Employee First Name]       = S.[EMPL_FIRST_NAME],
        [CSR Employee Last Name]        = S.[EMPL_LAST_NAME],
        [CSR Employee Full Name]        = S.[EMPL_FULL_NAME],
        [CSR Employee Status Indicator] = S.[EMPL_STATUS_IND],
        [CSR Employment Start Date]     = S.[EMPLNT_START_DATE],
        [CSR Employment End Date]       = S.[EMPLNT_END_DATE],
        [CSR Employee Group ID]         = S.[EMPL_GROUP_ID],
        [CSR Employee Group Description]= S.[EMPL_GROUP_DESC],
        [CSR Employee Subgroup ID]      = S.[EMPL_SUBGRP_ID],
        [CSR Employee Subgroup Description] = S.[EMPL_SUBGRP_DESC],
        [CSR Employee Telephone 1]      = S.[EMPL_TEL_1],
        [CSR Employee Telephone 2]      = S.[EMPL_TEL_2],
        [CSR Email Address]             = S.[EMAIL_ADDR],
        [CSR Created Date]              = S.[CREATED_DATE],
        [CSR Created User ID]           = S.[CREATED_USR_ID],
        [CSR Updated Date]              = S.[UPDTD_DATE],
        [CSR Updated User ID]           = S.[UPDTD_USR_ID],
        [ETL_SRC_SYS_CD]                = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]        = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]          = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]              = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]                = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]                  = S.[ETL_UPDTD_TS],
        [CSR Company Description]       = S.[CMPNY_DESC],
        [CSR Plant Description]         = S.[PLNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[CUSTOMER_SERVICE_REP] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    -- Step 2: Insert new records that do not exist in target
    INSERT INTO [GLOBAL_EDW].[Propelis].[CUSTOMER_SERVICE_REP]
    (
        [PERSONEL_KEY],
        [CSR Personnel Number],
        [CSR Company Code],
        [CSR Plant ID],
        [CSR Cost Center ID],
        [CSR Success Factors ID],
        [CSR Employee First Name],
        [CSR Employee Last Name],
        [CSR Employee Full Name],
        [CSR Employee Status Indicator],
        [CSR Employment Start Date],
        [CSR Employment End Date],
        [CSR Employee Group ID],
        [CSR Employee Group Description],
        [CSR Employee Subgroup ID],
        [CSR Employee Subgroup Description],
        [CSR Employee Telephone 1],
        [CSR Employee Telephone 2],
        [CSR Email Address],
        [CSR Created Date],
        [CSR Created User ID],
        [CSR Updated Date],
        [CSR Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [CSR Company Description],
        [CSR Plant Description]
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CUSTOMER_SERVICE_REP] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;
END
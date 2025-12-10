CREATE     PROCEDURE [Propelis].[Proc_SALES_EMPLOYEE]
AS
BEGIN
    -- Step 1: Update existing records in the target table
    UPDATE T
    SET 
        [Sales Employee Number]                 = S.[PERSONEL_NUM],
        [Sales Employee Company Code]           = S.[CMPNY_CD],
        [Sales Employee Plant]                  = S.[PLNT_ID],
        [Sales Employee Cost Center ID]         = S.[COST_CNTR_ID],
        [Sales Employee Success Factors ID]     = S.[SUCCESS_FACTORS_ID],
        [Sales Employee First Name]             = S.[EMPL_FIRST_NAME],
        [Sales Employee Last Name]              = S.[EMPL_LAST_NAME],
        [Sales Employee Full Name]              = S.[EMPL_FULL_NAME],
        [Sales Employee Status Indicator]       = S.[EMPL_STATUS_IND],
        [Sales Employee Start Date]             = S.[EMPLNT_START_DATE],
        [Sales Employee End Date]               = S.[EMPLNT_END_DATE],
        [Sales Employee Group ID]               = S.[EMPL_GROUP_ID],
        [Sales Employee Group Description]      = S.[EMPL_GROUP_DESC],
        [Sales Employee Subgroup ID]            = S.[EMPL_SUBGRP_ID],
        [Sales Employee Subgroup Description]   = S.[EMPL_SUBGRP_DESC],
        [Sales Employee Telephone 1]            = S.[EMPL_TEL_1],
        [Sales Employee Telephone 2]            = S.[EMPL_TEL_2],
        [Sales Employee Email Address]          = S.[EMAIL_ADDR],
        [Sales Employee Created Date]           = S.[CREATED_DATE],
        [Sales Employee Created User ID]        = S.[CREATED_USR_ID],
        [Sales Employee Updated Date]           = S.[UPDTD_DATE],
        [Sales Employee Updated User ID]        = S.[UPDTD_USR_ID],
        [ETL_SRC_SYS_CD]                        = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]                = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]                  = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]                      = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]                        = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]                          = S.[ETL_UPDTD_TS],
        [Sales Employee Company Description]    = S.[CMPNY_DESC],
        [Sales Employee Plant Description]      = S.[PLNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[SALES_EMPLOYEE] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    -- Step 2: Insert new records not present in the target
    INSERT INTO [GLOBAL_EDW].[Propelis].[SALES_EMPLOYEE]
    (
        [PERSONEL_KEY],
        [Sales Employee Number],
        [Sales Employee Company Code],
        [Sales Employee Plant],
        [Sales Employee Cost Center ID],
        [Sales Employee Success Factors ID],
        [Sales Employee First Name],
        [Sales Employee Last Name],
        [Sales Employee Full Name],
        [Sales Employee Status Indicator],
        [Sales Employee Start Date],
        [Sales Employee End Date],
        [Sales Employee Group ID],
        [Sales Employee Group Description],
        [Sales Employee Subgroup ID],
        [Sales Employee Subgroup Description],
        [Sales Employee Telephone 1],
        [Sales Employee Telephone 2],
        [Sales Employee Email Address],
        [Sales Employee Created Date],
        [Sales Employee Created User ID],
        [Sales Employee Updated Date],
        [Sales Employee Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Sales Employee Company Description],
        [Sales Employee Plant Description]
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SALES_EMPLOYEE] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;
END
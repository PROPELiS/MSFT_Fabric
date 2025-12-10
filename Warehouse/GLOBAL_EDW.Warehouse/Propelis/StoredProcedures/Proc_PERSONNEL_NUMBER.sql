CREATE   PROCEDURE [Propelis].[Proc_PERSONNEL_NUMBER]
AS
BEGIN

    --------------------------------------------------------
    -- UPDATE existing records
    --------------------------------------------------------
    UPDATE T
    SET 
        T.[PERSONEL_KEY]                           = S.[PERSONEL_KEY],
        T.[Personnel Number]                       = S.[PERSONEL_NUM],
        T.[Personnel Company Code]                 = S.[CMPNY_CD],
        T.[Personnel Plant]                        = S.[PLNT_ID],
        T.[Personnel Cost Center ID]               = S.[COST_CNTR_ID],
        T.[Personnel Success Factors ID]           = S.[SUCCESS_FACTORS_ID],
        T.[Personnel Employee First Name]          = S.[EMPL_FIRST_NAME],
        T.[Personnel Employee Last Name]           = S.[EMPL_LAST_NAME],
        T.[Personnel Employee Full Name]           = S.[EMPL_FULL_NAME],
        T.[Personnel Employee Status Indicator]    = S.[EMPL_STATUS_IND],
        T.[Personnel Employee Start Date]          = S.[EMPLNT_START_DATE],
        T.[Personnel Employee End Date]            = S.[EMPLNT_END_DATE],
        T.[Personnel Employee Group ID]            = S.[EMPL_GROUP_ID],
        T.[Personnel Employee Group Description]   = S.[EMPL_GROUP_DESC],
        T.[Personnel Employee Subgroup ID]         = S.[EMPL_SUBGRP_ID],
        T.[Personnel Employee Subgroup Description]= S.[EMPL_SUBGRP_DESC],
        T.[Personnel Employee Telephone 1]         = S.[EMPL_TEL_1],
        T.[Personnel Employee Telephone 2]         = S.[EMPL_TEL_2],
        T.[Personnel Employee Email Address]       = S.[EMAIL_ADDR],
        T.[Personnel Created Date]                 = S.[CREATED_DATE],
        T.[Personnel Created User ID]              = S.[CREATED_USR_ID],
        T.[Personnel Updated Date]                 = S.[UPDTD_DATE],
        T.[Personnel Updated User ID]              = S.[UPDTD_USR_ID],
        T.[ETL_SRC_SYS_CD]                         = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                 = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                   = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                       = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                         = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                           = S.[ETL_UPDTD_TS],
        T.[Personnel Company Description]          = S.[CMPNY_DESC],
        T.[Plant Description]                      = S.[PLNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    --------------------------------------------------------
    -- INSERT new records
    --------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER]
    (
        [PERSONEL_KEY],
        [Personnel Number],
        [Personnel Company Code],
        [Personnel Plant],
        [Personnel Cost Center ID],
        [Personnel Success Factors ID],
        [Personnel Employee First Name],
        [Personnel Employee Last Name],
        [Personnel Employee Full Name],
        [Personnel Employee Status Indicator],
        [Personnel Employee Start Date],
        [Personnel Employee End Date],
        [Personnel Employee Group ID],
        [Personnel Employee Group Description],
        [Personnel Employee Subgroup ID],
        [Personnel Employee Subgroup Description],
        [Personnel Employee Telephone 1],
        [Personnel Employee Telephone 2],
        [Personnel Employee Email Address],
        [Personnel Created Date],
        [Personnel Created User ID],
        [Personnel Updated Date],
        [Personnel Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Personnel Company Description],
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
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;
END;
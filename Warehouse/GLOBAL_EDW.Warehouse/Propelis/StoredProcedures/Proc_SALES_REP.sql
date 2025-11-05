CREATE   PROCEDURE [Propelis].[Proc_SALES_REP]
AS
BEGIN
    -- Step 1: Update existing records in target
    UPDATE T
    SET
        [Sales Rep Personnel Number]          = S.[PERSONEL_NUM],
        [Sales Rep Company Code]              = S.[CMPNY_CD],
        [Sales Rep Plant ID]                  = S.[PLNT_ID],
        [Sales Rep Cost Center ID]            = S.[COST_CNTR_ID],
        [Sales Rep Success Factors ID]        = S.[SUCCESS_FACTORS_ID],
        [Sales Rep Employee First Name]       = S.[EMPL_FIRST_NAME],
        [Sales Rep Employee Last Name]        = S.[EMPL_LAST_NAME],
        [Sales Rep Employee Full Name]        = S.[EMPL_FULL_NAME],
        [Sales Rep Employee Status Indicator] = S.[EMPL_STATUS_IND],
        [Sales Rep Employment Start Date]     = S.[EMPLNT_START_DATE],
        [Sales Rep Employment End Date]       = S.[EMPLNT_END_DATE],
        [Sales Rep Employee Group ID]         = S.[EMPL_GROUP_ID],
        [Sales Rep Employee Group Description]= S.[EMPL_GROUP_DESC],
        [Sales Rep Employee Subgroup ID]      = S.[EMPL_SUBGRP_ID],
        [Sales Rep Employee Subgroup Description] = S.[EMPL_SUBGRP_DESC],
        [Sales Rep Employee Telephone 1]      = S.[EMPL_TEL_1],
        [Sales Rep Employee Telephone 2]      = S.[EMPL_TEL_2],
        [Sales Rep Email Address]             = S.[EMAIL_ADDR],
        [Sales Rep Created Date]              = S.[CREATED_DATE],
        [Sales Rep Created User ID]           = S.[CREATED_USR_ID],
        [Sales Rep Updated Date]              = S.[UPDTD_DATE],
        [Sales Rep Updated User ID]           = S.[UPDTD_USR_ID],
        [ETL_SRC_SYS_CD]                      = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]              = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]                = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]                    = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]                      = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]                        = S.[ETL_UPDTD_TS],
        [Sales Rep Company Description]       = S.[CMPNY_DESC],
        [Sales Rep Plant Description]         = S.[PLNT_DESC]
    FROM [GLOBAL_EDW].[Propelis].[SALES_REP] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    -- Step 2: Insert new records that do not exist in target
    INSERT INTO [GLOBAL_EDW].[Propelis].[SALES_REP]
    (
        [PERSONEL_KEY],
        [Sales Rep Personnel Number],
        [Sales Rep Company Code],
        [Sales Rep Plant ID],
        [Sales Rep Cost Center ID],
        [Sales Rep Success Factors ID],
        [Sales Rep Employee First Name],
        [Sales Rep Employee Last Name],
        [Sales Rep Employee Full Name],
        [Sales Rep Employee Status Indicator],
        [Sales Rep Employment Start Date],
        [Sales Rep Employment End Date],
        [Sales Rep Employee Group ID],
        [Sales Rep Employee Group Description],
        [Sales Rep Employee Subgroup ID],
        [Sales Rep Employee Subgroup Description],
        [Sales Rep Employee Telephone 1],
        [Sales Rep Employee Telephone 2],
        [Sales Rep Email Address],
        [Sales Rep Created Date],
        [Sales Rep Created User ID],
        [Sales Rep Updated Date],
        [Sales Rep Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Sales Rep Company Description],
        [Sales Rep Plant Description]
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
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SALES_REP] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;
END
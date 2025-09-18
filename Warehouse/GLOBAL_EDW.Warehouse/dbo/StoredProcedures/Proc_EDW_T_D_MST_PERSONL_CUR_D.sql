CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_PERSONL_CUR_D]
AS
BEGIN
    -- Step 1: Update existing records in target
    UPDATE T
    SET 
        [Personnel Number]                 = S.[PERSONEL_NUM],
        [Personnel Company Code]           = S.[CMPNY_CD],
        [Personnel Plant ID]               = S.[PLNT_ID],
        [Personnel Cost Center ID]         = S.[COST_CNTR_ID],
        [Personnel Success Factors ID]     = S.[SUCCESS_FACTORS_ID],
        [Personnel Employee First Name]    = S.[EMPL_FIRST_NAME],
        [Personnel Employee Last Name]     = S.[EMPL_LAST_NAME],
        [Personnel Employee Full Name]     = S.[EMPL_FULL_NAME],
        [Personnel Employee Status Indicator] = S.[EMPL_STATUS_IND],
        [Personnel Employment Start Date]  = S.[EMPLNT_START_DATE],
        [Personnel Employment End Date]    = S.[EMPLNT_END_DATE],
        [Personnel Employee Group ID]      = S.[EMPL_GROUP_ID],
        [Personnel Employee Group Description] = S.[EMPL_GROUP_DESC],
        [Personnel Employee SubGroup ID]   = S.[EMPL_SUBGRP_ID],
        [Personnel Employee SubGroup Description] = S.[EMPL_SUBGRP_DESC],
        [Personnel Employee Telephone 1]   = S.[EMPL_TEL_1],
        [Personnel Employee Telephone 2]   = S.[EMPL_TEL_2],
        [EMAIL_ADDR]                       = S.[EMAIL_ADDR],
        [Personnel Created Date]           = S.[CREATED_DATE],
        [Personnel Created User ID]        = S.[CREATED_USR_ID],
        [Personnel Updated Date]           = S.[UPDTD_DATE],
        [Personnel Updated User ID]        = S.[UPDTD_USR_ID],
        [ETL_SRC_SYS_CD]                   = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]           = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]             = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]                 = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]                   = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]                     = S.[ETL_UPDTD_TS],
        [Personnel Company Description]    = S.[CMPNY_DESC],
        [Personnel Plant Description]      = S.[PLNT_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY];

    -- Step 2: Insert new records that do not exist in target
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D]
    (
        [PERSONEL_KEY],
        [Personnel Number],
        [Personnel Company Code],
        [Personnel Plant ID],
        [Personnel Cost Center ID],
        [Personnel Success Factors ID],
        [Personnel Employee First Name],
        [Personnel Employee Last Name],
        [Personnel Employee Full Name],
        [Personnel Employee Status Indicator],
        [Personnel Employment Start Date],
        [Personnel Employment End Date],
        [Personnel Employee Group ID],
        [Personnel Employee Group Description],
        [Personnel Employee SubGroup ID],
        [Personnel Employee SubGroup Description],
        [Personnel Employee Telephone 1],
        [Personnel Employee Telephone 2],
        [EMAIL_ADDR],
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
        [Personnel Plant Description]
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
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS T
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE T.[PERSONEL_KEY] IS NULL;

END
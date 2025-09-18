CREATE   PROCEDURE Propelis.Proc_PERSONNEL_NUMBER
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

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
        S.[PLNT_DESC],

        HASHBYTES('SHA2_256', 
            CONCAT(
                 ISNULL(S.[PERSONEL_KEY], ''), '|',
                 ISNULL(S.[PERSONEL_NUM], ''), '|',
                 ISNULL(S.[CMPNY_CD], ''), '|',
                 ISNULL(S.[PLNT_ID], ''), '|',
                 ISNULL(S.[COST_CNTR_ID], ''), '|',
                 ISNULL(S.[SUCCESS_FACTORS_ID], ''), '|',
                 ISNULL(S.[EMPL_FIRST_NAME], ''), '|',
                 ISNULL(S.[EMPL_LAST_NAME], ''), '|',
                 ISNULL(S.[EMPL_FULL_NAME], ''), '|',
                 ISNULL(S.[EMPL_STATUS_IND], ''), '|',
                 ISNULL(S.[EMPLNT_START_DATE], ''), '|',
                 ISNULL(S.[EMPLNT_END_DATE], ''), '|',
                 ISNULL(S.[EMPL_GROUP_ID], ''), '|',
                 ISNULL(S.[EMPL_GROUP_DESC], ''), '|',
                 ISNULL(S.[EMPL_SUBGRP_ID], ''), '|',
                 ISNULL(S.[EMPL_SUBGRP_DESC], ''), '|',
                 ISNULL(S.[EMPL_TEL_1], ''), '|',
                 ISNULL(S.[EMPL_TEL_2], ''), '|',
                 ISNULL(S.[EMAIL_ADDR], ''), '|',
                 ISNULL(S.[CREATED_DATE], ''), '|',
                 ISNULL(S.[CREATED_USR_ID], ''), '|',
                 ISNULL(S.[UPDTD_DATE], ''), '|',
                 ISNULL(S.[UPDTD_USR_ID], ''), '|',
                 ISNULL(S.[ETL_SRC_SYS_CD], ''), '|',
                 ISNULL(S.[ETL_EFFECTV_BEGIN_DATE], ''), '|',
                 ISNULL(S.[ETL_EFFECTV_END_DATE], ''), '|',
                 ISNULL(S.[ETL_CURR_RCD_IND], ''), '|',
                 ISNULL(S.[ETL_CREATED_TS], ''), '|',
                 ISNULL(S.[ETL_UPDTD_TS], ''), '|',
                 ISNULL(S.[CMPNY_DESC], ''), '|',
                 ISNULL(S.[PLNT_DESC], ''), '|'

            )
        ) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PERSONL_CUR_D  ] AS S;

    -- Step 2: Update changed rows in target (hash mismatch)
    UPDATE T
    SET 
        [PERSONEL_KEY]	= S.[PERSONEL_KEY],
        [Personnel Number]	= S.[PERSONEL_NUM],
        [Personnel Company Code]	= S.[CMPNY_CD],
        [Personnel Plant]	= S.[PLNT_ID],
        [Personnel Cost Center ID]	= S.[COST_CNTR_ID],
        [Personnel Success Factors ID]	= S.[SUCCESS_FACTORS_ID],
        [Personnel Employee First Name]	= S.[EMPL_FIRST_NAME],
        [Personnel Employee Last Name]	= S.[EMPL_LAST_NAME],
        [Personnel Employee Full Name]	= S.[EMPL_FULL_NAME],
        [Personnel Employee Status Indicator]	= S.[EMPL_STATUS_IND],
        [Personnel Employee Start Date]	= S.[EMPLNT_START_DATE],
        [Personnel Employee End Date]	= S.[EMPLNT_END_DATE],
        [Personnel Employee Group ID]	= S.[EMPL_GROUP_ID],
        [Personnel Employee Group Description]	= S.[EMPL_GROUP_DESC],
        [Personnel Employee Subgroup ID]	= S.[EMPL_SUBGRP_ID],
        [Personnel Employee Subgroup Description]	= S.[EMPL_SUBGRP_DESC],
        [Personnel Employee Telephone 1]	= S.[EMPL_TEL_1],
        [Personnel Employee Telephone 2]	= S.[EMPL_TEL_2],
        [Personnel Employee Email Address]	= S.[EMAIL_ADDR],
        [Personnel Created Date]	= S.[CREATED_DATE],
        [Personnel Created User ID]	= S.[CREATED_USR_ID],
        [Personnel Updated Date]	= S.[UPDTD_DATE],
        [Personnel Updated User ID]	= S.[UPDTD_USR_ID],
        [ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
        [Personnel Company Description]	= S.[CMPNY_DESC],
        [Plant Description]	= S.[PLNT_DESC]


    FROM [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER] AS T
    INNER JOIN #SourceData AS S
        ON T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    WHERE HASHBYTES('SHA2_256', 
            CONCAT(
				ISNULL(T.[PERSONEL_KEY], ''), '|',
                ISNULL(T.[Personnel Number], ''), '|',
                ISNULL(T.[Personnel Company Code], ''), '|',
                ISNULL(T.[Personnel Plant], ''), '|',
                ISNULL(T.[Personnel Cost Center ID], ''), '|',
                ISNULL(T.[Personnel Success Factors ID], ''), '|',
                ISNULL(T.[Personnel Employee First Name], ''), '|',
                ISNULL(T.[Personnel Employee Last Name], ''), '|',
                ISNULL(T.[Personnel Employee Full Name], ''), '|',
                ISNULL(T.[Personnel Employee Status Indicator], ''), '|',
                ISNULL(T.[Personnel Employee Start Date], ''), '|',
                ISNULL(T.[Personnel Employee End Date], ''), '|',
                ISNULL(T.[Personnel Employee Group ID], ''), '|',
                ISNULL(T.[Personnel Employee Group Description], ''), '|',
                ISNULL(T.[Personnel Employee Subgroup ID], ''), '|',
                ISNULL(T.[Personnel Employee Subgroup Description], ''), '|',
                ISNULL(T.[Personnel Employee Telephone 1], ''), '|',
                ISNULL(T.[Personnel Employee Telephone 2], ''), '|',
                ISNULL(T.[Personnel Employee Email Address], ''), '|',
                ISNULL(T.[Personnel Created Date], ''), '|',
                ISNULL(T.[Personnel Created User ID], ''), '|',
                ISNULL(T.[Personnel Updated Date], ''), '|',
                ISNULL(T.[Personnel Updated User ID], ''), '|',
                ISNULL(T.[ETL_SRC_SYS_CD], ''), '|',
                ISNULL(T.[ETL_EFFECTV_BEGIN_DATE], ''), '|',
                ISNULL(T.[ETL_EFFECTV_END_DATE], ''), '|',
                ISNULL(T.[ETL_CURR_RCD_IND], ''), '|',
                ISNULL(T.[ETL_CREATED_TS], ''), '|',
                ISNULL(T.[ETL_UPDTD_TS], ''), '|',
                ISNULL(T.[Personnel Company Description], ''), '|',
                ISNULL(T.[Plant Description], ''), '|'

            )
        ) <> S.HashKey;

    -- Step 3: Insert only new rows not already in target
	
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

    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER] AS T
        WHERE T.[PERSONEL_KEY] = S.[PERSONEL_KEY]
    );

END;
CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_SAL_DCHAREN_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Object Number]              = S.[OBJ_NUM],
        T.[Class Type]                 = S.[CLASS_TYP],
        T.[Class Number]               = S.[CLASS_NUM],
        T.[Class Status]               = S.[CLASS_STATUS],
        T.[G Printer ID]               = S.[G_PRINTER_ID],
        T.[G Brand Owner]              = S.[G_BRANDOWNER],
        T.[G TD Description]           = S.[G_TD_DESC],
        T.[G SU Width]                 = S.[G_SU_WIDTH],
        T.[G SU Height]                = S.[G_SU_HEIGHT],
        T.[G Item Number]              = S.[G_ITM_NO],
        T.[G Job Type]                 = S.[G_JOBTYPE],
        T.[G Country ID]               = S.[G_CNTRY_ID],
        T.[G Is Security]              = S.[G_IS_SECURITY],
        T.[G Roto Technology]          = S.[G_ROTO_TECHNOLOGY],
        T.[ETL_SRC_SYS_CD]             = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]     = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]       = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]           = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]             = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]               = S.[ETL_UPDTD_TS],
        T.[G Supplied Files Min Date]  = S.[G_SUPPLIED_FILES_MIN_DATE],
        T.[G Supplied Files Max Date]  = S.[G_SUPPLIED_FILES_MAX_DATE]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] S
        ON T.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY] = S.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] (
        [DOCMT_CHARS_ENCOWAY_JS_HDR_KEY],
        [Object Number],
        [Class Type],
        [Class Number],
        [Class Status],
        [G Printer ID],
        [G Brand Owner],
        [G TD Description],
        [G SU Width],
        [G SU Height],
        [G Item Number],
        [G Job Type],
        [G Country ID],
        [G Is Security],
        [G Roto Technology],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [G Supplied Files Min Date],
        [G Supplied Files Max Date],
        [SequenceID]
    )
    SELECT
        S.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY],
        S.[OBJ_NUM],
        S.[CLASS_TYP],
        S.[CLASS_NUM],
        S.[CLASS_STATUS],
        S.[G_PRINTER_ID],
        S.[G_BRANDOWNER],
        S.[G_TD_DESC],
        S.[G_SU_WIDTH],
        S.[G_SU_HEIGHT],
        S.[G_ITM_NO],
        S.[G_JOBTYPE],
        S.[G_CNTRY_ID],
        S.[G_IS_SECURITY],
        S.[G_ROTO_TECHNOLOGY],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[G_SUPPLIED_FILES_MIN_DATE],
        S.[G_SUPPLIED_FILES_MAX_DATE],
        S.[SequenceID]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] T
        ON T.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY] = S.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY]
    WHERE T.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY] IS NULL;
END
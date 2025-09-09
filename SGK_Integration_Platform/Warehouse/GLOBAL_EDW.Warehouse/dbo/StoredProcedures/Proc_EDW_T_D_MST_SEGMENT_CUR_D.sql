CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_SEGMENT_CUR_D]
AS
BEGIN
    
    UPDATE T
    SET
        T.[SEG_KEY] = S.[SEG_KEY],
        T.[Segment ID] = S.[SEG_ID],
        T.[Segment Code Description] = S.[SEG_CD_DESC],
        T.[Segment Business Unit Code] = S.[BIZ_UNIT_CD],
        T.[Segment Business Unit Code Description] = S.[BIZ_UNIT_CD_DESC],
        T.[Segment Created Date] = S.[CREATED_DATE],
        T.[Segment Created User ID] = S.[CREATED_USR_ID],
        T.[Segment Updated Date] = S.[UPDTD_DATE],
        T.[Segment Updated User ID] = S.[UPDTD_USR_ID],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
		
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] S
        ON T.[SEG_KEY] = S.[SEG_KEY];

    -- Step 2: Insert new records from [GLOBAL_EDW_MIRROR] to [GLOBAL_EDW] where SEG_KEY does not exist
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] (
		[SEG_KEY],
		[Segment ID],
		[Segment Code Description],
		[Segment Business Unit Code],
		[Segment Business Unit Code Description],
		[Segment Created Date],
		[Segment Created User ID],
		[Segment Updated Date],
		[Segment Updated User ID],
		[ETL_SRC_SYS_CD],
		[ETL_EFFECTV_BEGIN_DATE],
		[ETL_EFFECTV_END_DATE],
		[ETL_CURR_RCD_IND],
		[ETL_CREATED_TS],
		[ETL_UPDTD_TS]

    )
    SELECT
        S.[SEG_KEY],
		S.[SEG_ID],
		S.[SEG_CD_DESC],
		S.[BIZ_UNIT_CD],
		S.[BIZ_UNIT_CD_DESC],
		S.[CREATED_DATE],
		S.[CREATED_USR_ID],
		S.[UPDTD_DATE],
		S.[UPDTD_USR_ID],
		S.[ETL_SRC_SYS_CD],
		S.[ETL_EFFECTV_BEGIN_DATE],
		S.[ETL_EFFECTV_END_DATE],
		S.[ETL_CURR_RCD_IND],
		S.[ETL_CREATED_TS],
		S.[ETL_UPDTD_TS]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] T
        ON T.[SEG_KEY] = S.[SEG_KEY]
    WHERE T.[SEG_KEY] IS NULL;

END
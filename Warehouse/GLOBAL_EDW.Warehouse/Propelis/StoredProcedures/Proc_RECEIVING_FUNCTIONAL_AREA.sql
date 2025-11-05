CREATE   PROCEDURE [Propelis].[Proc_RECEIVING_FUNCTIONAL_AREA]
AS
BEGIN
    --------------------------------------------
    -- Step 1: Update existing records
    --------------------------------------------
    UPDATE T
    SET
        [Receiving Functional Area ID] = S.[FUNCTNL_AREA_ID],
        [Receiving Functional Area Description] = S.[FUNCTNL_AREA_DESC],
        [ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        [ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        [ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[RECEIVING_FUNCTIONAL_AREA] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] AS S
        ON T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY];

    --------------------------------------------
    -- Step 2: Insert new records
    --------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[RECEIVING_FUNCTIONAL_AREA] (
        [FUNCTNL_AREA_KEY],
        [Receiving Functional Area ID],
        [Receiving Functional Area Description],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[FUNCTNL_AREA_KEY],
        S.[FUNCTNL_AREA_ID],
        S.[FUNCTNL_AREA_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[RECEIVING_FUNCTIONAL_AREA] AS T
        ON T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY]
    WHERE T.[FUNCTNL_AREA_KEY] IS NULL;
END;
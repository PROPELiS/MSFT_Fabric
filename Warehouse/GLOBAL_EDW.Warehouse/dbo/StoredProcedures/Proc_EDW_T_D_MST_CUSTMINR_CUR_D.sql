CREATE       PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSTMINR_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[CUST_MATRL_INFO_RCD_KEY]	= S.[CUST_MATRL_INFO_RCD_KEY],
        T.[CMIR Sales Organization]	= S.[SALES_ORGZTN],
        T.[CMIR Distribution Channel]	= S.[DISTRBN_CHNL],
        T.[CMIR Customer Number]	= S.[CUST_ID],
        T.[CMIR Material ID]	= S.[MATRL_ID],
        T.[CMIR Customer Material ID]	= S.[CUST_MATRL_ID],
        T.[CMIR Base UoM]	= S.[BASE_UOM],
        T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        T.[Current Record Indicator of Customer Material Info Record]	= S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS],
        T.[CMIR Customer Description]	= S.[CUST_DESC]



 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTMINR_CUR_D] AS S
        ON T.[CUST_MATRL_INFO_RCD_KEY] = S.[CUST_MATRL_INFO_RCD_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] (
        [CUST_MATRL_INFO_RCD_KEY],
        [CMIR Sales Organization],
        [CMIR Distribution Channel],
        [CMIR Customer Number],
        [CMIR Material ID],
        [CMIR Customer Material ID],
        [CMIR Base UoM],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [Current Record Indicator of Customer Material Info Record],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [CMIR Customer Description]

)
    SELECT
       S.[CUST_MATRL_INFO_RCD_KEY],
       S.[SALES_ORGZTN],
       S.[DISTRBN_CHNL],
       S.[CUST_ID],
       S.[MATRL_ID],
       S.[CUST_MATRL_ID],
       S.[BASE_UOM],
       S.[ETL_SRC_SYS_CD],
       S.[ETL_EFFECTV_BEGIN_DATE],
       S.[ETL_EFFECTV_END_DATE],
       S.[ETL_CURR_RCD_IND],
       S.[ETL_CREATED_TS],
       S.[ETL_UPDTD_TS],
       S.[CUST_DESC]
    
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_CUSTMINR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] AS T
        ON T.[CUST_MATRL_INFO_RCD_KEY] = S.[CUST_MATRL_INFO_RCD_KEY]
    WHERE T.[CUST_MATRL_INFO_RCD_KEY] IS NULL;

END
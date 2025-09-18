CREATE PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSULPT_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Customer Number]                  = S.[CUST_CD],
        T.[Unloading Point]                  = S.[UNLOADING_PNT],
        T.[Sequential Number for Unloading Points] = S.[SEQAL_NUM_FOR_UNLOADING_POINTS],
        T.[Customer Factory Calendar]        = S.[CUSTOMERS_FACTORY_CLNDR],
        T.[Tax Classification Description]   = S.[TAX_CLSFTN_DESC],
        T.[ETL Source System Code]           = S.[ETL_SRC_SYS_CD],
        T.[ETL Effective Begin Date]         = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL Effective End Date]           = S.[ETL_EFFECTV_END_DATE],
        T.[ETL Current Record Indicator]     = S.[ETL_CURR_RCD_IND],
        T.[ETL Created Timestamp]            = S.[ETL_CREATED_TS],
        T.[ETL Updated Timestamp]            = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSULPT_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSULPT_CUR_D] S
        ON T.[CUST_UNLOADING_PNT_KEY] = S.[CUST_UNLOADING_PNT_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSULPT_CUR_D] (
        [CUST_UNLOADING_PNT_KEY],
        [Customer Number],
        [Unloading Point],
        [Sequential Number for Unloading Points],
        [Customer Factory Calendar],
        [Tax Classification Description],
        [ETL Source System Code],
        [ETL Effective Begin Date],
        [ETL Effective End Date],
        [ETL Current Record Indicator],
        [ETL Created Timestamp],
        [ETL Updated Timestamp]
    )
    SELECT
        S.[CUST_UNLOADING_PNT_KEY],
        S.[CUST_CD],
        S.[UNLOADING_PNT],
        S.[SEQAL_NUM_FOR_UNLOADING_POINTS],
        S.[CUSTOMERS_FACTORY_CLNDR],
        S.[TAX_CLSFTN_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSULPT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSULPT_CUR_D] T
        ON T.[CUST_UNLOADING_PNT_KEY] = S.[CUST_UNLOADING_PNT_KEY]
    WHERE T.[CUST_UNLOADING_PNT_KEY] IS NULL;
END
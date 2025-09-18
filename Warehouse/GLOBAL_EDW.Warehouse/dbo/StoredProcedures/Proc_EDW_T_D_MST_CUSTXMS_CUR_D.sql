CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSTXMS_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[CUST_TAX_MAST_KEY]	= S.[CUST_TAX_MAST_KEY],
        T.[Customer Number] = 	S.[CUST_CD],
        T.[Departure Country]	= S.[DEPARTURE_CNTRY],
        T.[Tax Category]	= S.[TAX_CTGRY],
        T.[Tax Category Description]	= S.[TAX_CTGRY_DESC],
        T.[Tax Classification]	= S.[TAX_CLSFTN_FOR_CUST],
        T.[Tax Classification Description]	= S.[TAX_CLSFTN_DESC],
        T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
        T.[Current Record Indicator of Customer Tax Master]	= S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS]
 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTXMS_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSTXMS_CUR_D] AS S
        ON T.[CUST_TAX_MAST_KEY] = S.[CUST_TAX_MAST_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTXMS_CUR_D] (
        [CUST_TAX_MAST_KEY],
        [Customer Number],
        [Departure Country],
        [Tax Category],
        [Tax Category Description],
        [Tax Classification],
        [Tax Classification Description],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [Current Record Indicator of Customer Tax Master],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
)
    SELECT
       S.[CUST_TAX_MAST_KEY],
       S.[CUST_CD],
       S.[DEPARTURE_CNTRY],
       S.[TAX_CTGRY],
       S.[TAX_CTGRY_DESC],
       S.[TAX_CLSFTN_FOR_CUST],
       S.[TAX_CLSFTN_DESC],
       S.[ETL_SRC_SYS_CD],
       S.[ETL_EFFECTV_BEGIN_DATE],
       S.[ETL_EFFECTV_END_DATE],
       S.[ETL_CURR_RCD_IND],
       S.[ETL_CREATED_TS],
       S.[ETL_UPDTD_TS]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSTXMS_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTXMS_CUR_D] AS T
        ON T.[CUST_TAX_MAST_KEY] = S.[CUST_TAX_MAST_KEY]
    WHERE T.[CUST_TAX_MAST_KEY] IS NULL;

END
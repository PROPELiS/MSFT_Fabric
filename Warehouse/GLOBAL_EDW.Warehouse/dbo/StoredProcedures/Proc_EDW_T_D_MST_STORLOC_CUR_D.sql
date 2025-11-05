CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_STORLOC_CUR_D]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records in target table
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Storage Location Code] = S.[STOR_LOC_CD],
        T.[Store Location Plant ID] = S.[PLNT_ID],
        T.[Store Location Warehouse Number] = S.[WRHSE_NUM],
        T.[Storage Location Description] = S.[STOR_LOC_DESC],
        T.[Store Location Shipping Receiving Point] = S.[SHIPG_RECVNG_PNT],
        T.[Store Location Sales Organization] = S.[SALES_ORGZTN],
        T.[Store Location Vendor Account Number] = S.[VNDR_ID],
        T.[Store Location Customer Account Number] = S.[CUST_ID],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Store Location Plant Description] = S.[PLNT_DESC],
        T.[Store Location Warehouse Description] = S.[WRHSE_DESC],
        T.[Store Location Shipping Receiving Point Description] = S.[SHIPG_RECVNG_PNT_DESC],
        T.[Store Location Sales Organization Description] = S.[SALES_ORG_DESC]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_STORLOC_CUR_D] T
    INNER JOIN [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_STORLOC_CUR_D] S
        ON T.[STOR_LOC_KEY] = S.[STOR_LOC_KEY];

    ----------------------------------------------------------------------
    -- Insert new records into target table
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_STORLOC_CUR_D] (
        [STOR_LOC_KEY],
        [Storage Location Code],
        [Store Location Plant ID],
        [Store Location Warehouse Number],
        [Storage Location Description],
        [Store Location Shipping Receiving Point],
        [Store Location Sales Organization],
        [Store Location Vendor Account Number],
        [Store Location Customer Account Number],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Store Location Plant Description],
        [Store Location Warehouse Description],
        [Store Location Shipping Receiving Point Description],
        [Store Location Sales Organization Description]
    )
    SELECT
        S.[STOR_LOC_KEY],
        S.[STOR_LOC_CD],
        S.[PLNT_ID],
        S.[WRHSE_NUM],
        S.[STOR_LOC_DESC],
        S.[SHIPG_RECVNG_PNT],
        S.[SALES_ORGZTN],
        S.[VNDR_ID],
        S.[CUST_ID],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[PLNT_DESC],
        S.[WRHSE_DESC],
        S.[SHIPG_RECVNG_PNT_DESC],
        S.[SALES_ORG_DESC]
    FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_STORLOC_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_STORLOC_CUR_D] T
        ON T.[STOR_LOC_KEY] = S.[STOR_LOC_KEY]
    WHERE T.[STOR_LOC_KEY] IS NULL;
END
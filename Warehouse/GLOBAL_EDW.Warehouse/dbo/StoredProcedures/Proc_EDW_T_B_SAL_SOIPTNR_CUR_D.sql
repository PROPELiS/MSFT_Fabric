CREATE       PROCEDURE [dbo].[Proc_EDW_T_B_SAL_SOIPTNR_CUR_D]
AS
BEGIN
    -------------------------------------------------------------------
    -- Step 1: Update existing records
    -------------------------------------------------------------------
    UPDATE T
    SET
        T.[BILL_TO_CUST_KEY]             = S.[BILLTO_CUST_KEY],
        T.[PAYER_CUST_KEY]               = S.[PAYER_CUST_KEY],
        T.[SHIP_TO_CUST_KEY]             = S.[SHIPTO_CUST_KEY],
        T.[BRAND_OWNER_CUST_KEY]         = S.[BRAND_OWNER_CUST_KEY],
        T.[CUST_CONTACT_KEY]             = S.[CUST_CONTACT_KEY],
        T.[CUST_PROJ_MGR_KEY]            = S.[CUST_POJECT_MGR_KEY],
        T.[SALES_PERSON_PERSONNEL_KEY]   = S.[SALES_PERSON_PERSONEL_KEY],
        T.[PERSON_RESPNSBLE_KEY]         = S.[PERSON_RESPNSBLE_PERSONEL_KEY],
        T.[CYLINDER_OWNER_CUST_KEY]      = S.[CYLINDER_OWNER_CUST_KEY],
        T.[SOLDTO_CUST_KEY]              = S.[SOLDTO_CUST_KEY],
        T.[PARENT_CUST_KEY]              = S.[PARNT_CUST_KEY],
        T.[AGENCY_CUST_KEY]              = S.[AGENCY_CUST_KEY],
        T.[PRINTER_CUST_KEY]             = S.[PRINTER_CUST_KEY],
        T.[SUPPLIER_CUST_KEY]            = S.[SUPLR_CUST_KEY],
        T.[EMPL_RESPNSBLE_KEY]           = S.[EMPL_RESPNSBLE_KEY],
        T.[EMPL_RESPNSBLE_2_KEY]         = S.[EMPL_RESPNSBLE_2_KEY],
        T.[ETL_SRC_SYS_CD]               = S.[ETL_SRC_SYS_CD],
        T.[ETL_CREATED_TS]               = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                 = S.[ETL_UPDTD_TS],
        T.[FORWARDING_AGENT_VNDR_KEY]    = S.[FORWARDING_AGENT_VNDR_KEY],
        T.[UNITIZATION_AGENT_VNDR_KEY]   = S.[UNITIZATION_AGENT_VNDR_KEY],
        T.[ORDER_FOR_CUST_KEY]           = S.[ORDER_FOR_CUST_KEY],
        T.[SO_PARENT_CUST_KEY]           = S.[SO_PARNT_CUST_KEY],
        T.[SOIPTNR_KEY] = CONCAT(S.[SALES_ORDER_ID],'-',S.[SALES_ORDER_ITM_ID])
    FROM [GLOBAL_EDW].[dbo].[EDW_T_B_SAL_SOIPTNR_CUR_D] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_B_SAL_SOIPTNR_CUR_D] S
        ON T.[SALES_ORDER_ID] = S.[SALES_ORDER_ID]
       AND T.[SALES_ORDER_ITM_ID] = S.[SALES_ORDER_ITM_ID];

    -------------------------------------------------------------------
    -- Step 2: Insert new records
    -------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_B_SAL_SOIPTNR_CUR_D] (
        [SALES_ORDER_ID],
        [SALES_ORDER_ITM_ID],
        [BILL_TO_CUST_KEY],
        [PAYER_CUST_KEY],
        [SHIP_TO_CUST_KEY],
        [BRAND_OWNER_CUST_KEY],
        [CUST_CONTACT_KEY],
        [CUST_PROJ_MGR_KEY],
        [SALES_PERSON_PERSONNEL_KEY],
        [PERSON_RESPNSBLE_KEY],
        [CYLINDER_OWNER_CUST_KEY],
        [SOLDTO_CUST_KEY],
        [PARENT_CUST_KEY],
        [AGENCY_CUST_KEY],
        [PRINTER_CUST_KEY],
        [SUPPLIER_CUST_KEY],
        [EMPL_RESPNSBLE_KEY],
        [EMPL_RESPNSBLE_2_KEY],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [FORWARDING_AGENT_VNDR_KEY],
        [UNITIZATION_AGENT_VNDR_KEY],
        [ORDER_FOR_CUST_KEY],
        [SO_PARENT_CUST_KEY],
        [SOIPTNR_KEY]
    )
    SELECT
        S.[SALES_ORDER_ID],
        S.[SALES_ORDER_ITM_ID],
        S.[BILLTO_CUST_KEY],
        S.[PAYER_CUST_KEY],
        S.[SHIPTO_CUST_KEY],
        S.[BRAND_OWNER_CUST_KEY],
        S.[CUST_CONTACT_KEY],
        S.[CUST_POJECT_MGR_KEY],
        S.[SALES_PERSON_PERSONEL_KEY],
        S.[PERSON_RESPNSBLE_PERSONEL_KEY],
        S.[CYLINDER_OWNER_CUST_KEY],
        S.[SOLDTO_CUST_KEY],
        S.[PARNT_CUST_KEY],
        S.[AGENCY_CUST_KEY],
        S.[PRINTER_CUST_KEY],
        S.[SUPLR_CUST_KEY],
        S.[EMPL_RESPNSBLE_KEY],
        S.[EMPL_RESPNSBLE_2_KEY],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[FORWARDING_AGENT_VNDR_KEY],
        S.[UNITIZATION_AGENT_VNDR_KEY],
        S.[ORDER_FOR_CUST_KEY],
        S.[SO_PARNT_CUST_KEY],
        CONCAT(S.[SALES_ORDER_ID],'-',S.[SALES_ORDER_ITM_ID])
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_B_SAL_SOIPTNR_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_B_SAL_SOIPTNR_CUR_D] T
        ON T.[SALES_ORDER_ID] = S.[SALES_ORDER_ID]
       AND T.[SALES_ORDER_ITM_ID] = S.[SALES_ORDER_ITM_ID]
    WHERE T.[SALES_ORDER_ID] IS NULL;
END
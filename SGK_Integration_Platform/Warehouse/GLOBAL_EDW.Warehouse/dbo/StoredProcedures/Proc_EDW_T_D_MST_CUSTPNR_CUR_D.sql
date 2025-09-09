CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_MST_CUSTPNR_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[CUST_PRTNR_KEY]	= S.[CUST_PRTNR_KEY],
        T.[Customer Partner Customer ID]	      = S.[CUST_ID],
        T.[Customer Partner Sales Organization]	      = S.[SALES_ORGZTN],
        T.[Customer Partner Sales Organization Description]	      = S.[SALES_ORG_DESC],
        T.[Customer Partner Distribution Channel]	      = S.[DISTRBN_CHNL],
        T.[Customer Partner Distribution Channel Description]	      = S.[DISTRBN_CHNL_DESC],
        T.[Customer Partner Division]	      = S.[DIVISION],
        T.[Customer Partner Division Description]	      = S.[DIVISION_DESC],
        T.[Customer Partner Customer Number of Business Partner]	      = S.[CUST_2_ID],
        T.[Customer Partner Function]	      = S.[PRTNR_FUNCTION],
        T.[Customer Partner Function Description]	      = S.[PRTNR_FUNCTION_DESC],
        T.[Customer Partner Personel ID]	      = S.[PERSONEL_ID],
        T.[Customer Partner Counter]	      = S.[PRTNR_COUNTER],
        T.[ETL_SRC_SYS_CD]	      = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]	      = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]	      = S.[ETL_EFFECTV_END_DATE],
        T.[Current Record Indicator of Customer Partner]	      = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]	      = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	      = S.[ETL_UPDTD_TS],
        T.[Customer Partner Vendor ID]	      = S.[VNDR_ID],
        T.[Customer Partner Contact Person ID]	      = S.[CONTACT_PERSON_ID],
        T.[Customer Partner Description]	      = S.[CUST_DESC_OF_PRTNR],
        T.[Customer Partner Default Partner]	      = S.[DEFAULT_PRTNR]

    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTPNR_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSTPNR_CUR_D] AS S
        ON T.[CUST_PRTNR_KEY] = S.[CUST_PRTNR_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTPNR_CUR_D] (
        [CUST_PRTNR_KEY],
        [Customer Partner Customer ID],
        [Customer Partner Sales Organization],
        [Customer Partner Sales Organization Description],
        [Customer Partner Distribution Channel],
        [Customer Partner Distribution Channel Description],
        [Customer Partner Division],
        [Customer Partner Division Description],
        [Customer Partner Customer Number of Business Partner],
        [Customer Partner Function],
        [Customer Partner Function Description],
        [Customer Partner Personel ID],
        [Customer Partner Counter],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [Current Record Indicator of Customer Partner],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Customer Partner Vendor ID],
        [Customer Partner Contact Person ID],
        [Customer Partner Description],
        [Customer Partner Default Partner]

    )
    SELECT
       S.[CUST_PRTNR_KEY],
       S.[CUST_ID],
       S.[SALES_ORGZTN],
       S.[SALES_ORG_DESC],
       S.[DISTRBN_CHNL],
       S.[DISTRBN_CHNL_DESC],
       S.[DIVISION],
       S.[DIVISION_DESC],
       S.[CUST_2_ID],
       S.[PRTNR_FUNCTION],
       S.[PRTNR_FUNCTION_DESC],
       S.[PERSONEL_ID],
       S.[PRTNR_COUNTER],
       S.[ETL_SRC_SYS_CD],
       S.[ETL_EFFECTV_BEGIN_DATE],
       S.[ETL_EFFECTV_END_DATE],
       S.[ETL_CURR_RCD_IND],
       S.[ETL_CREATED_TS],
       S.[ETL_UPDTD_TS],
       S.[VNDR_ID],
       S.[CONTACT_PERSON_ID],
       S.[CUST_DESC_OF_PRTNR],
       S.[DEFAULT_PRTNR]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_CUSTPNR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTPNR_CUR_D] AS T
        ON T.[CUST_PRTNR_KEY] = S.[CUST_PRTNR_KEY]
    WHERE T.[CUST_PRTNR_KEY] IS NULL;

END
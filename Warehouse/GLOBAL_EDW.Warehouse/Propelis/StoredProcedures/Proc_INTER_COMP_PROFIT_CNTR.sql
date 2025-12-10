CREATE   PROCEDURE [Propelis].[Proc_INTER_COMP_PROFIT_CNTR]
AS
BEGIN

    ------------------------------------
    -- UPDATE EXISTING RECORDS
    ------------------------------------
    UPDATE T
    SET
        T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY],
        T.[Inter Comp Profit Center ID] = S.[PROFT_CNTR_ID],
        T.[Inter Comp Profit Center Tax Jurisdiction] = S.[TAX_JRSDCTN],
        T.[Inter Comp Profit Center Valid To Date] = S.[VALID_TO_DATE],
        T.[Inter Comp Profit Center Valid From Date] = S.[VALID_FROM_DATE],
        T.[Inter Comp Profit Center Area] = S.[PROFT_CNTR_AREA],
        T.[Inter Comp Profit Center Generic Mastdf Record Status] = S.[GENERIC_MAST_RCD_STATUS],
        T.[Inter Comp Profit Center Status Description] = S.[STATUS_DESC],
        T.[Inter Comp Profit Center Controlling Area Description] = S.[CONTROLLING_AREA_DESC],
        T.[Inter Comp Profit Center Country Description] = S.[CNTRY_DESC],
        T.[Inter Comp Profit Center Company Description] = S.[CMPNY_DESC],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Inter Comp Profit Center Company Code] = S.[CMPNY_CD],
        T.[Inter Comp Profit Center Long Description] = S.[PROFT_CNTR_LONG_DESC],
        T.[Inter Comp Profit Center Short Description] = S.[PROFT_CNTR_SHORT_DESC],
        T.[Inter Comp Profit Center Lock Indicator] = S.[LOCK_IND],
        T.[Inter Comp Profit Center Name 1] = S.[NAME_1],
        T.[Inter Comp Profit Center Updated User ID] = S.[UPDTD_USR_ID],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[Inter Comp Profit Center Postal Code] = S.[POSTAL_CD],
        T.[Inter Comp Profit Center Country Code] = S.[CNTRY_CD],
        T.[Inter Comp Profit Center Region Code] = S.[REGN_CD],
        T.[Inter Comp Profit Center Created Date] = S.[CREATED_DATE],
        T.[Inter Comp Profit Center Created User ID] = S.[CREATED_USR_ID],
        T.[Inter Comp Profit Center Updated Date] = S.[UPDTD_DATE],
        T.[Inter Comp Profit Center Language Type Code] = S.[LANG_TYP_CD],
        T.[Inter Comp Profit Center Segment ID] = S.[SEG_ID],
        T.[Inter Comp Profit Center Address Line 1] = S.[ADRESS_LINE_1],
        T.[Inter Comp Profit Center Address Line 2] = S.[ADRESS_LINE_2],
        T.[Inter Comp Profit Center City Name] = S.[CITY_NAME],
        T.[Inter Comp Profit Center Distrtict Name] = S.[DISTR_NAME],
        T.[Inter Comp Profit Center Description] = S.[PROFT_CNTR_DESC],
        T.[Legacy Profit Center Code] = S.[LGCY_PROFT_CNTR_CD],
        T.[Legacy Profit Center Description] = S.[LGCY_PROFT_CNTR_DESC],
        T.[Inter Comp Profit Center Controlling Area ID] = S.[CONTROLLING_AREA_ID],
        T.[Inter Comp Profit Center User Responsible ID] = S.[USR_RESPNSBLE_ID],
        T.[Inter Comp Profit Center Person Responsible] = S.[PERSON_RESPNSBLE]
    FROM [GLOBAL_EDW].[Propelis].[INTER_COMP_PROFIT_CNTR] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PROFCTR_CUR_D] AS S
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];


    ------------------------------------
    -- INSERT NEW RECORDS
    ------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[INTER_COMP_PROFIT_CNTR]
    (
        [PROFT_CNTR_KEY],
        [Inter Comp Profit Center ID],
        [Inter Comp Profit Center Tax Jurisdiction],
        [Inter Comp Profit Center Valid To Date],
        [Inter Comp Profit Center Valid From Date],
        [Inter Comp Profit Center Area],
        [Inter Comp Profit Center Generic Mastdf Record Status],
        [Inter Comp Profit Center Status Description],
        [Inter Comp Profit Center Controlling Area Description],
        [Inter Comp Profit Center Country Description],
        [Inter Comp Profit Center Company Description],
        [ETL_UPDTD_TS],
        [Inter Comp Profit Center Company Code],
        [Inter Comp Profit Center Long Description],
        [Inter Comp Profit Center Short Description],
        [Inter Comp Profit Center Lock Indicator],
        [Inter Comp Profit Center Name 1],
        [Inter Comp Profit Center Updated User ID],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [Inter Comp Profit Center Postal Code],
        [Inter Comp Profit Center Country Code],
        [Inter Comp Profit Center Region Code],
        [Inter Comp Profit Center Created Date],
        [Inter Comp Profit Center Created User ID],
        [Inter Comp Profit Center Updated Date],
        [Inter Comp Profit Center Language Type Code],
        [Inter Comp Profit Center Segment ID],
        [Inter Comp Profit Center Address Line 1],
        [Inter Comp Profit Center Address Line 2],
        [Inter Comp Profit Center City Name],
        [Inter Comp Profit Center Distrtict Name],
        [Inter Comp Profit Center Description],
        [Legacy Profit Center Code],
        [Legacy Profit Center Description],
        [Inter Comp Profit Center Controlling Area ID],
        [Inter Comp Profit Center User Responsible ID],
        [Inter Comp Profit Center Person Responsible]
    )
    SELECT
        S.[PROFT_CNTR_KEY],
        S.[PROFT_CNTR_ID],
        S.[TAX_JRSDCTN],
        S.[VALID_TO_DATE],
        S.[VALID_FROM_DATE],
        S.[PROFT_CNTR_AREA],
        S.[GENERIC_MAST_RCD_STATUS],
        S.[STATUS_DESC],
        S.[CONTROLLING_AREA_DESC],
        S.[CNTRY_DESC],
        S.[CMPNY_DESC],
        S.[ETL_UPDTD_TS],
        S.[CMPNY_CD],
        S.[PROFT_CNTR_LONG_DESC],
        S.[PROFT_CNTR_SHORT_DESC],
        S.[LOCK_IND],
        S.[NAME_1],
        S.[UPDTD_USR_ID],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[POSTAL_CD],
        S.[CNTRY_CD],
        S.[REGN_CD],
        S.[CREATED_DATE],
        S.[CREATED_USR_ID],
        S.[UPDTD_DATE],
        S.[LANG_TYP_CD],
        S.[SEG_ID],
        S.[ADRESS_LINE_1],
        S.[ADRESS_LINE_2],
        S.[CITY_NAME],
        S.[DISTR_NAME],
        S.[PROFT_CNTR_DESC],
        S.[LGCY_PROFT_CNTR_CD],
        S.[LGCY_PROFT_CNTR_DESC],
        S.[CONTROLLING_AREA_ID],
        S.[USR_RESPNSBLE_ID],
        S.[PERSON_RESPNSBLE]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PROFCTR_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[INTER_COMP_PROFIT_CNTR] AS T
        ON T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY]
    WHERE T.[PROFT_CNTR_KEY] IS NULL;

END;
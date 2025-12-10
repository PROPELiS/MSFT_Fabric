CREATE     PROCEDURE [Propelis].[Proc_DISCOVERY_DATE] 
AS
BEGIN  
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[DATE_KEY] = S.[DATE_KEY],
        T.[Discovery Date] = S.[THE_DATE],
        T.[Discovery Fiscal Year] = S.[FISCAL_YR],
        T.[Discovery Fiscal Period Number] = S.[FISCAL_PER_NUM],
        T.[Discovery Fiscal Period Code] = S.[FISCAL_PER_CD],
        T.[Discovery Fiscal Quarter Number] = S.[FISCAL_QTR_NUM],
        T.[Discovery Fiscal Quarter Code] = S.[FISCAL_QTR_CD],
        T.[Discovery Calendar Year] = S.[CLNDR_YR],
        T.[Discovery Calendar Month Number] = S.[CLNDR_MNTH_NUM],
        T.[Discovery Calendar Month Code] = S.[CLNDR_MNTH_CD],
        T.[Discovery Calendar Month Description] = S.[CLNDR_MNTH_DESC],
        T.[Discovery Calendar Quarter Number] = S.[CLNDR_QTR_NUM],
        T.[Discovery Calendar Quarter Code] = S.[CLNDR_QTR_CD],
        T.[Discovery Calendar Weekday Number] = S.[CLNDR_WEEKDAY_NUM],
        T.[Discovery Calendar Weekday Name] = S.[CLNDR_WEEKDAY_NAME],
        T.[Discovery Calendar Day In Month] = S.[CLNDR_DAY_IN_MNTH],
        T.[Discovery Calendar Day In Year] = S.[CLNDR_DAY_IN_YR],
        T.[Discovery Calendar Week] = S.[CLNDR_WK],
        T.[Discovery Calendar Month In Quarter Number] = S.[CLNDR_MNTH_IN_QTR_NUM],
        T.[Discovery Calendar Days In Month] = S.[CLNDR_DAYS_IN_MNTH],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[Propelis].[DISCOVERY_DATE] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_DATE_CUR_D] S
        ON T.[DATE_KEY] = S.[DATE_KEY];

    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[DISCOVERY_DATE] (
        [DATE_KEY],
        [Discovery Date],
        [Discovery Fiscal Year],
        [Discovery Fiscal Period Number],
        [Discovery Fiscal Period Code],
        [Discovery Fiscal Quarter Number],
        [Discovery Fiscal Quarter Code],
        [Discovery Calendar Year],
        [Discovery Calendar Month Number],
        [Discovery Calendar Month Code],
        [Discovery Calendar Month Description],
        [Discovery Calendar Quarter Number],
        [Discovery Calendar Quarter Code],
        [Discovery Calendar Weekday Number],
        [Discovery Calendar Weekday Name],
        [Discovery Calendar Day In Month],
        [Discovery Calendar Day In Year],
        [Discovery Calendar Week],
        [Discovery Calendar Month In Quarter Number],
        [Discovery Calendar Days In Month],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS]
    )
    SELECT
        S.[DATE_KEY],
        S.[THE_DATE],
        S.[FISCAL_YR],
        S.[FISCAL_PER_NUM],
        S.[FISCAL_PER_CD],
        S.[FISCAL_QTR_NUM],
        S.[FISCAL_QTR_CD],
        S.[CLNDR_YR],
        S.[CLNDR_MNTH_NUM],
        S.[CLNDR_MNTH_CD],
        S.[CLNDR_MNTH_DESC],
        S.[CLNDR_QTR_NUM],
        S.[CLNDR_QTR_CD],
        S.[CLNDR_WEEKDAY_NUM],
        S.[CLNDR_WEEKDAY_NAME],
        S.[CLNDR_DAY_IN_MNTH],
        S.[CLNDR_DAY_IN_YR],
        S.[CLNDR_WK],
        S.[CLNDR_MNTH_IN_QTR_NUM],
        S.[CLNDR_DAYS_IN_MNTH],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_DATE_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[DISCOVERY_DATE] T
        ON T.[DATE_KEY] = S.[DATE_KEY]
    WHERE T.[DATE_KEY] IS NULL;
END;
CREATE       PROCEDURE [Propelis].[Proc_CREATION_DATE]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET 
        T.[DATE_KEY]	   = S.[DATE_KEY],
        T.[Creation Date]	        = S.[THE_DATE],
        T.[Creation Fiscal Year]	        = S.[FISCAL_YR],
        T.[Creation Fiscal Period Number]	        = S.[FISCAL_PER_NUM],
        T.[Creation Fiscal Period Code]	        = S.[FISCAL_PER_CD],
        T.[Creation Fiscal Quarter Number]	        = S.[FISCAL_QTR_NUM],
        T.[Creation Fiscal Quarter Code]	        = S.[FISCAL_QTR_CD],
        T.[Creation Calendar Year]	        = S.[CLNDR_YR],
        T.[Creation Calendar Month Number]	        = S.[CLNDR_MNTH_NUM],
        T.[Creation Calendar Month Code]	        = S.[CLNDR_MNTH_CD],
        T.[Creation Calendar Month Description]	        = S.[CLNDR_MNTH_DESC],
        T.[Creation Calendar Quarter Number]	        = S.[CLNDR_QTR_NUM],
        T.[Creation Calendar Quarter Code]	        = S.[CLNDR_QTR_CD],
        T.[Creation Calendar Weekday Number]	        = S.[CLNDR_WEEKDAY_NUM],
        T.[Creation Calendar Weekday Name]	        = S.[CLNDR_WEEKDAY_NAME],
        T.[Creation Calendar Day In Month]	        = S.[CLNDR_DAY_IN_MNTH],
        T.[Creation Calendar Day In Year]	        = S.[CLNDR_DAY_IN_YR],
        T.[Creation Calendar Week]	        = S.[CLNDR_WK],
        T.[Creation Calendar Month In Quarter Number]	        = S.[CLNDR_MNTH_IN_QTR_NUM],
        T.[Creation Calendar Days In Month]	        = S.[CLNDR_DAYS_IN_MNTH],
        T.[ETL_SRC_SYS_CD]	        = S.[ETL_SRC_SYS_CD],
        T.[ETL_CREATED_TS]	        = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]	        = S.[ETL_UPDTD_TS],
        T.[Creation Current Day Flag]	        = S.[CURR_DAY_FLG],
        T.[Creation Current Day Minus 1 Flag]	        = S.[CURR_DAY_MINUS_1_FLG],
        T.[Creation Current Calendar Week Flag]	        = S.[CURR_CLNDR_WK_FLG],
        T.[Creation Current Calendar Week Minus 1 Flag]	        = S.[CURR_CLNDR_WK_MINUS_1_FLG],
        T.[Creation Current Month Flag]	        = S.[CURR_MNTH_FLG],
        T.[Creation Current Month Minus 1 Flag]	        = S.[CURR_MNTH_MINUS_1_FLG],
        T.[Creation Current Month Minus 12 Flag]	        = S.[CURR_MNTH_MINUS_12_FLG],
        T.[Creation Current Quarter Flag]	        = S.[CURR_QTR_FLG],
        T.[Creation Current Quarter Minus 1 Flag]	        = S.[CURR_QTR_MINUS_1_FLG],
        T.[Creation Current Quarter Minus 4 Flag]	        = S.[CURR_QTR_MINUS_4_FLG],
        T.[Creation Current Calendar Year Flag]	        = S.[CURR_CLNDR_YR_FLG],
        T.[Creation Current Calendar Year Minus 1 Flag]	        = S.[CURR_CLNDR_YR_MINUS_1_FLG],
        T.[Creation Current Calendar Year Minus 2 Flag]	        = S.[CURR_CLNDR_YR_MINUS_2_FLG],
        T.[Creation Current Fiscal Year Flag]	        = S.[CURR_FISCAL_YR_FLG],
        T.[Creation Current Fiscal Year Minus 1 Flag]	        = S.[CURR_FISCAL_YR_MINUS_1_FLG],
        T.[Creation Current Fiscal Year Minus 2 Flag]	        = S.[CURR_FISCAL_YR_MINUS_2_FLG],
        T.[Creation Current Calendar Year Plus 1 Flag]	        = S.[CURR_CLNDR_YR_PLUS_1_FLG],
        T.[Creation Current Fiscal Year Plus 1 Flag]	        = S.[CURR_FISCAL_YR_PLUS_1_FLG],
        T.[Creation Day In Fiscal Year]	        = S.[DAY_IN_FISCAL_YR],
        T.[Creation Date Is Weekday]	        = S.[IS_WEEKDAY],
        T.[Creation Fiscal Week]	        = S.[FISCAL_WK],
        T.[Creation Current Fiscal Week Flag]	        = S.[CURR_FISCAL_WK_FLG],
        T.[Creation Current Fiscal Week Minus 1 Flag]	        = S.[CURR_FISCAL_WK_MINUS_1_FLG],
        T.[Creation Fiscal Weekday Number]	        = S.[FISCAL_WEEKDAY_NUM],
        T.[Creation Fiscal Weekday Name]	        = S.[FISCAL_WEEKDAY_NAME],
        T.[Creation Current Month Minus 2 Flag]	        = S.[CURR_MNTH_MINUS_2_FLG],
        T.[Creation Current Fiscal Week Minus 2 Flag]	        = S.[CURR_FISCAL_WK_MINUS_2_FLG],
        T.[Creation Current Fiscal Week Minus 6 Flag]	        = S.[CURR_FISCAL_WK_MINUS_6_FLG],
        T.[Creation Current Fiscal Week Minus 3 Flag]	        = S.[CURR_FISCAL_WK_MINUS_3_FLG],
        T.[Creation Current Fiscal Week Minus 4 Flag]	        = S.[CURR_FISCAL_WK_MINUS_4_FLG],
        T.[Creation Current Fiscal Week Minus 5 Flag]	        = S.[CURR_FISCAL_WK_MINUS_5_FLG],
        T.[Creation Current Fiscal Week Minus 7 Flag]	        = S.[CURR_FISCAL_WK_MINUS_7_FLG]

    FROM [GLOBAL_EDW].[Propelis].[CREATION_DATE] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_DATE_CUR_D] AS S
        ON T.[DATE_KEY] = S.[DATE_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[Propelis].[CREATION_DATE] 
    (
       [DATE_KEY]
       ,[Creation Date]
       ,[Creation Fiscal Year]
       ,[Creation Fiscal Period Number]
       ,[Creation Fiscal Period Code]
       ,[Creation Fiscal Quarter Number]
       ,[Creation Fiscal Quarter Code]
       ,[Creation Calendar Year]
       ,[Creation Calendar Month Number]
       ,[Creation Calendar Month Code]
       ,[Creation Calendar Month Description]
       ,[Creation Calendar Quarter Number]
       ,[Creation Calendar Quarter Code]
       ,[Creation Calendar Weekday Number]
       ,[Creation Calendar Weekday Name]
       ,[Creation Calendar Day In Month]
       ,[Creation Calendar Day In Year]
       ,[Creation Calendar Week]
       ,[Creation Calendar Month In Quarter Number]
       ,[Creation Calendar Days In Month]
       ,[ETL_SRC_SYS_CD]
       ,[ETL_CREATED_TS]
       ,[ETL_UPDTD_TS]
       ,[Creation Current Day Flag]
       ,[Creation Current Day Minus 1 Flag]
       ,[Creation Current Calendar Week Flag]
       ,[Creation Current Calendar Week Minus 1 Flag]
       ,[Creation Current Month Flag]
       ,[Creation Current Month Minus 1 Flag]
       ,[Creation Current Month Minus 12 Flag]
       ,[Creation Current Quarter Flag]
       ,[Creation Current Quarter Minus 1 Flag]
       ,[Creation Current Quarter Minus 4 Flag]
       ,[Creation Current Calendar Year Flag]
       ,[Creation Current Calendar Year Minus 1 Flag]
       ,[Creation Current Calendar Year Minus 2 Flag]
       ,[Creation Current Fiscal Year Flag]
       ,[Creation Current Fiscal Year Minus 1 Flag]
       ,[Creation Current Fiscal Year Minus 2 Flag]
       ,[Creation Current Calendar Year Plus 1 Flag]
       ,[Creation Current Fiscal Year Plus 1 Flag]
       ,[Creation Day In Fiscal Year]
       ,[Creation Date Is Weekday]
       ,[Creation Fiscal Week]
       ,[Creation Current Fiscal Week Flag]
       ,[Creation Current Fiscal Week Minus 1 Flag]
       ,[Creation Fiscal Weekday Number]
       ,[Creation Fiscal Weekday Name]
       ,[Creation Current Month Minus 2 Flag]
       ,[Creation Current Fiscal Week Minus 2 Flag]
       ,[Creation Current Fiscal Week Minus 6 Flag]
       ,[Creation Current Fiscal Week Minus 3 Flag]
       ,[Creation Current Fiscal Week Minus 4 Flag]
       ,[Creation Current Fiscal Week Minus 5 Flag]
       ,[Creation Current Fiscal Week Minus 7 Flag]
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
        S.[ETL_UPDTD_TS],
        S.[CURR_DAY_FLG],
        S.[CURR_DAY_MINUS_1_FLG],
        S.[CURR_CLNDR_WK_FLG],
        S.[CURR_CLNDR_WK_MINUS_1_FLG],
        S.[CURR_MNTH_FLG],
        S.[CURR_MNTH_MINUS_1_FLG],
        S.[CURR_MNTH_MINUS_12_FLG],
        S.[CURR_QTR_FLG],
        S.[CURR_QTR_MINUS_1_FLG],
        S.[CURR_QTR_MINUS_4_FLG],
        S.[CURR_CLNDR_YR_FLG],
        S.[CURR_CLNDR_YR_MINUS_1_FLG],
        S.[CURR_CLNDR_YR_MINUS_2_FLG],
        S.[CURR_FISCAL_YR_FLG],
        S.[CURR_FISCAL_YR_MINUS_1_FLG],
        S.[CURR_FISCAL_YR_MINUS_2_FLG],
        S.[CURR_CLNDR_YR_PLUS_1_FLG],
        S.[CURR_FISCAL_YR_PLUS_1_FLG],
        S.[DAY_IN_FISCAL_YR],
        S.[IS_WEEKDAY],
        S.[FISCAL_WK],
        S.[CURR_FISCAL_WK_FLG],
        S.[CURR_FISCAL_WK_MINUS_1_FLG],
        S.[FISCAL_WEEKDAY_NUM],
        S.[FISCAL_WEEKDAY_NAME],
        S.[CURR_MNTH_MINUS_2_FLG],
        S.[CURR_FISCAL_WK_MINUS_2_FLG],
        S.[CURR_FISCAL_WK_MINUS_6_FLG],
        S.[CURR_FISCAL_WK_MINUS_3_FLG],
        S.[CURR_FISCAL_WK_MINUS_4_FLG],
        S.[CURR_FISCAL_WK_MINUS_5_FLG],
        S.[CURR_FISCAL_WK_MINUS_7_FLG]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_DATE_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CREATION_DATE] AS T
        ON T.[DATE_KEY] = S.[DATE_KEY]
    WHERE T.[DATE_KEY] IS NULL;
END;
-- Auto Generated (Do not modify) FFB49B81CEEAE2E3F546E8403F1565F7DE32C81640B003CD18217888A5A21610
CREATE   view [Propelis].[V_CREATION_DATE] 
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
as select 
        [DATE_KEY],
        [Date],
        [Date Fiscal Year],
        [FISCAL_PER_NUM],
        [Date Fiscal Period Code],
        [Date Fiscal Quarter Number],
        [Date Fiscal Quarter Code],
        [Date Calendar Year],
        [Date Calendar Month Number],
        [Date Calendar Month Code],
        [Date Calendar Month Desc],
        [CLNDR_QTR_NUM],
        [Date Calendar Quarter Code],
        [Date Calendar Weekday Number],
        [Date Calendar Weekday Name],
        [Date Calendar Day in Month],
        [Date Calendar Day in Year],
        [Date Calendar Week],
        [Date Calendar Month in Quarter Number],
        [Date Calendar Days in Month],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Date Current Day Flag],
        [Date Current Day Minus 1 Flag],
        [Date Current Calendar Week Flag],
        [Date Current Calendar Week Minus 1 Flag],
        [Date Current Month Flag],
        [Date Current Month Minus 1 Flag],
        [Date Current Month Minus 12 Flag],
        [Date Current Quarter Flag],
        [Date Current Quarter Minus 1 Flag],
        [Date Current Quarter Minus 4 Flag],
        [Date Current Calendar Year Flag],
        [Date Current Calendar Year Minus 1 Flag],
        [Date Current Calendar Year Minus 2 Flag],
        [Date Current Fiscal Year Flag],
        [Date Current Fiscal Year Minus 1 Flag],
        [Date Current Fiscal Year Minus 2 Flag],
        [Date Current Calendar Year Plus 1 Flag],
        [Date Current Fiscal Year Plus 1 Flag],
        [Date Day in Fiscal Year],
        [Date Date Is Weekday],
        [Date Fiscal Week],
        [Date Current Fiscal Week Flag],
        [Date Current Fiscal Week Minus 1 Flag],
        [Date Fiscal Weekday Number],
        [Date Fiscal Weekday Name],
        [Date Current Month Minus 2 Flag],
        [Date Current Fiscal Week Minus 2 Flag],
        [Date Current Fiscal Week Minus 6 Flag],
        [Date Current Fiscal Week Minus 3 Flag],
        [Date Current Fiscal Week Minus 4 Flag],
        [Date Current Fiscal Week Minus 5 Flag],
        [Date Current Fiscal Week Minus 7 Flag]
   from  [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D];
-- Auto Generated (Do not modify) 0EC80CFCC18A738891D600B5E52FCD1E31C6A7FC050AE5CEBF6896F22A7A8E78
CREATE   VIEW [Propelis].[V_CUST_CONTACT](
    [CUST_CONTACT_KEY],
    [Customer Contact Person ID],
    [Customer Contact Customer ID],
    [Customer Contact First Name],
    [Customer Contact Name],
    [Customer Contact Contact Person Dept],
    [Customer Contact Contact Person Dept Name],
    [ETL_SRC_SYS_CD],
    [ETL_EFFECTV_BEGIN_DATE],
    [ETL_EFFECTV_END_DATE],
    [ETL_CURR_RCD_IND],
    [ETL_CREATED_TS],
    [ETL_UPDTD_TS]
)AS
SELECT
    [CUST_CONTACT_KEY],
    [Customer Contact Person ID],
    [Customer Contact Customer ID],
    [Customer Contact First Name],
    [Customer Contact Name],
    [Customer Contact Contact Person Dept],
    [Customer Contact Contact Person Dept Name],
    [ETL_SRC_SYS_CD],
    [ETL_EFFECTV_BEGIN_DATE],
    [ETL_EFFECTV_END_DATE],
    [ETL_CURR_RCD_IND],
    [ETL_CREATED_TS],
    [ETL_UPDTD_TS]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D];
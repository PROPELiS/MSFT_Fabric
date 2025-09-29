-- Auto Generated (Do not modify) 39C38AE8CE6470582D6132FCC7B0E63B9A89D5584F7B1D5011E347252D0F8926
CREATE   VIEW [Propelis].[V_CUSTOMER_PROJECT_MANAGER](
    [CUST_CONTACT_KEY],
    [Customer Project Manager Contact Person ID],
    [Customer Project Manager Customer ID],
    [Customer Project Manager First Name],
    [Customer Project Manager Name],
    [Customer Project Manager Contact Person Dept],
    [Customer Project Manager Contact Person Dept Name],
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
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCNT_CUR_D]
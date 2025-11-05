CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_FI_BPCDTAU_CUR_D]
AS
BEGIN 

UPDATE T 
SET 
T.[ACCT_KEY] = S.[ACCT_KEY],
T.[Audit Trail] = S.[Audit Trail],
T.[Currency] = S.[Currency],
T.[Custom1] = S.[Custom1],
T.[ENTITY_KEY] = S.[ENTITY_KEY],
T.[Flow] = S.[Flow],
T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY],
T.[INTER_CMPNY_PROFT_CNTR_KEY] = S.[INTER_CMPNY_PROFT_CNTR_KEY],
T.[INTER_CMPNY_KEY] = S.[INTER_CMPNY_KEY],
T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY],
T.[Project] = S.[Project],
T.[Scope] = S.[Scope],
T.[Version] = S.[Version],
T.[Fiscal Year] = S.[Fiscal Year],
T.[FYP] = S.[FYP],
T.[Signdata Amount] = S.[Signdata Amount],
T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
T.[Account ID] = S.[Account ID],
T.[Entity ID] = S.[Entity ID],
T.[Functional Area ID] = S.[Functional Area ID],
T.[Inter Company Profit Center ID] = S.[Inter Company Profit Center ID],
T.[Profit Center ID] = S.[Profit Center ID],
T.[Inter Company ID] = S.[Inter Company ID],
T.[Signdata Amt - Current] = S.[Signdata Amt - Current]
FROM [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_BPCDTAU_CUR_D] T 
INNER JOIN [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_BPCDTAU_CUR_D] S 
ON T.[ACCT_KEY] = S.[ACCT_KEY]
AND T.[ENTITY_KEY] = S.[ENTITY_KEY]
AND T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY]
AND T.[INTER_CMPNY_PROFT_CNTR_KEY] = S.[INTER_CMPNY_PROFT_CNTR_KEY]
AND T.[INTER_CMPNY_KEY] = S.[INTER_CMPNY_KEY]
AND T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY];

INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_BPCDTAU_CUR_D] 
(
[ACCT_KEY],
[Audit Trail],
[Currency],
[Custom1],
[ENTITY_KEY],
[Flow],
[FUNCTNL_AREA_KEY],
[INTER_CMPNY_PROFT_CNTR_KEY],
[INTER_CMPNY_KEY],
[PROFT_CNTR_KEY],
[Project],
[Scope],
[Version],
[Fiscal Year],
[FYP],
[Signdata Amount],
[ETL_SRC_SYS_CD],
[ETL_CREATED_TS],
[ETL_UPDTD_TS],
[Account ID],
[Entity ID],
[Functional Area ID],
[Inter Company Profit Center ID],
[Profit Center ID],
[Inter Company ID],
[Signdata Amt - Current]
)
SELECT 
S.[ACCT_KEY],
S.[Audit Trail],
S.[Currency],
S.[Custom1],
S.[ENTITY_KEY],
S.[Flow],
S.[FUNCTNL_AREA_KEY],
S.[INTER_CMPNY_PROFT_CNTR_KEY],
S.[INTER_CMPNY_KEY],
S.[PROFT_CNTR_KEY],
S.[Project],
S.[Scope],
S.[Version],
S.[Fiscal Year],
S.[FYP],
S.[Signdata Amount],
S.[ETL_SRC_SYS_CD],
S.[ETL_CREATED_TS],
S.[ETL_UPDTD_TS],
S.[Account ID],
S.[Entity ID],
S.[Functional Area ID],
S.[Inter Company Profit Center ID],
S.[Profit Center ID],
S.[Inter Company ID],
S.[Signdata Amt - Current]
FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_FI_BPCDTAU_CUR_D] S 
INNER JOIN [GLOBAL_EDW].[Propelis].[EDW_T_FACT_FI_BPCDTAU_CUR_D] T 
ON S.[ACCT_KEY] = T.[ACCT_KEY]
AND S.[ENTITY_KEY] = T.[ENTITY_KEY]
AND S.[FUNCTNL_AREA_KEY] = T.[FUNCTNL_AREA_KEY]
AND S.[INTER_CMPNY_PROFT_CNTR_KEY] = T.[INTER_CMPNY_PROFT_CNTR_KEY]
AND S.[INTER_CMPNY_KEY] = T.[INTER_CMPNY_KEY]
AND S.[PROFT_CNTR_KEY] = T.[PROFT_CNTR_KEY];

END;
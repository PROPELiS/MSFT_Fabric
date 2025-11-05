CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_PRD_SERVRES_CUR_D]
AS
BEGIN

UPDATE T 
SET 
T.[SERVC_ORDER_KEY] = S.[SERVC_ORDER_KEY],
T.[Service Order] = S.[Service Order],
T.[Operation Activity ID] = S.[Operation Activity ID],
T.[Operation Task List ID] = S.[Operation Task List ID],
T.[Operation Counter ID] = S.[Operation Counter ID],
T.[Number of Reservation] = S.[Number of Reservation],
T.[Record Type] = S.[Record Type],
T.[REQ_DATE_KEY] = S.[REQ_DATE_KEY],
T.[GL_KEY] = S.[GL_KEY],
T.[MATRL_KEY] = S.[MATRL_KEY],
T.[BOM Item Number] = S.[BOM Item Number],
T.[MOVEMNT_TYP] = S.[MOVEMNT_TYP],
T.[Fixed Quantity] = S.[Fixed Quantity],
T.[Requirement Type] = S.[Requirement Type],
T.[Base UoM] = S.[Base UoM],
T.[Unit Of Entry] = S.[Unit Of Entry],
T.[Item Number of Reservation] = S.[Item Number of Reservation],
T.[Price Unit] = S.[Price Unit],
T.[Quantity In Unit Of Entry] = S.[Quantity In Unit Of Entry],
T.[Requirement Quantity] = S.[Requirement Quantity],
T.[Value Withdrawn Amount] = S.[Value Withdrawn Amount],
T.[PLNT_KEY] = S.[PLNT_KEY],
T.[Debit Credit Indicator] = S.[Debit Credit Indicator],
T.[DOCMT_CRNCY_KEY] = S.[DOCMT_CRNCY_KEY],
T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
T.[Quantity Withdrawn] = S.[Quantity Withdrawn],
T.[Price in Document Currency] = S.[Price in Document Currency]
FROM [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVRES_CUR_D] T 
INNER JOIN [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVRES_CUR_D] S
ON T.[SERVC_ORDER_KEY] = S.[SERVC_ORDER_KEY]
AND T.[REQ_DATE_KEY] = S.[REQ_DATE_KEY]
AND T.[MATRL_KEY] = S.[MATRL_KEY]
AND T.[PLNT_KEY] = S.[PLNT_KEY]
AND T.[DOCMT_CRNCY_KEY] = S.[DOCMT_CRNCY_KEY]
AND T.[GL_KEY] = S.[GL_KEY];

INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVRES_CUR_D]
(
[SERVC_ORDER_KEY],
[Service Order],
[Operation Activity ID],
[Operation Task List ID],
[Operation Counter ID],
[Number of Reservation],
[Record Type],
[REQ_DATE_KEY],
[GL_KEY],
[MATRL_KEY],
[BOM Item Number],
[MOVEMNT_TYP],
[Fixed Quantity],
[Requirement Type],
[Base UoM],
[Unit Of Entry],
[Item Number of Reservation],
[Price Unit],
[Quantity In Unit Of Entry],
[Requirement Quantity],
[Value Withdrawn Amount],
[PLNT_KEY],
[Debit Credit Indicator],
[DOCMT_CRNCY_KEY],
[ETL_SRC_SYS_CD],
[ETL_CREATED_TS],
[ETL_UPDTD_TS],
[Quantity Withdrawn],
[Price in Document Currency]
)

SELECT 

S.[SERVC_ORDER_KEY],
S.[Service Order],
S.[Operation Activity ID],
S.[Operation Task List ID],
S.[Operation Counter ID],
S.[Number of Reservation],
S.[Record Type],
S.[REQ_DATE_KEY],
S.[GL_KEY],
S.[MATRL_KEY],
S.[BOM Item Number],
S.[MOVEMNT_TYP],
S.[Fixed Quantity],
S.[Requirement Type],
S.[Base UoM],
S.[Unit Of Entry],
S.[Item Number of Reservation],
S.[Price Unit],
S.[Quantity In Unit Of Entry],
S.[Requirement Quantity],
S.[Value Withdrawn Amount],
S.[PLNT_KEY],
S.[Debit Credit Indicator],
S.[DOCMT_CRNCY_KEY],
S.[ETL_SRC_SYS_CD],
S.[ETL_CREATED_TS],
S.[ETL_UPDTD_TS],
S.[Quantity Withdrawn],
S.[Price in Document Currency]

FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVRES_CUR_D] S 
LEFT JOIN [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVRES_CUR_D] T 
ON S.[SERVC_ORDER_KEY] = T.[SERVC_ORDER_KEY]
AND S.[REQ_DATE_KEY] = T.[REQ_DATE_KEY]
AND S.[MATRL_KEY] = T.[MATRL_KEY]
AND S.[PLNT_KEY] = T.[PLNT_KEY]
AND S.[DOCMT_CRNCY_KEY] = T.[DOCMT_CRNCY_KEY]
AND S.[GL_KEY] = T.[GL_KEY];

END;
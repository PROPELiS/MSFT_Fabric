CREATE PROCEDURE [Propelis].[Proc_TEMP_EDW_T_FACT_PRD_SERVRES_CUR_D]
AS
BEGIN

DELETE FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVRES_CUR_D];


INSERT INTO [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVRES_CUR_D]
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

Fact.[SERVC_ORDER_KEY],
Fact.[SERVC_ORDER],
Fact.[OPERATION_ACTIVITY_ID],
Fact.[OPERATION_TASK_LIST_ID],
Fact.[OPERATION_COUNTER_ID],
Fact.[NUM_OF_RESRVTN],
Fact.[RCD_TYP],
Fact.[REQ_DATE_KEY],
Fact.[GL_KEY],
Fact.[MATRL_KEY],
Fact.[BOM_ITM_NUM],
Fact.[MOVEMNT_TYP],
Fact.[FIXED_QTY],
Fact.[REQ_TYP],
Fact.[BASE_UOM],
Fact.[UNIT_OF_ENTRY],
Fact.[ITM_NUM_OF_RESRVTN],
Fact.[PRICE_UNIT],
Fact.[QTY_IN_UNIT_OF_ENTRY],
Fact.[REQ_QTY],
Fact.[VAL_WTHDRN_AMT],
Fact.[PLNT_KEY],
Fact.[DEBIT_CREDIT_IND],
Fact.[DOCMT_CRNCY_KEY],
Fact.[ETL_SRC_SYS_CD],
Fact.[ETL_CREATED_TS],
Fact.[ETL_UPDTD_TS],
Fact.[QTY_WTHDRN],
Fact.[PRICE_IN_DOCMT_CRNCY]
FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_F_PRD_SERVRES_CUR_D] Fact
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRD_SERVORD_CUR_D] a
ON Fact.[SERVC_ORDER_KEY] = a.[SERVC_ORDER_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[REQUIREMENT_DATE] b
ON Fact.[REQ_DATE_KEY] = b.[DATE_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] c
ON Fact.[MATRL_KEY] = c.[MATRL_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANT] d 
ON Fact.[PLNT_KEY] = d.[PLNT_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D] e 
ON Fact.[DOCMT_CRNCY_KEY] = e.[CRNCY_KEY]
LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_GENLDGR_CUR_D] f 
ON Fact.[GL_KEY] = f.[GL_KEY]
LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANT_REGION ] g
ON d.[Plant ID] = g.[PLANT];


END;
-- Auto Generated (Do not modify) FC0005241CFF24C406DFC709CCD85E009C0F8FE858B8E7719BE0A65784B8903C
CREATE     view [Propelis].[V_PRODUCING_PLANT_REGION](
  [Producing Plant Business Function],
[Producing Plant Region],
[Producing Plant]
) AS
SELECT
    [Business Function],
    [SGK Plant Region],
    [PLANT] 
FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
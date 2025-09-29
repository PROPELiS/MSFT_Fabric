-- Auto Generated (Do not modify) E9619F81C5155D299F2DB674659168479EFA518A6763E8B31134082682863310
CREATE   view [Propelis].[V_PLANT_REGION](
    [Plant Business Function],
    [Plant Region],
    [PLANT]
) AS
SELECT
    [Business Function],
    [SGK Plant Region],
    [PLANT] 
FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
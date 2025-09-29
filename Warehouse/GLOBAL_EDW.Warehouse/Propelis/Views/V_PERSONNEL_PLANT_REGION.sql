-- Auto Generated (Do not modify) 544BA2C41FFF2C33AB4CD45982C3A6F8981B75ED088437DE13219DA08C1F86C3
CREATE   view [Propelis].[V_PERSONNEL_PLANT_REGION](
   [Personnel Business Function],
   [Personnel Plant Region],
   [Personnel Plant]  
) AS
SELECT
    [Business Function],
    [SGK Plant Region],
    [PLANT] 
FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
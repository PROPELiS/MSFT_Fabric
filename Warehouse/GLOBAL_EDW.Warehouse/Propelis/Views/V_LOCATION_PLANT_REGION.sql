-- Auto Generated (Do not modify) 3580EA2883CB35FAD330FEC21DDB13A045F4FCD3AE5EF7B330F15A2C99E36A59
CREATE     view [Propelis].[V_LOCATION_PLANT_REGION](
   [Location Plant Business Function],
[Location Plant Region],
[Location Plant]
) AS
SELECT
    [Business Function],
    [SGK Plant Region],
    [PLANT] 
FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
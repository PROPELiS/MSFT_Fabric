-- Auto Generated (Do not modify) 9BA6EBD025175D7B01D9249183C02982ED7F8DD881842EB79874AD8497BF00B9

create view [Propelis].[V_MAINTENANCE_PLANT_REGION](
[Maintenance Plant Business Function],
	[Maintenance Plant Region],
	[Maintenance Plant]
) as
SELECT 
   [Business Function],
	[SGK Plant Region],
	[PLANT] [varchar]
  FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
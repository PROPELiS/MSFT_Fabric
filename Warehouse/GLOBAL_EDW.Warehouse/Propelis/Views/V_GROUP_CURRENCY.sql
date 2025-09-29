-- Auto Generated (Do not modify) AAA913E439C1D9CDC09CCD596E2140DE8658DF39C74D3A0AE8F738220CE8186F

create view [Propelis].[V_GROUP_CURRENCY](
	[CRNCY_KEY],
	[Group Code Currency],
	[ETL_SRC_SYS_CD],
	[ETL_EFFECTV_BEGIN_DATE],
	[ETL_EFFECTV_END_DATE],
	[ETL_CURR_RCD_IND],
	[ETL_CREATED_TS],
	[ETL_UPDTD_TS],
	[Group Currency Short Description],
	[Group Currency Long Description]
) as
SELECT 
    [CRNCY_KEY],
	[Currency Code],
	[ETL_SRC_SYS_CD],
	[ETL_EFFECTV_BEGIN_DATE],
	[ETL_EFFECTV_END_DATE],
	[ETL_CURR_RCD_IND],
	[ETL_CREATED_TS],
	[ETL_UPDTD_TS]L,
	[Currency Short Description],
	[Currency Long Description]
  FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D];
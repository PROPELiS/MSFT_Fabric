-- Auto Generated (Do not modify) 3B52E0B162EC18BD3ABDB5C97A75474A75BB46AC1B56C41DDB3F8DCA8684DE14

create view [Propelis].[V_COMPANY_CURRENCY](
	[CRNCY_KEY],
	[Company Currency],
	[ETL_SRC_SYS_CD],
	[ETL_EFFECTV_BEGIN_DATE],
	[ETL_EFFECTV_END_DATE],
	[ETL_CURR_RCD_IND],
	[ETL_CREATED_TS],
	[ETL_UPDTD_TS],
	[Company Currency Short Description],
	[Company Currency Long Description]
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
-- Auto Generated (Do not modify) 5456F1FA110974F6FBAB771BA755F8719E16A33AD65BADD31DCC4F8CC82F80F6



create view [Propelis].[V_DOCUMENT_CURRENCY](
	[CRNCY_KEY],
	[Document Currency Code],
	[ETL_SRC_SYS_CD],
	[ETL_EFFECTV_BEGIN_DATE],
	[ETL_EFFECTV_END_DATE],
	[ETL_CURR_RCD_IND],
	[ETL_CREATED_TS],
	[ETL_UPDTD_TS],
	[Document Currency Short Description],
	[Document Currency Long Description]
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
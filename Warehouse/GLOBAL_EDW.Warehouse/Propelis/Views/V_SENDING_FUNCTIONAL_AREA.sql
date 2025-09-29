-- Auto Generated (Do not modify) 3642ECA24F2CC04FD09A6ED409D61BED269E4489649E1AADAAF0B491902564BE
create view [Propelis].[V_SENDING_FUNCTIONAL_AREA](
[FUNCTNL_AREA_KEY] 						,
[Sending Functional Area ID] 			,
[Sending Functional Area Description] 	,
[ETL_SRC_SYS_CD] 						,
[ETL_EFFECTV_BEGIN_DATE] 				,
[ETL_EFFECTV_END_DATE] 					,
[ETL_CURR_RCD_IND] 						,
[ETL_CREATED_TS] 						,
[ETL_UPDTD_TS] 							

) as
SELECT
	[FUNCTNL_AREA_KEY] 					,
	[Functional Area ID] 			    ,
	[Functional Area Description] 	    ,
	[ETL_SRC_SYS_CD] 				    ,
	[ETL_EFFECTV_BEGIN_DATE] 		    ,
	[ETL_EFFECTV_END_DATE] 			    ,
	[ETL_CURR_RCD_IND] 				    ,
	[ETL_CREATED_TS] 				    ,
	[ETL_UPDTD_TS] 					
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D];
-- Auto Generated (Do not modify) 4517ECE102AC362863A4726843956E2DE075AFBE2E603C3A10C749EC27E49B42
create view [Propelis].[V_RECEIVING_FUNCTIONAL_AREA](
[FUNCTNL_AREA_KEY] 						,
[Receiving Functional Area ID] 			,
[Receiving Functional Area Description] 	,
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
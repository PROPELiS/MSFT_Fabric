CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_MST_PERAREA_CUR_D]
AS
BEGIN
	UPDATE T
	SET
	T.[PERSONEL_AREA_KEY]     	= S.[PERSONEL_AREA_KEY]     	,
	T.[Personnel Area]        	= S.[PERSONEL_AREA]             ,
	T.[Country Grouping]      	= S.[CNTRY_GROUPING]            ,
	T.[Company Code]          	= S.[CMPNY_CD]                  ,
	T.[Personnel Area Text]   	= S.[PERSONEL_AREA_TEXT]        ,
	T.[House Number]          	= S.[HOUSE_NUM]                 ,
	T.[PO Box]                	= S.[PO_BOX]                    ,
	T.[City]                  	= S.[CITY]                      ,
	T.[Country]               	= S.[CNTRY]                     ,
	T.[Region]                	= S.[REGN]                      ,
	T.[County]                	= S.[CNTY]                      ,
	T.[Address]               	= S.[ADDR]                      ,
	T.[ETL_SRC_SYS_CD]        	= S.[ETL_SRC_SYS_CD]            ,
	T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE]    ,
	T.[ETL_EFFECTV_END_DATE]  	= S.[ETL_EFFECTV_END_DATE]      ,
	T.[ETL_CURR_RCD_IND]      	= S.[ETL_CURR_RCD_IND]          ,
	T.[ETL_CREATED_TS]        	= S.[ETL_CREATED_TS]            ,
	T.[ETL_UPDTD_TS]          	= S.[ETL_UPDTD_TS]				


FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERAREA_CUR_D] T
INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERAREA_CUR_D] S
    ON T.[PERSONEL_AREA_KEY] = S.[PERSONEL_AREA_KEY];

-- Step 2: Insert new records from [GLOBAL_EDW_MIRROR] to [GLOBAL_EDW] where PERSONEL_AREA_KEY does not exist

    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERAREA_CUR_D] 
	(
	T.[PERSONEL_AREA_KEY]     	,
	T.[Personnel Area]          ,
	T.[Country Grouping]        ,
	T.[Company Code]            ,
	T.[Personnel Area Text]     ,
	T.[House Number]            ,
	T.[PO Box]                  ,
	T.[City]                    ,
	T.[Country]                 ,
	T.[Region]                  ,
	T.[County]                  ,
	T.[Address]                 ,
	T.[ETL_SRC_SYS_CD]          ,
	T.[ETL_EFFECTV_BEGIN_DATE]  ,
	T.[ETL_EFFECTV_END_DATE]    ,
	T.[ETL_CURR_RCD_IND]        ,
	T.[ETL_CREATED_TS]          ,
	T.[ETL_UPDTD_TS]            

)
SELECT 

S.[PERSONEL_AREA_KEY]     		,
S.[PERSONEL_AREA]               ,
S.[CNTRY_GROUPING]              ,
S.[CMPNY_CD]                    ,
S.[PERSONEL_AREA_TEXT]          ,
S.[HOUSE_NUM]                   ,
S.[PO_BOX]                      ,
S.[CITY]                        ,
S.[CNTRY]                       ,
S.[REGN]                        ,
S.[CNTY]                        ,
S.[ADDR]                        ,
S.[ETL_SRC_SYS_CD]              ,
S.[ETL_EFFECTV_BEGIN_DATE]      ,
S.[ETL_EFFECTV_END_DATE]        ,
S.[ETL_CURR_RCD_IND]            ,
S.[ETL_CREATED_TS]              ,
S.[ETL_UPDTD_TS]                



FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_PERAREA_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERAREA_CUR_D] T
        ON T.[PERSONEL_AREA_KEY] = S.[PERSONEL_AREA_KEY]
    WHERE T.[PERSONEL_AREA_KEY] IS NULL;

END
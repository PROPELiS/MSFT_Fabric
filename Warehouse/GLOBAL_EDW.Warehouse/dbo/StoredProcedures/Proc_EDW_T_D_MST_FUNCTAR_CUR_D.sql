CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_MST_FUNCTAR_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Functional Area ID]                   = S.[FUNCTNL_AREA_ID],
        T.[Functional Area Description]          = S.[FUNCTNL_AREA_DESC],
        T.[ETL_SRC_SYS_CD]                       = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]               = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                 = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                     = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                       = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                         = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_FUNCTAR_CUR_D] S
        ON T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] (
        [FUNCTNL_AREA_KEY],
        [Functional Area ID],                             
		[Functional Area Description],                               
		[ETL_SRC_SYS_CD],                        
		[ETL_EFFECTV_BEGIN_DATE],                
		[ETL_EFFECTV_END_DATE],                  
		[ETL_CURR_RCD_IND],                      
		[ETL_CREATED_TS],                        
		[ETL_UPDTD_TS]           	
    )
    SELECT
        S.[FUNCTNL_AREA_KEY],
        S.[FUNCTNL_AREA_ID],
        S.[FUNCTNL_AREA_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_MST_FUNCTAR_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] T
        ON T.[FUNCTNL_AREA_KEY] = S.[FUNCTNL_AREA_KEY]
    WHERE T.[FUNCTNL_AREA_KEY] IS NULL;
END
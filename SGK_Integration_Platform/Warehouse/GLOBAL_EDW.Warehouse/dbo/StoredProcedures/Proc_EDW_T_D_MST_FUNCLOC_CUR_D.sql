CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_FUNCLOC_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Functional Location]                  = S.[FUNCTNL_LOC],
        T.[Functional Location Description]      = S.[FUNCTNL_LOC_DESC],
        T.[ETL_SRC_SYS_CD]                       = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]               = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                 = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                     = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                       = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                         = S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] S
        ON T.[FUNCTNL_LOC_KEY] = S.[FUNCTNL_LOC_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] (
        [FUNCTNL_LOC_KEY],
        [Functional Location],                             
		[Functional Location Description],                               
		[ETL_SRC_SYS_CD],                        
		[ETL_EFFECTV_BEGIN_DATE],                
		[ETL_EFFECTV_END_DATE],                  
		[ETL_CURR_RCD_IND],                      
		[ETL_CREATED_TS],                        
		[ETL_UPDTD_TS]           	
    )
    SELECT
        S.[FUNCTNL_LOC_KEY],
        S.[FUNCTNL_LOC],
        S.[FUNCTNL_LOC_DESC],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] T
        ON T.[FUNCTNL_LOC_KEY] = S.[FUNCTNL_LOC_KEY]
    WHERE T.[FUNCTNL_LOC_KEY] IS NULL;
END
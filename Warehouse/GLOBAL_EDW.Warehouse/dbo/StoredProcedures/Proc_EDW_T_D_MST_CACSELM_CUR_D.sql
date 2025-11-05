CREATE    PROCEDURE [dbo].[Proc_EDW_T_D_MST_CACSELM_CUR_D]
AS
BEGIN
    
    UPDATE T
    SET
      T.[CONTROLLING_AREA_COST_ELEMNT_KE]	= S.[CONTROLLING_AREA_COST_ELEMNT_KE],
      T.[Controlling Area]	= S.[CONTROLLING_AREA],
      T.[Cost Element]	= S.[COST_ELEMNT],
      T.[Valid To Date]	= S.[VALID_TO_DATE],
      T.[Valid From Date]	= S.[VALID_FROM_DATE],
      T.[Cost Element Category]	= S.[COST_ELEMNT_CTGRY],
      T.[Cost Element Category Description]	= S.[COST_ELEMNT_CTGRY_DESC],
      T.[Created On]	= S.[CREATED_ON],
      T.[Entered By]	= S.[ENTERED_BY],
      T.[Cost Element Attributes]	= S.[COST_ELEMNT_ATTRIBUTES],
      T.[Planning Access Indicator]	= S.[PLNG_ACCESS_IND],
      T.[Planning Location Indicator]	= S.[PLNG_LOC_IND],
      T.[Planning User Indicators]	= S.[PLNG_USR_INDICATORS],
      T.[Cost Center]	= S.[COST_CNTR],
      T.[Order Number]	= S.[ORDER_NUM],
      T.[Recording Consumption Quantities Indicator]	= S.[RCDG_CONSPTN_QUANTITIES_IND],
      T.[Unit Of Measurement]	= S.[UNIT_OF_MSRMNT],
      T.[Cost Element Deactivated Indicator]	= S.[COST_ELEMNT_DEACTIVATED_IND],
      T.[Cost Element Is Flagged For Deletion Indicator]	= S.[COST_ELEMNT_IS_FLAGGED_FOR_DELE],
      T.[Recovery Indicator]	= S.[RECOVERY_IND],
      T.[ETL_SRC_SYS_CD]	= S.[ETL_SRC_SYS_CD],
      T.[ETL_EFFECTV_BEGIN_DATE]	= S.[ETL_EFFECTV_BEGIN_DATE],
      T.[ETL_EFFECTV_END_DATE]	= S.[ETL_EFFECTV_END_DATE],
      T.[ETL_CURR_RCD_IND]	= S.[ETL_CURR_RCD_IND],
      T.[ETL_CREATED_TS]	= S.[ETL_CREATED_TS],
      T.[ETL_UPDTD_TS]	= S.[ETL_UPDTD_TS]
      
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CACSELM_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_CACSELM_CUR_D] AS S
        ON T.[CONTROLLING_AREA_COST_ELEMNT_KE] = S.[CONTROLLING_AREA_COST_ELEMNT_KE];

    
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CACSELM_CUR_D] (
       [CONTROLLING_AREA_COST_ELEMNT_KE],
       [Controlling Area],
       [Cost Element],
       [Valid To Date],
       [Valid From Date],
       [Cost Element Category],
       [Cost Element Category Description],
       [Created On],
       [Entered By],
       [Cost Element Attributes],
       [Planning Access Indicator],
       [Planning Location Indicator],
       [Planning User Indicators],
       [Cost Center],
       [Order Number],
       [Recording Consumption Quantities Indicator],
       [Unit Of Measurement],
       [Cost Element Deactivated Indicator],
       [Cost Element Is Flagged For Deletion Indicator],
       [Recovery Indicator],
       [ETL_SRC_SYS_CD],
       [ETL_EFFECTV_BEGIN_DATE],
       [ETL_EFFECTV_END_DATE],
       [ETL_CURR_RCD_IND],
       [ETL_CREATED_TS],
       [ETL_UPDTD_TS]
           
    )
    SELECT
        S.[CONTROLLING_AREA_COST_ELEMNT_KE],
        S.[CONTROLLING_AREA],
        S.[COST_ELEMNT],
        S.[VALID_TO_DATE],
        S.[VALID_FROM_DATE],
        S.[COST_ELEMNT_CTGRY],
        S.[COST_ELEMNT_CTGRY_DESC],
        S.[CREATED_ON],
        S.[ENTERED_BY],
        S.[COST_ELEMNT_ATTRIBUTES],
        S.[PLNG_ACCESS_IND],
        S.[PLNG_LOC_IND],
        S.[PLNG_USR_INDICATORS],
        S.[COST_CNTR],
        S.[ORDER_NUM],
        S.[RCDG_CONSPTN_QUANTITIES_IND],
        S.[UNIT_OF_MSRMNT],
        S.[COST_ELEMNT_DEACTIVATED_IND],
        S.[COST_ELEMNT_IS_FLAGGED_FOR_DELE],
        S.[RECOVERY_IND],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS]

    FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_MST_CACSELM_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CACSELM_CUR_D] AS T
        ON T.[CONTROLLING_AREA_COST_ELEMNT_KE] = S.[CONTROLLING_AREA_COST_ELEMNT_KE]
    WHERE T.[CONTROLLING_AREA_COST_ELEMNT_KE] IS NULL;

END
CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_MST_EQUIPMT_CUR_D]
AS
BEGIN
    -- Update existing records
    UPDATE T
    SET
        T.[Equipment]                                = S.[EQUIP],
        T.[Equipment Description]                    = S.[EQUIP_DESC],
        T.[Equipment Category]                       = S.[EQUIP_CTGRY],
        T.[Equipment Weight Of Object]               = S.[WGHT_OF_OBJ],
        T.[Equipment Unit Of Weight]                 = S.[UNIT_OF_WGHT],
        T.[Equipment Warranty End Date]              = S.[WARRANTY_END_DATE],
        T.[Equipment Note]                           = S.[NOTE],
        T.[Equipment Object Number]                  = S.[OBJ_NUM],
        T.[Equipment Material]                       = S.[MATRL],
        T.[Equipment Serial Number]                  = S.[SERIAL_NUM],
        T.[Equipment Size]                           = S.[SIZE],
        T.[Equipment Date Last Goods Movement]       = S.[DATE_LAST_GOODS_MOVMNT],
        T.[Equipment Authorization Group Origin]     = S.[AUTH_GROUP_ORIGIN],
        T.[Equipment Data Exists]                    = S.[EQUIP_DATA_EXISTS],
        T.[Equipment Serialization Data]             = S.[SERIALIZATION_DATA],
        T.[Equipment EQSI Exists]                    = S.[EQSI_EXISTS],
        T.[Equipment Stock Check]                    = S.[STK_CHK],
        T.[ETL_SRC_SYS_CD]                           = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE]                   = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE]                     = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND]                         = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS]                           = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS]                             = S.[ETL_UPDTD_TS],
        T.[Equipment Stock Type Of Goods Movement]   = S.[STK_TYP_OF_GOODS_MOVEMNT],
        T.[Equipment Plant]                          = S.[PLNT],
        T.[Equipment Storage Location]               = S.[STOR_LOC],
        T.[Equipment Special Stock Indicator]        = S.[SPCL_STK_IND],
        T.[Equipment System Status]                  = S.[SYS_STATUS]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] S
        ON T.[EQUIP_KEY] = S.[EQUIP_KEY];

    -- Insert new records
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] (
        [EQUIP_KEY],
        [Equipment],                             
		[Equipment Description],                 
		[Equipment Category],                    
		[Equipment Weight Of Object],           
		[Equipment Unit Of Weight],              
		[Equipment Warranty End Date],           
		[Equipment Note],                        
		[Equipment Object Number],               
		[Equipment Material],                    
		[Equipment Serial Number],               
		[Equipment Size],                        
		[Equipment Date Last Goods Movement],    
		[Equipment Authorization Group Origin],  
		[Equipment Data Exists],                 
		[Equipment Serialization Data],          
		[Equipment EQSI Exists],                 
		[Equipment Stock Check],                
		[ETL_SRC_SYS_CD],                        
		[ETL_EFFECTV_BEGIN_DATE],                
		[ETL_EFFECTV_END_DATE],                  
		[ETL_CURR_RCD_IND],                      
		[ETL_CREATED_TS],                        
		[ETL_UPDTD_TS],                          
		[Equipment Stock Type Of Goods Movement],
		[Equipment Plant],                       
		[Equipment Storage Location],           
		[Equipment Special Stock Indicator],    
		[Equipment System Status]               	
    )
    SELECT
        S.[EQUIP_KEY],
        S.[EQUIP],
        S.[EQUIP_DESC],
        S.[EQUIP_CTGRY],
        S.[WGHT_OF_OBJ],
        S.[UNIT_OF_WGHT],
        S.[WARRANTY_END_DATE],
        S.[NOTE],
        S.[OBJ_NUM],
        S.[MATRL],
        S.[SERIAL_NUM],
        S.[SIZE],
        S.[DATE_LAST_GOODS_MOVMNT],
        S.[AUTH_GROUP_ORIGIN],
        S.[EQUIP_DATA_EXISTS],
        S.[SERIALIZATION_DATA],
        S.[EQSI_EXISTS],
        S.[STK_CHK],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[STK_TYP_OF_GOODS_MOVEMNT],
        S.[PLNT],
        S.[STOR_LOC],
        S.[SPCL_STK_IND],
        S.[SYS_STATUS]
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] T
        ON T.[EQUIP_KEY] = S.[EQUIP_KEY]
    WHERE T.[EQUIP_KEY] IS NULL;
END
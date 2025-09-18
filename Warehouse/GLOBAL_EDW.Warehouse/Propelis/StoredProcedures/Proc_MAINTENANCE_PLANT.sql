CREATE       PROCEDURE [Propelis].[Proc_MAINTENANCE_PLANT] 
AS
BEGIN  
    ----------------------------------------------------------------------
    -- Update existing records
    ----------------------------------------------------------------------
    UPDATE T
    SET
            T.[PLNT_KEY]                                                  =  S.[PLNT_KEY]  
            ,T.[Maintenance Plant ID]	                                  =  S.[PLNT_ID]  
            ,T.[Maintenance Plant Legacy Plant ID]	                      =  S.[LGCY_PLNT_ID]  
            ,T.[Maintenance Plant Name]	                                  =  S.[PLNT_NAME]  
            ,T.[Maintenance Plant Company Code]	                          =  S.[CMPNY_CD]  
            ,T.[Maintenance Plant Sales Organization]	                  =  S.[SALES_ORGZTN]  
            ,T.[Maintenance Plant Purcahsing Organization]                 =  S.[PURNG_ORGZTN]  
            ,T.[Maintenance Plant Valuation Area]	                      =  S.[VALTN_AREA]  
            ,T.[Maintenance Plant Customer Numer of Plant]	              =  S.[CUST_NUM_OF_PLNT]  
            ,T.[Maintenance Plant Vendor Number of Plant]	              =  S.[VNDR_NUM_OF_PLNT]  
            ,T.[Maintenance Plant Factory Calendar Key]	                  =  S.[FACTORY_CLNDR_KEY]  
            ,T.[Maintenance Plant Line 1 Description]	                  =  S.[PLNT_LINE_1_DESC]  
            ,T.[Maintenance Plant Line 2 Description]	                  =  S.[PLNT_LINE_2_DESC]  
            ,T.[Maintenance Plant City Name]	                              =  S.[PLNT_CITY_NAME]  
            ,T.[Maintenance Plant Zip Code]	                              =  S.[PLNT_ZIP_CD]  
            ,T.[Maintenance Plant Country Code]	                          =  S.[PLNT_CNTRY_CD]  
            ,T.[Maintenance Plant Country Name]	                          =  S.[PLNT_CNTRY_NAME]  
            ,T.[Maintenance Plant Tax Jurisdiction]	                      =  S.[TAX_JRSDCTN]  
            ,T.[ETL_SRC_SYS_CD]	                                          =  S.[ETL_SRC_SYS_CD]  
            ,T.[ETL_EFFECTV_BEGIN_DATE]	                                  =  S.[ETL_EFFECTV_BEGIN_DATE]  
            ,T.[ETL_EFFECTV_END_DATE]	                                  =  S.[ETL_EFFECTV_END_DATE]  
            ,T.[ETL_CURR_RCD_IND]	                                      =  S.[ETL_CURR_RCD_IND]  
            ,T.[ETL_CREATED_TS]	                                          =  S.[ETL_CREATED_TS]  
            ,T.[ETL_UPDTD_TS]	                                          =  S.[ETL_UPDTD_TS]  
            ,T.[Maintenance Plant Is P001]	                              =  S.[IS_P001]  
            ,T.[Maintenance Plant Is P002]	                              =  S.[IS_P002]  
            ,T.[Maintenance Plant Company Description]	                  =  S.[CMPNY_DESC]  
            ,T.[Maintenance Plant Sales Organization Description]	      =  S.[SALES_ORG_DESC]  
            ,T.[Maintenance Plant Factory Calendar Description]	          =  S.[FACTORY_CLNDR_DESC]  
            ,T.[Maintenance Plant Purchasing Organization Description]	  =  S.[PURNG_ORGZTN_DESC]  
            ,T.[Maintenance Plant Tax Indicator]	                          =  S.[PLNT_TAX_IND]  
            ,T.[Maintenance Plant Tax Indicator Description]	              =  S.[PLNT_TAX_IND_DESC]  
            ,T.[Maintenance Plant Shipping/Receiving Point]	              =  S.[SHIPG_RECVNG_PNT]  
            ,T.[Maintenance Plant Shipping/Receiving Point Description]	  =  S.[SHIPG_RECVNG_PNT_DESC]

	FROM [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT] T
    INNER JOIN [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
        ON T.[PLNT_KEY] = S.[PLNT_KEY];
 
    ----------------------------------------------------------------------
    -- Insert new records
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT] (
        [PLNT_KEY]                                              
        ,[Maintenance Plant ID]	                                
        ,[Maintenance Plant Legacy Plant ID]	                    
        ,[Maintenance Plant Name]	                            
        ,[Maintenance Plant Company Code]	                    
        ,[Maintenance Plant Sales Organization]	                
        ,[Maintenance Plant Purcahsing Organization]             
        ,[Maintenance Plant Valuation Area]	                    
        ,[Maintenance Plant Customer Numer of Plant]	            
        ,[Maintenance Plant Vendor Number of Plant]	            
        ,[Maintenance Plant Factory Calendar Key]	            
        ,[Maintenance Plant Line 1 Description]	                
        ,[Maintenance Plant Line 2 Description]	                
        ,[Maintenance Plant City Name]	                        
        ,[Maintenance Plant Zip Code]	                        
        ,[Maintenance Plant Country Code]	                    
        ,[Maintenance Plant Country Name]	                    
        ,[Maintenance Plant Tax Jurisdiction]	                
        ,[ETL_SRC_SYS_CD]	                                    
        ,[ETL_EFFECTV_BEGIN_DATE]	                            
        ,[ETL_EFFECTV_END_DATE]	                                
        ,[ETL_CURR_RCD_IND]	                                    
        ,[ETL_CREATED_TS]	                                    
        ,[ETL_UPDTD_TS]	                                        
        ,[Maintenance Plant Is P001]	                            
        ,[Maintenance Plant Is P002]	                            
        ,[Maintenance Plant Company Description]	                
        ,[Maintenance Plant Sales Organization Description]	    
        ,[Maintenance Plant Factory Calendar Description]	    
        ,[Maintenance Plant Purchasing Organization Description]	
        ,[Maintenance Plant Tax Indicator]	                    
        ,[Maintenance Plant Tax Indicator Description]	        
        ,[Maintenance Plant Shipping/Receiving Point]	        
        ,[Maintenance Plant Shipping/Receiving Point Description]
    )
    SELECT
	
	     S.[PLNT_KEY]  
	     ,S.[PLNT_ID]  
	     ,S.[LGCY_PLNT_ID]  
	     ,S.[PLNT_NAME]  
	     ,S.[CMPNY_CD]  
	     ,S.[SALES_ORGZTN]  
	     ,S.[PURNG_ORGZTN]  
	     ,S.[VALTN_AREA]  
	     ,S.[CUST_NUM_OF_PLNT]  
	     ,S.[VNDR_NUM_OF_PLNT]  
	     ,S.[FACTORY_CLNDR_KEY]  
	     ,S.[PLNT_LINE_1_DESC]  
	     ,S.[PLNT_LINE_2_DESC]  
	     ,S.[PLNT_CITY_NAME]  
	     ,S.[PLNT_ZIP_CD]  
	     ,S.[PLNT_CNTRY_CD]  
	     ,S.[PLNT_CNTRY_NAME]  
	     ,S.[TAX_JRSDCTN]  
	     ,S.[ETL_SRC_SYS_CD]  
	     ,S.[ETL_EFFECTV_BEGIN_DATE] 
	     ,S.[ETL_EFFECTV_END_DATE]  
	     ,S.[ETL_CURR_RCD_IND]  
	     ,S.[ETL_CREATED_TS]  
	     ,S.[ETL_UPDTD_TS]  
	     ,S.[IS_P001]  
	     ,S.[IS_P002]  
	     ,S.[CMPNY_DESC]  
	     ,S.[SALES_ORG_DESC]  
	     ,S.[FACTORY_CLNDR_DESC]  
	     ,S.[PURNG_ORGZTN_DESC]  
	     ,S.[PLNT_TAX_IND]  
	     ,S.[PLNT_TAX_IND_DESC]  
	     ,S.[SHIPG_RECVNG_PNT]  
	     ,S.[SHIPG_RECVNG_PNT_DESC]

    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_MST_PLANT_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT] T
         ON T.[PLNT_KEY] = S.[PLNT_KEY]
         WHERE T.[PLNT_KEY] IS NULL;
    
   END
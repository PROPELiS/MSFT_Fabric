-- Auto Generated (Do not modify) E0CDEEF59896C5B5F713128C813EEEEC9FC3FD04421270183520954B5CBB681F
CREATE   VIEW [Propelis].[V_MAINTENANCE_PLANT](
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
)AS
SELECT
    [PLNT_KEY],
        [Plant ID],
        [Legacy Plant ID],
        [Plant Name],
        [Plant Company Code],
        [Plant Sales Organization],
        [Plant Purchasing Organization],
        [Plant Valuation Area],
        [Plant Customer Number],
        [Plant Vendor Number],
        [Plant Factory Calendar Key],
        [Plant Line 1 Description],
        [Plant Line 2 Description],
        [Plant City Name],
        [Plant Zip Code],
        [Plant Country Code],
        [Plant Country Name],
        [Plant Tax Jurisdiction],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Plant IS P001],
        [Plant IS P002],
        [Plant Company Description],
        [Plant Sales Organization Description],
        [Plant Factory Calendar Description],
        [Plant Purchasing Organization Description],
        [Plant Tax Indicator],
        [Plant Tax Indicator Description],
        [Plant Shipping/Receiving Point],
        [Plant Shipping/Receiving Point Description]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D];
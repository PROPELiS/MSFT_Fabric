CREATE PROCEDURE [Propelis].[Proc_TEMP_EDW_T_FACT_PRD_SERVCST_CUR_D] 
AS
BEGIN 
      -- Step 1: Delete existing data from the temp table
     DELETE FROM [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVCST_CUR_D];
		
    INSERT INTO [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_SERVCST_CUR_D]
       (
         [SERVC_ORDER_KEY],
         [PRODUCING_PLNT_KEY],
         [Document Number],
         [Period],
         [DOCMT_CRNCY_KEY],
         [CMPNY_CRNCY_KEY],
         [GROUP_CRNCY_KEY],
         [DOCMT_CRNCY_VAL],
         [CMPNY_CRNCY_VAL],
         [GROUP_CRNCY_VAL],
         [QTY_TOTAL],
         [UoM],
         [Fiscal Year],
         [Value Type],
         [COST_ELEMNT_KEY],
         [TRNSCTN_TYP],
         [Partner Object],
         [MATRL_KEY],
         [CMPNY_KEY],
         [PRTNR_CMPNY_KEY],
         [Order Type],
         [Order Category],
         [MAINTENANCE_WRK_CNTR_KEY],
         [Object Class],
         [RESPNSBLE_COST_CNTR_KEY],
         [PROFT_CNTR_KEY],
         [ETL_SRC_SYS_CD],
         [ETL_CREATED_TS],
         [ETL_UPDTD_TS],
         [LOC_PLNT_KEY],
         [Type Of Cost],
         [PERSONEL_NUM_KEY],
         [FUNCTNL_AREA_KEY],
         [Debit/Credit Indicator],
         [Reference Document Number],
         [Controlling Area],
         [PLNT_KEY],
         [PSTNG_DATE_KEY],
         [Value Type Description],
         [Transaction Type Description],
         [Controlling Area Description],
         [Is Internal  Posting],
         [ACTIVITY_TYP_KEY],
         [Posting Row],
         [Is INTER Company Posting],
         [Is INTRA Company Posting],
         [PRTNR_COST_CNTR_KEY],
         [TCHNCL_COMPLETION_DATE_KEY],
         [DOCMT_DATE_KEY],
         [CREATION_DATE_KEY],
         [Posting Creation Date Timestamp],
		 
		 --below added for alias
		 
		 [PERSONNEL_PLANT_REGION_PLANT],
		 [PLANT_REGION_PLANT],
		 [LOCATION_PLANT_REGION_PLANT],
		 [PRODUCING_PLANT_REGION_PLANT]
        )



SELECT 
        Fact.[SERVC_ORDER_KEY],
        Fact.[PRODUCING_PLNT_KEY],
        Fact.[DOCMT_NUM],
        Fact.[PER],
        Fact.[DOCMT_CRNCY_KEY],
        Fact.[CMPNY_CRNCY_KEY],
        Fact.[GROUP_CRNCY_KEY],
        Fact.[DOCMT_CRNCY_VAL],
        Fact.[CMPNY_CRNCY_VAL],
        Fact.[GROUP_CRNCY_VAL],
        Fact.[QTY_TOTAL],
        Fact.[UOM],
        Fact.[FISCAL_YR],
        Fact.[VAL_TYP],
        Fact.[COST_ELEMNT_KEY],
        Fact.[TRNSCTN_TYP],
        Fact.[PRTNR_OBJ],
        Fact.[MATRL_KEY],
        Fact.[CMPNY_KEY],
        Fact.[PRTNR_CMPNY_KEY],
        Fact.[ORDER_TYP],
        Fact.[ORDER_CTGRY],
        Fact.[MAINTENANCE_WRK_CNTR_KEY],
        Fact.[OBJ_CLASS],
        Fact.[RESPNSBLE_COST_CNTR_KEY],
        Fact.[PROFT_CNTR_KEY],
        Fact.[ETL_SRC_SYS_CD],
        Fact.[ETL_CREATED_TS],
        Fact.[ETL_UPDTD_TS],
        Fact.[LOC_PLNT_KEY],
        Fact.[TYP_OF_COST],
        Fact.[PERSONEL_NUM_KEY],
        Fact.[FUNCTNL_AREA_KEY],
        Fact.[DEBIT_CREDIT_IND],
        Fact.[REF_DOCMT_NUM],
        Fact.[CONTROLLING_AREA],
        Fact.[PLNT_KEY],
        Fact.[PSTNG_DATE_KEY],
        Fact.[VAL_TYP_DESC],
        Fact.[TRNSCTN_TYP_DESC],
        Fact.[CONTROLLING_AREA_DESC],
        Fact.[IS_INTERNL_PSTNG],
        Fact.[ACTIVITY_TYP_KEY],
        Fact.[PSTNG_ROW],
        Fact.[IS_INTER_CMPNY_PSTNG],
        Fact.[IS_INTRA_CMPNY_PSTNG],
        Fact.[PRTNR_COST_CNTR_KEY],
        Fact.[TCHNCL_COMPLETION_DATE_KEY],
        Fact.[DOCMT_DATE_KEY],
        Fact.[CREATION_DATE_KEY],
        Fact.[CREATION_DATE_TS],
		
		--below added for alias
		
		SUBSTRING(PERSONNEL_NUMBER.[Personnel Cost Center ID],4,4) AS PERSONNEL_PLANT_REGION_PLANT,
		PLANT.[Plant ID] AS PLANT_REGION_PLANT,
		LOCATION_PLANT.[Plant ID] AS LOCATION_PLANT_REGION_PLANT,
		PRODUCING_PLANT.[Plant ID] AS PRODUCING_PLANT_REGION_PLANT

        FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_F_PRD_SERVCST_CUR_D] AS Fact
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D ] AS COSTELM
         
          ON Fact.COST_ELEMNT_KEY = COSTELM.COST_ELEMNT_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D]  AS FUNCTAR
         
          ON Fact.FUNCTNL_AREA_KEY = FUNCTAR.FUNCTNL_AREA_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
         
          ON Fact.MATRL_KEY = MATERIAL.MATRL_KEY
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D]  AS PROFCTR
         
          ON Fact.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_WORKCTR_CUR_D]  AS WORKCTR
         
          ON Fact.MAINTENANCE_WRK_CNTR_KEY = WORKCTR.WRK_CNTR_KEY   
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRD_SERVORD_CUR_D]  AS SERVORD
         
          ON Fact.SERVC_ORDER_KEY = SERVORD.SERVC_ORDER_KEY
         
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D]  As DATE_CUR_D
         
          ON Fact.PSTNG_DATE_KEY = DATE_CUR_D.DATE_KEY   
         
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D]  As ACTTYPE
         
          ON Fact.ACTIVITY_TYP_KEY = ACTTYPE.ACTIVITY_TYP_KEY  
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D] As DOCUMENT_CURRENCY
         
          ON Fact.DOCMT_CRNCY_KEY = DOCUMENT_CURRENCY.CRNCY_KEY  
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D] As COMPANY_CURRENCY
         
          ON Fact.CMPNY_CRNCY_KEY = COMPANY_CURRENCY.CRNCY_KEY  
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D]  As GROUP_CURRENCY 
         
          ON Fact.GROUP_CRNCY_KEY = GROUP_CURRENCY.CRNCY_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] As COMPANY
         
          ON Fact.CMPNY_KEY = COMPANY.CMPNY_KEY   
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] As PARTNER_COMPANY
         
          ON Fact.PRTNR_CMPNY_KEY = PARTNER_COMPANY.CMPNY_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D]  As PERSONNEL_NUMBER 
         
          ON Fact.PERSONEL_NUM_KEY = PERSONNEL_NUMBER.PERSONEL_KEY    
          
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] As RESP_COST_CENTER 
         
          ON Fact.RESPNSBLE_COST_CNTR_KEY = RESP_COST_CENTER.COST_CNTR_KEY   
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] As PLANT
         
          ON Fact.PLNT_KEY = PLANT.PLNT_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D]  As LOCATION_PLANT
         
          ON Fact.LOC_PLNT_KEY = LOCATION_PLANT.PLNT_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D]  As PRODUCING_PLANT
         
          ON Fact.PRODUCING_PLNT_KEY = PRODUCING_PLANT.PLNT_KEY  
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D] As TECH_COMPLETION_DATE
         
          ON Fact.TCHNCL_COMPLETION_DATE_KEY = TECH_COMPLETION_DATE.DATE_KEY    
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D] As CREATION_DATE
         
          ON Fact.CREATION_DATE_KEY = CREATION_DATE.DATE_KEY  
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D] As DOCUMENT_DATE
         
          ON Fact.DOCMT_DATE_KEY = DOCUMENT_DATE.DATE_KEY 
        
        LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] As PARTNER_COST_CENTER
         
          ON Fact.PRTNR_COST_CNTR_KEY = PARTNER_COST_CENTER.COST_CNTR_KEY
		
	END
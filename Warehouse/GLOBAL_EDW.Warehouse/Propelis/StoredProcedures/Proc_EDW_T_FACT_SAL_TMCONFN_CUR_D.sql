CREATE       PROCEDURE [Propelis].[Proc_EDW_T_FACT_SAL_TMCONFN_CUR_D]
AS 
BEGIN

   -- Step 1: Clear existing data from the target table
   
   TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_TMCONFN_CUR_D];
   
   -- Step 2: Insert refreshed data from mirror table with joins
   
   INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_TMCONFN_CUR_D]
   (
       [Controlling Area],    
       [Document Number],
       [Posting Row],
       [Fiscal Year],
       [Version Header],
       [Business Transaction Header],
       [DOCMT_DATE_KEY],
       [PSTNG_DATE_KEY],
       [Document Type],
       [User Name],
       [Period],
       [PLNT_KEY],
       [COST_CNTR_KEY],
       [ACTIVITY_TYP_KEY],
       [Value Type],
       [Version],
       [Business Transaction],
       [PERSONEL_NUM_KEY],
       [Activity UOM],
       [ACTIVITY_QTY],
       [Actual Work UOM],
       [ACTUAL_WRK],
       [ACTUAL_WRK_IN_HRS],
       [Actual Work in Hours UOM],
       [Object Number],
       [Description],
       [ETL_SRC_SYS_CD],
       [ETL_CREATED_TS],
       [ETL_UPDTD_TS],
       [PROFT_CNTR_KEY],
	   
	   --Below columns are added for the Alias tables
	   
	   [PLANT_REGION_PLANT],
       [PERSONNEL_PLANT_REGION_PLANT],
	   [COSTCHZ_COST_CNTR_KEY],
	   [COSTCVZ_COST_CNTR_KEY],
	   [PROFCHZ_PROFT_CNTR_KEY],
 	   [PROFCVZ_PROFT_CNTR_KEY]
    )
    SELECT 
       FACT.[CONTROLLING_AREA],
       FACT.[DOCMT_NUM],
       FACT.[PSTNG_ROW],
       FACT.[FISCAL_YR],
       FACT.[VRSN_HDR],
       FACT.[BIZ_TRNSCTN_HDR],
       FACT.[DOCMT_DATE_KEY],
       FACT.[PSTNG_DATE_KEY],  
       FACT.[DOCMT_TYP],
       FACT.[USR_NAME],
       FACT.[PER],
       FACT.[PLNT_KEY],
       FACT.[COST_CNTR_KEY],
       FACT.[ACTIVITY_TYP_KEY],
       FACT.[VAL_TYP],
       FACT.[VRSN],
       FACT.[BIZ_TRNSCTN],
       FACT.[PERSONEL_NUM_KEY],
       FACT.[ACTIVITY_UOM],
       FACT.[ACTIVITY_QTY],
       FACT.[ACTUAL_WRK_UOM],
       FACT.[ACTUAL_WRK],
       FACT.[ACTUAL_WRK_IN_HRS],
       FACT.[ACTUAL_WRK_IN_HRS_UOM],
       FACT.[OBJ_NUM],
       FACT.[DESC],
       FACT.[ETL_SRC_SYS_CD],
       FACT.[ETL_CREATED_TS],
       FACT.[ETL_UPDTD_TS],
       FACT.[PROFT_CNTR_KEY],
	   
	   --Below columns are added for the Alias tables
	   
	   PLANT.[Plant ID] AS PLANT_REGION_PLANT,
       SUBSTRING(PERSONL.[Personnel Cost Center ID],4,4) AS PERSONNEL_PLANT_REGION_PLANT,
	   COSCNTR.COST_CNTR_KEY AS COSTCHZ_COST_CNTR_KEY,
	   COSCNTR.COST_CNTR_KEY AS COSTCVZ_COST_CNTR_KEY,
	   PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
	   PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY

    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_SAL_TMCONFN_CUR_D] AS FACT

    LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] AS DOC_DT
        ON FACT.DOCMT_DATE_KEY = DOC_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] AS POS_DT
        ON FACT.PSTNG_DATE_KEY = POS_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] AS COSCNTR
        ON FACT.COST_CNTR_KEY = COSCNTR.COST_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] AS ACTTYPE
        ON FACT.ACTIVITY_TYP_KEY = ACTTYPE.ACTIVITY_TYP_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS PERSONL
        ON FACT.PERSONEL_NUM_KEY = PERSONL.PERSONEL_KEY
     	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
        ON FACT.PLNT_KEY = PLANT.PLNT_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
        ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY;
END
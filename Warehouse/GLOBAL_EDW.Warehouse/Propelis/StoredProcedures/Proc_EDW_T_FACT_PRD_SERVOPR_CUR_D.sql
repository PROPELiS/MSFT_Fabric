CREATE   PROCEDURE [Propelis].[Proc_EDW_T_FACT_PRD_SERVOPR_CUR_D]
AS
BEGIN
        
	-- STEP 1: TRUNCATE THE TEMP TABLE (TRUNCATE IS FASTER THAN DELETE)
		
    TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVOPR_CUR_D];
		
	-- Step 2: Insert refreshed data from mirror table with joins
		
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_SERVOPR_CUR_D](
		
		[Service Order Execution Factor],
        [Service Order Group Counter],
        [Service Order Operation System Status],
        [EMPL_RESPNSBLE_KEY],
        [Is Deletion Set],
        [Sales Order Item],
        [Sales Order ID],
        [GROUP_CRNCY_ACTUAL_WRK_AMT],
        [CMPNY_CRNCY_ACTUAL_WRK_AMT],
        [GROUP_CRNCY_NORMAL_DUR_AMT],
        [CMPNY_CRNCY_NORMAL_DUR_AMT],
        [GROUP_CRNCY_WRK_INV_ACTIVITY_AM],
        [CMPNY_CRNCY_WRK_INV_ACTIVITY_AM],
        [OPERATION_LVL_WRK_CNTR_KEY],
        [Printer Specification Document Number],
        [CUST_MATRL_INFO_RCD_KEY],
        [BRAND_OWNER_CUST_KEY],
        [Press Name],
        [Service Order Task List Type Description],
        [Activity Typ 06 PP Description],
        [Activity Typ 04 PP Description],
        [Activity Typ 03 PP Description],
        [Activity Typ 02 PP Description],
        [Activity Typ 01 PP Description],
        [PARNT_CUST_KEY],
        [Operation User Status],
        [Operations User Status Profile],
        [Header User Status Profile],
        [User Field 7 Unit],
        [User Field 6 Unit],
        [User Field 5 Unit],
        [User Field 4 Unit],
        [Regulatory Indicator],
        [Recovery Indicator],
        [General Counter For Order],
        [Created On],
        [User Field 08],
        [Order Complexity],
        [Created By],
        [User Field 02],
        [User Field 01],
        [User Field 00],
        [Service Order Task List Type],
        [Second Line Description],
        [Material Base Unit Of Measure],
        [Actual Finish Date],
        [Activity Type PP 6],
        [Activity Type PP 4],
        [Activity Type PP 3],
        [Activity Type PP 2],
        [Activity Type PP 1],
        [CREATED_ON_DATE_KEY],
        --[Standard Text Key],
        [FUNCTNL_AREA_KEY],
        [PERSONEL_NUM_KEY],
        [ACTIVITY_TYP_KEY],
        [SERVC_MATRL_KEY],
        [ETL_UPDTD_TS],
        [ETL_CREATED_TS],
        [ETL_SRC_SYS_CD],
        [Finish Constraint Timestamp],
        [FINISH_CNSTRNT_DATE_KEY],
        [Is Finish Constraint],
        [Start Constraint Timestamp],
        [START_CNSTRNT_DATE_KEY],
        [Is Start Constraint],
        [Is Correction Approval Rework Description],
        [Is Correction Approval Rework],
        [Sort Term],
        [Is On Time Delivery Production Center],
        [Is On Time Delivery Business Center],
        [Is Right First Time Production Center],
        [Is Right First Time Business Center],
        [Account Indicator],
        [PRINTER_CUST_KEY],
        [SOLD_TO_CUST_KEY],
        [SHIP_TO_CUST_KEY],
        [BILL_TO_CUST_KEY],
        [SALES_ORDER_KEY],
        [FUNCTNL_LOC_KEY],
        [SERVC_ORDER_KEY],
        [Latest Scheduled Start Execution Timestamp],
        [WRK_CNTR_KEY],
        [Control Key Description],
        [Actual Finish Execution Timestamp],
        [ACTUAL_FINISH_EXCT_DATE_KEY],
        [Latest Scheduled Finish Execution Timestamp],
        [LATEST_SCHLD_FINISH_EXCT_DATE_KEY],
        [Earliest Scheduled Finish Execution Timestamp],
        [ERLST_SCHED_FINISH_EXCT_DATE_KEY],
        [Actual Start Execution Setup Timestamp],
        [ACTUAL_START_EXCT_DATE_KEY],
        [LATEST_SCHLD_START_EXCT_DATE_KEY],
        [Earliest Scheduled Start Execution Timestamp],
        [ERLST_SCHLD_START_EXCT_DATE_KEY],
        [FORECASTED_WRK_ACTUAL_REMG],
        [ACTUAL_WRK],
        [Normal Duration UOM],
        [NORMAL_DUR_OF_THE_ACTIVITY],
        [WRK_UOM],
        [WRK_INVOLVED_IN_THE_ACTIVITY],
        [PRICE],
        [Work Percentage],
        [Operation Short Text],
        [Object Number],
        [Object ID],
        [CTRL_KEY],
        [Breakdown Duration],
        [Breakdown Duration UOM],
        [End Of Malfunction Timestamp],
        [END_OF_MALFUNCTION_DATE_KEY],
        [Start Of Malfunction Timestamp],
        [START_OF_MALFUNCTION_DATE_KEY],
        [COST_CNTR_KEY],
        [NOTIFICATION_KEY],
        [PLNG_PLNT_KEY],
        [EQUIP_KEY],
        [Service Order Capacity Requirements ID],
        [TRGT_QTY],
        [TOTAL_SCRAP_AMT],
        [PLND_RELS_DATE_KEY],
        [ACTUAL_RELS_DATE_KEY],
        [ACTUAL_FINISH_DATE_KEY],
        [ACTUAL_START_DATE_KEY],
        [SCHLD_START_DATE_KEY],
        [SCHLD_FINISH_DATE_KEY],
        [SCHLD_RELS_DATE_KEY],
        [BASIC_FINISH_DATE_KEY],
        [BASIC_START_DATE_KEY],
        [PROFT_CNTR_KEY],
        [RESPNSBLE_COST_CNTR_KEY],
        [MAINTENANCE_PLNT_KEY],
        [LOC_PLNT_KEY],
        [CLS_DATE_KEY],
        [TCHNCL_COMPLETION_DATE_KEY],
        [RELS_DATE_KEY],
        [DOCMT_CRNCY_KEY],
        [PLNT_KEY],
        [CMPNY_CD_KEY],
        [Order Category],
        [Service Order Order Type],
        [Operation Counter],
        [Operation Sequence ID],
        [Operation Activity ID],
        [Operation Confirmation ID],
        [Operation Counter ID],
        [Operation Task List ID],
        [Service Order],
    
	--Below coloumns are added for Alias tables
		
		[PLANT_REGION_PLANT],
		[PERSONNEL_PLANT_REGION_PLANT],
		[LOCATION_PLANT_REGION_PLANT],
		[PLANNING_PLANT_REGION_PLANT],
		[OPERATING_PLANT_REGION_PLANT],
		[MAINTENANCE_PLANT_REGION_PLANT],
		[SO_PARENT_CUST_KEY],
		[PARENT_CUST_KEY],
		[PROFCVZ_PROFT_CNTR_KEY],
		[PROFCHZ_PROFT_CNTR_KEY],
		[WORKCHZ_WRK_CNTR_KEY],
		[WORKCVZ_WRK_CNTR_KEY],
		[SALES_EMPLOYEE_PERSONEL_KEY],
		
		[Resp_Cost_Center_Horizontal_Hierarchy_KEY],
		[Resp_Cost_Center_Vertical_Hierarchy_KEY],
		[Cost_Center_Vertical_Hierarchy_KEY],
		[Cost_Center_Horizontal_Hierarchy_KEY],
		[PRDVCON_KEY],
		[SOIPTNR_KEY]
		
	)
	
    -- YOUR ENTIRE SELECT QUERY GOES HERE:
	
	SELECT 
	
	    FACT.[EXCT_FCTR],
        FACT.[GROUP_COUNTER],
        FACT.[OPERATION_SYS_STATUS],
        FACT.[EMPL_RESPNSBLE_KEY],
        FACT.[IS_DELETION_SET],
        FACT.[SALES_ORDER_ITM],
        FACT.[SALES_ORDER_ID],
        FACT.[GROUP_CRNCY_ACTUAL_WRK_AMT],
        FACT.[CMPNY_CRNCY_ACTUAL_WRK_AMT],
        FACT.[GROUP_CRNCY_NORMAL_DUR_AMT],
        FACT.[CMPNY_CRNCY_NORMAL_DUR_AMT],
        FACT.[GROUP_CRNCY_WRK_INV_ACTIVITY_AM],
        FACT.[CMPNY_CRNCY_WRK_INV_ACTIVITY_AM],
        FACT.[OPERATION_LVL_WRK_CNTR_KEY],
        FACT.[PRINTER_SPEC_DOCMT_NUM],
        FACT.[CUST_MATRL_INFO_RCD_KEY],
        FACT.[BRAND_OWNER_CUST_KEY],
        FACT.[PRESS_NAME],
        FACT.[TASK_LIST_TYP_DESC],
        FACT.[ACTIVITY_TYP_06_PP_DESC],
        FACT.[ACTIVITY_TYP_04_PP_DESC],
        FACT.[ACTIVITY_TYP_03_PP_DESC],
        FACT.[ACTIVITY_TYP_02_PP_DESC],
        FACT.[ACTIVITY_TYP_01_PP_DESC],
        FACT.[PARNT_CUST_KEY],
        FACT.[OPERATION_USR_STATUS],
        FACT.[OPS_USR_STATUS_PROFILE],
        FACT.[HDR_USR_STATUS_PROFILE],
        FACT.[USR_FLD_7_UNIT],
        FACT.[USR_FLD_6_UNIT],
        FACT.[USR_FLD_5_UNIT],
        FACT.[USR_FLD_4_UNIT],
        FACT.[REGULATORY_IND],
        FACT.[RECOVERY_IND],
        FACT.[GENERAL_COUNTER_FOR_ORDER],
        FACT.[CREATE_ON],
        FACT.[USR_FLD_08],
        FACT.[ORDER_COMPLEXITY],
        FACT.[CREATE_BY],
        FACT.[USR_FLD_02],
        FACT.[USR_FLD_01],
        FACT.[USR_FLD_00],
        FACT.[TASK_LIST_TYP],
        FACT.[SECOND_LINE_DESC],
        FACT.[BASE_UNIT_OF_MEASUR],
        FACT.[ACTUAL_FINISH_DATE],
        FACT.[ACTIVITY_TYP_06],
        FACT.[ACTIVITY_TYP_04],
        FACT.[ACTIVITY_TYP_03],
        FACT.[ACTIVITY_TYP_02],
        FACT.[ACTIVITY_TYP_01],
        FACT.[CREATED_ON_DATE_KEY],
        FACT.[FUNCTNL_AREA_KEY],
        FACT.[PERSONEL_NUM_KEY],
        FACT.[ACTIVITY_TYP_KEY],
        FACT.[SERVC_MATRL_KEY],
        FACT.[ETL_UPDTD_TS],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[FINISH_CNSTRNT_TS],
        FACT.[FINISH_CNSTRNT_DATE_KEY],
        FACT.[IS_FINISH_CNSTRNT],
        FACT.[START_CNSTRNT_TS],
        FACT.[START_CNSTRNT_DATE_KEY],
        FACT.[IS_START_CNSTRNT],
        FACT.[IS_CORRECTION_APPROVAL_REWORK_D],
        FACT.[IS_CORRECTION_APPROVAL_REWORK],
        FACT.[SRT_TERM],
        FACT.[IS_ON_TM_DLVRY_PROD_CNTR],
        FACT.[IS_ON_TM_DLVRY_BIZ_CNTR],
        FACT.[IS_RIGHT_FIRST_TM_PROD_CNTR],
        FACT.[IS_RIGHT_FIRST_TM_BIZ_CNTR],
        FACT.[ACCT_IND],
        FACT.[PRINTER_CUST_KEY],
        FACT.[SOLD_TO_CUST_KEY],
        FACT.[SHIP_TO_CUST_KEY],
        FACT.[BILL_TO_CUST_KEY],
        FACT.[SALES_ORDER_KEY],
        FACT.[FUNCTNL_LOC_KEY],
        FACT.[SERVC_ORDER_KEY],
        FACT.[LATEST_SCHLD_START_EXCT_TS],
        FACT.[WRK_CNTR_KEY],
        FACT.[CTRL_KEY_DESC],
        FACT.[ACTUAL_FINISH_EXCT_TS],
        FACT.[ACTUAL_FINISH_EXCT_DATE_KEY],
        FACT.[LATEST_SCHLD_FINISH_EXCT_TS],
        FACT.[LATEST_SCHLD_FINISH_EXCT_DATE_K],
        FACT.[ERLST_SCHED_FINISH_EXCT_TS],
        FACT.[ERLST_SCHED_FINISH_EXCT_DATE_KE],
        FACT.[ACTUAL_START_EXCT_SETUP_TS],
        FACT.[ACTUAL_START_EXCT_DATE_KEY],
        FACT.[LATEST_SCHLD_START_EXCT_DATE_KE],
        FACT.[ERLST_SCHLD_START_EXCT_TS],
        FACT.[ERLST_SCHLD_START_EXCT_DATE_KEY],
        FACT.[FORECASTED_WRK_ACTUAL_REMG],
        FACT.[ACTUAL_WRK],
        FACT.[NORMAL_DUR_UOM],
        FACT.[NORMAL_DUR_OF_THE_ACTIVITY],
        FACT.[WRK_UOM],
        FACT.[WRK_INVOLVED_IN_THE_ACTIVITY],
        FACT.[PRICE],
        FACT.[WRK_PRCNTG],
        FACT.[OPERATION_SHORT_TEXT],
        FACT.[OBJ_NUM],
        FACT.[OBJ_ID],
        FACT.[CTRL_KEY],
        FACT.[BREAKDOWN_DUR],
        FACT.[BREAKDOWN_DUR_UOM],
        FACT.[END_OF_MALFUNCTION_TS],
        FACT.[END_OF_MALFUNCTION_DATE_KEY],
        FACT.[START_OF_MALFUNCTION_TS],
        FACT.[START_OF_MALFUNCTION_DATE_KEY],
        FACT.[COST_CNTR_KEY],
        FACT.[NOTIFICATION_KEY],
        FACT.[PLNG_PLNT_KEY],
        FACT.[EQUIP_KEY],
        FACT.[CAPACITY_REQMNTS_ID],
        FACT.[TRGT_QTY],
        FACT.[TOTAL_SCRAP_AMT],
        FACT.[PLND_RELS_DATE_KEY],
        FACT.[ACTUAL_RELS_DATE_KEY],
        FACT.[ACTUAL_FINISH_DATE_KEY],
        FACT.[ACTUAL_START_DATE_KEY],
        FACT.[SCHLD_START_DATE_KEY],
        FACT.[SCHLD_FINISH_DATE_KEY],
        FACT.[SCHLD_RELS_DATE_KEY],
        FACT.[BASIC_FINISH_DATE_KEY],
        FACT.[BASIC_START_DATE_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[RESPNSBLE_COST_CNTR_KEY],
        FACT.[MAINTENANCE_PLNT_KEY],
        FACT.[LOC_PLNT_KEY],
        FACT.[CLS_DATE_KEY],
        FACT.[TCHNCL_COMPLETION_DATE_KEY],
        FACT.[RELS_DATE_KEY],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[PLNT_KEY],
        FACT.[CMPNY_CD_KEY],
        FACT.[ORDER_CTGRY],
        FACT.[ORDER_TYP],
        FACT.[OPERATION_COUNTER],
        FACT.[OPERATION_SEQ_ID],
        FACT.[OPERATION_ACTIVITY_ID],
        FACT.[OPERATION_CONFIRM_ID],
        FACT.[OPERATION_COUNTER_ID],
        FACT.[OPERATION_TASK_LIST_ID],
        FACT.[SERVC_ORDER],
        
	    
    --BELOW COLUMNS ARE ADDED FOR THE ALIAS TABLES
	    
	   
	    PLT. [Plant ID] AS PLANT_REGION_PLANT,
	    SUBSTRING(PER_NUM.[Personnel Cost Center ID],4,4) AS PERSONNEL_PLANT_REGION_PLANT,
	    LOC_PLNT.[Location Plant ID] AS LOCATION_PLANT_REGION_PLANT,
	    PLNN_PLNT.[Planning Plant ID] AS PLANNING_PLANT_REGION_PLANT,
	    OPR_PLT.[Operating Plant ID] AS OPERATING_PLANT_REGION_PLANT,
	    MAIN_PLNT.[Maintenance Plant ID] AS MAINTENANCE_PLANT_REGION_PLANT,
	    SOIPTNR.SO_PARNT_CUST_KEY AS SO_PARENT_CUST_KEY,
		SOIPTNR.PARNT_CUST_KEY AS PARENT_CUST_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY,
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
	    WORKCTR.WRK_CNTR_KEY AS WORKCHZ_WRK_CNTR_KEY,
	    WORKCTR.WRK_CNTR_KEY AS WORKCVZ_WRK_CNTR_KEY,
		SVORDPTR.SALES_EMPL_KEY AS SALES_EMPLOYEE_PERSONEL_KEY,
	   
		REP_CT_CNTR.COST_CNTR_KEY AS Resp_Cost_Center_Horizontal_Hierarchy_KEY,
		REP_CT_CNTR.COST_CNTR_KEY AS Resp_Cost_Center_Vertical_Hierarchy_KEY,
		CT_CNTR.COST_CNTR_KEY AS Cost_Center_Vertical_Hierarchy_KEY,
		CT_CNTR.COST_CNTR_KEY AS Cost_Center_Horizontal_Hierarchy_KEY,
		CONCAT(FACT.SALES_ORDER_ID,'_',FACT.SALES_ORDER_ITM) AS PRDVCON_KEY,
	    CONCAT(FACT.SALES_ORDER_ID,'_',FACT.SALES_ORDER_ITM) AS SOIPTNR_KEY
	
	
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_PRD_SERVOPR_CUR_D] AS FACT
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ACTUAL_FINISH_DATE] AS ACT_FIN_DT
        
    	ON FACT.ACTUAL_FINISH_DATE_KEY=ACT_FIN_DT.DATE_KEY
                
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ACTUAL_FINISH_EXEC_DATE] AS ACT_FIN_EX_DT
    
    	ON FACT.ACTUAL_FINISH_EXCT_DATE_KEY = ACT_FIN_EX_DT.DATE_KEY
            
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ACTUAL_RELEASE_DATE] AS ACT_REL_DT
    
    	ON FACT.ACTUAL_RELS_DATE_KEY = ACT_REL_DT.DATE_KEY
            
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ACTUAL_START_DATE] AS ACT_SRT_DT 
    
    	ON FACT.ACTUAL_START_DATE_KEY = ACT_SRT_DT.DATE_KEY
            
    LEFT JOIN [GLOBAL_EDW].[Propelis].[ACTUAL_START_EXEC_DATE] AS ACT_SRT_EX_DT
    
    	ON FACT.ACTUAL_START_EXCT_DATE_KEY = ACT_SRT_EX_DT.DATE_KEY
            
    LEFT JOIN [GLOBAL_EDW].[Propelis].[BASIC_FINISH_DATE] AS BSE_FIN_DT
    
    	ON FACT.BASIC_FINISH_DATE_KEY = BSE_FIN_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[BASIC_START_DATE] AS BSE_SRT_DT
    
    	ON FACT.BASIC_START_DATE_KEY = BSE_SRT_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CLOSE_DATE] AS CLO_DT
    
    	ON FACT.CLS_DATE_KEY = CLO_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY]  AS CMP
    	ON FACT.CMPNY_CD_KEY = CMP.CMPNY_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[COST_CENTER] AS CT_CNTR
    	ON FACT.COST_CNTR_KEY = CT_CNTR.COST_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] AS DOC_CNRNCY
    	ON FACT.DOCMT_CRNCY_KEY = DOC_CNRNCY.CRNCY_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[END_OF_MALFUNCTION_DATE] AS END_MAL_DT
    	ON FACT.END_OF_MALFUNCTION_DATE_KEY = END_MAL_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[EARLIEST_SCHLD_FINISH_EXEC_DATE] AS EAR_SC_FIN_EX_DT
    	ON FACT.ERLST_SCHED_FINISH_EXCT_DATE_KE = EAR_SC_FIN_EX_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[EARLIEST_SCHLD_START_EXEC_DATE] AS EAR_SC_SRT_EX
    	ON FACT.ERLST_SCHLD_START_EXCT_DATE_KEY = EAR_SC_SRT_EX.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[FINISH_CONSTRAINT_DATE] AS FIN_CON_DT
    	ON FACT.FINISH_CNSTRNT_DATE_KEY = FIN_CON_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[LATEST_SCHLD_FINISH_EXEC_DATE] AS LAT_SC_FIN_EX_DT
    	ON FACT.LATEST_SCHLD_FINISH_EXCT_DATE_K = LAT_SC_FIN_EX_DT.DATE_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[LATEST_SCHLD_START_EXEC_DATE] AS LAT_SC_SRT_EX_DT												
    	ON FACT.LATEST_SCHLD_START_EXCT_DATE_KE	= LAT_SC_SRT_EX_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[LOCATION_PLANT] AS LOC_PLNT
    	ON FACT.LOC_PLNT_KEY = LOC_PLNT.PLNT_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[MAINTENANCE_PLANT] AS MAIN_PLNT
    	ON FACT.MAINTENANCE_PLNT_KEY = MAIN_PLNT.PLNT_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANNING_PLANT] AS PLNN_PLNT
    	ON FACT.PLNG_PLNT_KEY = PLNN_PLNT.PLNT_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANT] AS PLT
    	ON FACT.PLNT_KEY = PLT.PLNT_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[RELEASE_DATE] AS REL_DT
    	ON FACT.RELS_DATE_KEY = REL_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[RESP_COST_CENTER] AS REP_CT_CNTR
    	ON FACT.RESPNSBLE_COST_CNTR_KEY= REP_CT_CNTR.COST_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SCHLD_FINISH_DATE] AS SC_FIN_DT
    	ON FACT.SCHLD_FINISH_DATE_KEY= SC_FIN_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SCHLD_RELEASE_DATE] AS SC_REL_DT
    	ON FACT.SCHLD_RELS_DATE_KEY = SC_REL_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[SCHLD_START_DATE] AS SC_SRT_DT
    	ON FACT.SCHLD_START_DATE_KEY = SC_SRT_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[START_CONSTRAINT_DATE] AS SRT_CON_DT
    	ON FACT.START_CNSTRNT_DATE_KEY = SRT_CON_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[START_OF_MALFUNCTION_DATE] AS SRT_MAL_DT
    	ON FACT.START_OF_MALFUNCTION_DATE_KEY = SRT_MAL_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[TECH_COMPLETION_DATE] AS TECH_COM_DT
    	ON FACT.TCHNCL_COMPLETION_DATE_KEY = TECH_COM_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PLANNED_RELEASE_DATE] AS PLNN_REL_DT
    	ON FACT.PLND_RELS_DATE_KEY = PLNN_REL_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[PERSONNEL_NUMBER] AS PER_NUM
    	ON FACT.PERSONEL_NUM_KEY = PER_NUM.PERSONEL_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[OPERATING_PLANT] AS OPR_PLT
    	ON FACT.OPER_PLANT_KEY = OPR_PLT.PLNT_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[CREATED_ON_DATE] AS CRT_DT
    	ON FACT.CREATED_ON_DATE_KEY = CRT_DT.DATE_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[OPERATION_LEVEL_WORK_CENTER] AS OPR_LEV_WK_CNTR
    	ON FACT.OPERATION_LVL_WRK_CNTR_KEY = OPR_LEV_WK_CNTR.WRK_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[Propelis].[EMPLOYEE_RESPONSIBLE] AS EMP_RESP
    	ON FACT.EMPL_RESPNSBLE_KEY = EMP_RESP.PERSONEL_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_R_MST_SGK_STNDRD_TXT_CATG_CUR_D] AS SGK_STNDRD
    	ON FACT.TASK_TYPE_DESC = SGK_STNDRD.STANDARD_TEXT_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_B_PRD_SVORDPTR_CUR_D] AS SVORDPTR
    	ON FACT.SERVC_ORDER = SVORDPTR.SERVC_ORDER
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_EQUIPMT_CUR_D] AS EQUIPMT
    	ON FACT.EQUIP_KEY= EQUIPMT.EQUIP_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCLOC_CUR_D] AS FUNCLOC
    	ON FACT.FUNCTNL_LOC_KEY= FUNCLOC.FUNCTNL_LOC_KEY
    	
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_FUNCTAR_CUR_D] AS FUNCTAR
    	ON FACT.FUNCTNL_AREA_KEY= FUNCTAR.FUNCTNL_AREA_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_NOTFICN_CUR_D] AS NOTFICN
    	ON FACT.NOTIFICATION_KEY = NOTFICN.NOTIFICATION_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
    	ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
    	ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRD_SERVORD_CUR_D] AS SERVORD
    	ON FACT.SERVC_ORDER_KEY = SERVORD.SERVC_ORDER_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_WORKCTR_CUR_D] AS WORKCTR
    	ON FACT.WRK_CNTR_KEY = WORKCTR.WRK_CNTR_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
    	ON FACT.SERVC_MATRL_KEY = MATERIAL.MATRL_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_ACTTYPE_CUR_D] AS ACTTYPE
    	ON FACT.ACTIVITY_TYP_KEY = ACTTYPE.ACTIVITY_TYP_KEY
    
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] AS CUSTMINR
    	ON FACT.CUST_MATRL_INFO_RCD_KEY = CUSTMINR.CUST_MATRL_INFO_RCD_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_B_SAL_SOIPTNR_CUR_D] AS SOIPTNR
	    ON FACT.SALES_ORDER_ID = SOIPTNR.SALES_ORDER_ID
	    AND FACT.SALES_ORDER_ITM = SOIPTNR.SALES_ORDER_ITM_ID
	  
	LEFT JOIN [GLOBAL_EDW].[Propelis].[EDW_V_D_SAL_PRDVCON_CUR_D] AS PRDVCON
	    ON FACT.SALES_ORDER_ID = PRDVCON.SALES_ORDER_ID
		AND FACT.SALES_ORDER_ITM = PRDVCON.SALES_ORDER_ITM_ID;
      
END
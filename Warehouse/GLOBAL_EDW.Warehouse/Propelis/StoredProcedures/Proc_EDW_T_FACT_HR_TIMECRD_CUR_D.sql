CREATE PROCEDURE [Propelis].[Proc_EDW_T_FACT_HR_TIMECRD_CUR_D]
AS  
BEGIN  
    -- Step 1: Clear existing data from the target table
	
    TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_HR_TIMECRD_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
 
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_HR_TIMECRD_CUR_D]
(    
    [PERSONEL_NUM_KEY],
    [DATE_KEY],
    [TOTAL_HRS],
    [WRK_HRS],
    [ABSNE_HRS],
    [LUNCH_HRS],
    [OVERTIME_HRS],
    [Shift Start Time],
    [SHIFT_START_TM_2],
    [Shift End Time],
    [SHIFT_END_TM_2],
    [APPRVD_WORKING_HRS],
    [Shift Scheduled Start Time],
    [Shift Scheduled End Time],
    [Comments],
    [ETL_SRC_SYS_CD],
    [ETL_CREATED_TS],
    [ETL_UPDTD_TS],
    [COST_CNTR_KEY],
    [CMPNY_KEY],
    [PLNT_KEY],
    [PERSONEL_AREA_KEY],
    [FLEX_BAL_TS],
    [Approver User ID],
    [PAID_BRK],
    [ANNUAL_LEAVE_HRS],
    [OTHR_LEAVE_HRS],
    [SICK_LEAVE_HRS],
    [UNPAID_LEAVE_HRS],
    [PUNCH_IN_OUT_HRS],
        
    --Alias Added Key List 
    [SGK_OPERREG_PLANT],
    [COSTCVZ_COST_CNTR_KEY],
    [COSTCHZ_COST_CNTR_KEY]
    )
SELECT 
        FACT.[PERSONEL_NUM_KEY],
        FACT.[DATE_KEY],
        FACT.[TOTAL_HRS],
        FACT.[WRK_HRS],
        FACT.[ABSNE_HRS],
        FACT.[LUNCH_HRS],
        FACT.[OVERTIME_HRS],
        FACT.[SHIFT_START_TM],
        FACT.[SHIFT_START_TM_2],
        FACT.[SHIFT_END_TM],
        FACT.[SHIFT_END_TM_2],
        FACT.[APPRVD_WORKING_HRS],
        FACT.[SHIFT_SCHLD_START_TM],
        FACT.[SHIFT_SCHLD_END_TM],
        FACT.[HDR_COMMENTS],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[COST_CNTR_KEY],
        FACT.[CMPNY_KEY],
        FACT.[PLNT_KEY],
        FACT.[PERSONEL_AREA_KEY],
        FACT.[FLEX_BAL_TS],
        FACT.[APPROVER_USERID],
        FACT.[PAID_BRK],
        FACT.[ANNUAL_LEAVE_HRS],
        FACT.[OTHR_LEAVE_HRS],
        FACT.[SICK_LEAVE_HRS],
        FACT.[UNPAID_LEAVE_HRS],
        FACT.[PUNCH_IN_OUT_HRS],
        
        --Alias Added Key List
        SUBSTRING(PERSONL.[Personnel Cost Center ID],4,4) AS SGK_OPERREG_PLANT,
        COSCNTR.COST_CNTR_KEY AS COSTCVZ_COST_CNTR_KEY,
        COSCNTR.COST_CNTR_KEY AS COSTCHZ_COST_CNTR_KEY
        
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_HR_TIMECRD_CUR_D] AS FACT

    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_DATE_CUR_D] AS MST_DATE 
        ON FACT.[DATE_KEY] = MST_DATE.[DATE_KEY]
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERSONL_CUR_D] AS PERSONL
        ON FACT.[PERSONEL_NUM_KEY] = PERSONL.[PERSONEL_KEY] 
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSCNTR_CUR_D] AS COSCNTR
        ON FACT.[COST_CNTR_KEY] = COSCNTR.[COST_CNTR_KEY]
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
        ON FACT.[CMPNY_KEY] = CMPNYCD.[CMPNY_KEY]
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
        ON FACT.[PLNT_KEY] = PLANT.[PLNT_KEY]
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PERAREA_CUR_D] AS PERAREA
        ON FACT.[PERSONEL_AREA_KEY] = PERAREA.[PERSONEL_AREA_KEY];
END;
-- Create or alter the stored procedure for updating and inserting records 
-- into the EDW_T_FACT_PRD_TMCONFS_CUR_D table in the Propelis schema
CREATE   PROCEDURE [Propelis].[Proc_EDW_T_FACT_PRD_TMCONFS_CUR_D]
AS
BEGIN
    
    -- Step 1: Update existing records in the target table (T) 
    -- with values from the source table (S) based on matching keys
    UPDATE T
    SET
        T.[SEG_CD_KEY] = S.[SEG_CD_KEY],                                -- Update segment code key
        T.[Operation Task List ID] = S.[Operation Task List ID],        -- Update operation task list ID
        T.[Operation Counter ID] = S.[Operation Counter ID],            -- Update operation counter ID
        T.[Operation Activity ID] = S.[Operation Activity ID],          -- Update operation activity ID
        T.[Operation Confirmation ID] = S.[Operation Confirmation ID],  -- Update operation confirmation ID
        T.[Operation Sequence ID] = S.[Operation Sequence ID],  -- Update operation sequence ID
        T.[PROFT_CNTR_KEY] = S.[PROFT_CNTR_KEY],  -- Update profit center key
        T.[SALES_ORDER_KEY] = S.[SALES_ORDER_KEY],  -- Update sales order key
        T.[Sales Order Item Number] = S.[Sales Order Item Number],  -- Update sales order item number
        T.[CMPNY_KEY] = S.[CMPNY_KEY],  -- Update company key
        T.[WRK_CNTR_KEY] = S.[WRK_CNTR_KEY],  -- Update work center key
        T.[COST_CNTR_KEY] = S.[COST_CNTR_KEY],  -- Update cost center key
        T.[OPERATION_STD_TEXT] = S.[OPERATION_STD_TEXT],  -- Update operation standard text
        T.[Operation Short Text] = S.[Operation Short Text],  -- Update operation short text
        T.[PSTNG_DATE_KEY] = S.[PSTNG_DATE_KEY],  -- Update posting date key
        T.[PLNG_PLNT_KEY] = S.[PLNG_PLNT_KEY],  -- Update planning plant key
        T.[Entry Create Date] = S.[Entry Create Date],  -- Update entry creation date
        T.[Person Who Entererd] = S.[Person Who Entererd],  -- Update person who entered the record
        T.[Operation Confirm Counter] = S.[Operation Confirm Counter],  -- Update operation confirm counter
        T.[Actual Work] = S.[Actual Work],  -- Update actual work
        T.[Actual Work UOM] = S.[Actual Work UOM],  -- Update actual work unit of measure
        T.[Accounting Indicator] = S.[Accounting Indicator],  -- Update accounting indicator
        T.[Remaining Work] = S.[Remaining Work],  -- Update remaining work
        T.[Remaining Work UOM] = S.[Remaining Work UOM],  -- Update remaining work unit of measure
        T.[Actual Work in Hours] = S.[Actual Work in Hours],  -- Update actual work in hours
        T.[Remaining Work in Hours] = S.[Remaining Work in Hours],  -- Update remaining work in hours
        T.[Work In Hours UOM] = S.[Work In Hours UOM],  -- Update work in hours unit of measure
        T.[ACTIVITY_TYP_KEY] = S.[ACTIVITY_TYP_KEY],  -- Update activity type key
        T.[Operation Confirm Text] = S.[Operation Confirm Text],  -- Update operation confirm text
        T.[Reason for Variance] = S.[Reason for Variance],  -- Update reason for variance
        T.[Confirm  Counter  Of  Cancelled  Confirmation] = S.[Confirm  Counter  Of  Cancelled  Confirmation],  -- Update confirm counter of cancelled confirmation
        T.[SOLD_TO_CUST_KEY] = S.[SOLD_TO_CUST_KEY],  -- Update sold to customer key
        T.[PERSONEL_NUM_KEY] = S.[PERSONEL_NUM_KEY],  -- Update personnel number key
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],  -- Update ETL source system code
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],  -- Update ETL created timestamp
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],  -- Update ETL updated timestamp
        T.[SERVC_ORDER_KEY] = S.[SERVC_ORDER_KEY],  -- Update service order key
        T.[SALES_AREA_KEY] = S.[SALES_AREA_KEY],  -- Update sales area key
        T.[OPERATING_PLNT_KEY] = S.[OPERATING_PLNT_KEY],  -- Update operating plant key
        T.[SERVC_ORDER_CREATE_DATE_KEY] = S.[SERVC_ORDER_CREATE_DATE_KEY],  -- Update service order create date key
        T.[Account Indicator] = S.[Account Indicator],  -- Update account indicator
        T.[PARNT_CUST_KEY] = S.[PARNT_CUST_KEY],  -- Update parent customer key
        T.[Reason for Variance Description] = S.[Reason for Variance Description],  -- Update reason for variance description
        T.[Partial Final Confirm] = S.[Partial Final Confirm],  -- Update partial final confirm
        T.[TCHNCL_COMPLETION_DATE_KEY] = S.[TCHNCL_COMPLETION_DATE_KEY],  -- Update technical completion date key
        T.[SALES_ORDER_ID] = S.[SALES_ORDER_ID],  -- Update sales order ID
        T.[Accounting Indicator Description] = S.[Accounting Indicator Description],  -- Update accounting indicator description
        T.[SERV_ORDER_INVOICE_CREATED_DATE_KEY] = S.[SERV_ORDER_INVOICE_CREATED_DATE_KEY],  -- Update service order invoice created date key
        T.[OPERATING_PLANT_REGION_PLANT] = S.[OPERATING_PLANT_REGION_PLANT],  -- Update operating plant region plant
        T.[PLANNING_PLANT_REGION_PLANT] = S.[PLANNING_PLANT_REGION_PLANT],  -- Update planning plant region plant
        T.[PERSONNEL_PLANT_REGION_PLANT] = S.[PERSONNEL_PLANT_REGION_PLANT],  -- Update personnel plant region plant
        T.[PARENT_CUST_KEY] = S.[PARENT_CUST_KEY],  -- Update parent customer key
        T.[PAYER_CUST_KEY] = S.[PAYER_CUST_KEY],  -- Update payer customer key
        T.[SHIP_TO_CUST_KEY] = S.[SHIP_TO_CUST_KEY],  -- Update ship to customer key
        T.[BRAND_OWNER_CUST_KEY] = S.[BRAND_OWNER_CUST_KEY],  -- Update brand owner customer key
        T.[CUST_CONTACT_KEY] = S.[CUST_CONTACT_KEY],  -- Update customer contact key
        T.[PERSON_RESP_PERSONNEL_KEY] = S.[PERSON_RESP_PERSONNEL_KEY],  -- Update person responsible personnel key
        T.[CYLINDER_OWNER_CUST_KEY] = S.[CYLINDER_OWNER_CUST_KEY],  -- Update cylinder owner customer key
        T.[AGENCY_CUST_KEY] = S.[AGENCY_CUST_KEY],  -- Update agency customer key
        T.[PRINTER_CUST_KEY] = S.[PRINTER_CUST_KEY],  -- Update printer customer key
        T.[SUPPLIER_CUST_KEY] = S.[SUPPLIER_CUST_KEY],  -- Update supplier customer key
        T.[EMPLOYEE_RESP_PERSONEL_KEY] = S.[EMPLOYEE_RESP_PERSONEL_KEY],  -- Update employee responsible personnel key
        T.[EMPLOYEE_RESP_2_PERSONEL_KEY] = S.[EMPLOYEE_RESP_2_PERSONEL_KEY],  -- Update second employee responsible personnel key
        T.[FORWARDING_AGENT_VENDOR_KEY] = S.[FORWARDING_AGENT_VENDOR_KEY],  -- Update forwarding agent vendor key
        T.[UNITIZATION_AGENT_VENDOR_KEY] = S.[UNITIZATION_AGENT_VENDOR_KEY],  -- Update unitization agent vendor key
        T.[ORDER_FOR_CUSTOMER_ADDR_KEY] = S.[ORDER_FOR_CUSTOMER_ADDR_KEY],  -- Update order for customer address key
        T.[SO_PARENT_CUST_KEY] = S.[SO_PARENT_CUST_KEY],  -- Update sales order parent customer key
        T.[BILL_TO_CUST_KEY] = S.[BILL_TO_CUST_KEY],  -- Update bill to customer key
        T.[CUSTOMER_PROJECT_MANAGER_CUST_CONTACT_KEY] = S.[CUSTOMER_PROJECT_MANAGER_CUST_CONTACT_KEY],  -- Update customer project manager contact key
        T.[SALES_PERSON_PERSONEL_KEY] = S.[SALES_PERSON_PERSONEL_KEY],  -- Update sales person personnel key
        T.[PRDT_VARIANT_CONFIG_KEY] = S.[PRDT_VARIANT_CONFIG_KEY],  -- Update product variant configuration key
        T.[SOIPTNR_KEY] = S.[SOIPTNR_KEY]  -- Update sales order item partner key
    FROM 
        [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_TMCONFS_CUR_D] T
    INNER JOIN 
        [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_TMCONFS_CUR_D] S
    ON 
        T.[SEG_CD_KEY] = S.[SEG_CD_KEY] AND
        T.[Operation Task List ID] = S.[Operation Task List ID] AND
        T.[Operation Counter ID] = S.[Operation Counter ID] AND
        T.[Operation Confirmation ID] = S.[Operation Confirmation ID] AND
        T.[CMPNY_KEY] = S.[CMPNY_KEY] AND
        T.[Operation Confirm Counter] = S.[Operation Confirm Counter] AND
        T.[SERVC_ORDER_KEY] = S.[SERVC_ORDER_KEY];
            
    -- Step 2: Insert new records from the temporary table (S) 
    -- into the target table (T) where the service order key does not exist
    INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_TMCONFS_CUR_D] (
        [SEG_CD_KEY],
        [Operation Task List ID],
        [Operation Counter ID],
        [Operation Activity ID],
        [Operation Confirmation ID],
        [Operation Sequence ID],
        [PROFT_CNTR_KEY],
        [SALES_ORDER_KEY],
        [Sales Order Item Number],
        [CMPNY_KEY],
        [WRK_CNTR_KEY],
        [COST_CNTR_KEY],
        [OPERATION_STD_TEXT],
        [Operation Short Text],
        [PSTNG_DATE_KEY],
        [PLNG_PLNT_KEY],
        [Entry Create Date],
        [Person Who Entererd],
        [Operation Confirm Counter],
        [Actual Work],
        [Actual Work UOM],
        [Accounting Indicator],
        [Remaining Work],
        [Remaining Work UOM],
        [Actual Work in Hours],
        [Remaining Work in Hours],
        [Work In Hours UOM],
        [ACTIVITY_TYP_KEY],
        [Operation Confirm Text],
        [Reason for Variance],
        [Confirm  Counter  Of  Cancelled  Confirmation],
        [SOLD_TO_CUST_KEY],
        [PERSONEL_NUM_KEY],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [SERVC_ORDER_KEY],
        [SALES_AREA_KEY],
        [OPERATING_PLNT_KEY],
        [SERVC_ORDER_CREATE_DATE_KEY],
        [Account Indicator],
        [PARNT_CUST_KEY],
        [Reason for Variance Description],
        [Partial Final Confirm],
        [TCHNCL_COMPLETION_DATE_KEY],
        [SALES_ORDER_ID],
        [Accounting Indicator Description],
        [SERV_ORDER_INVOICE_CREATED_DATE_KEY],
        [OPERATING_PLANT_REGION_PLANT],
        [PLANNING_PLANT_REGION_PLANT],
        [PERSONNEL_PLANT_REGION_PLANT],
        [PARENT_CUST_KEY],
        [PAYER_CUST_KEY],
        [SHIP_TO_CUST_KEY],
        [BRAND_OWNER_CUST_KEY],
        [CUST_CONTACT_KEY],
        [PERSON_RESP_PERSONNEL_KEY],
        [CYLINDER_OWNER_CUST_KEY],
        [AGENCY_CUST_KEY],
        [PRINTER_CUST_KEY],
        [SUPPLIER_CUST_KEY],
        [EMPLOYEE_RESP_PERSONEL_KEY],
        [EMPLOYEE_RESP_2_PERSONEL_KEY],
        [FORWARDING_AGENT_VENDOR_KEY],
        [UNITIZATION_AGENT_VENDOR_KEY],
        [ORDER_FOR_CUSTOMER_ADDR_KEY],
        [SO_PARENT_CUST_KEY],
        [BILL_TO_CUST_KEY],
        [CUSTOMER_PROJECT_MANAGER_CUST_CONTACT_KEY],
        [SALES_PERSON_PERSONEL_KEY],
        [PRDT_VARIANT_CONFIG_KEY],
        [SOIPTNR_KEY]
    )
    SELECT
        S.[SEG_CD_KEY],
        S.[Operation Task List ID],
        S.[Operation Counter ID],
        S.[Operation Activity ID],
        S.[Operation Confirmation ID],
        S.[Operation Sequence ID],
        S.[PROFT_CNTR_KEY],
        S.[SALES_ORDER_KEY],
        S.[Sales Order Item Number],
        S.[CMPNY_KEY],
        S.[WRK_CNTR_KEY],
        S.[COST_CNTR_KEY],
        S.[OPERATION_STD_TEXT],
        S.[Operation Short Text],
        S.[PSTNG_DATE_KEY],
        S.[PLNG_PLNT_KEY],
        S.[Entry Create Date],
        S.[Person Who Entererd],
        S.[Operation Confirm Counter],
        S.[Actual Work],
        S.[Actual Work UOM],
        S.[Accounting Indicator],
        S.[Remaining Work],
        S.[Remaining Work UOM],
        S.[Actual Work in Hours],
        S.[Remaining Work in Hours],
        S.[Work In Hours UOM],
        S.[ACTIVITY_TYP_KEY],
        S.[Operation Confirm Text],
        S.[Reason for Variance],
        S.[Confirm  Counter  Of  Cancelled  Confirmation],
        S.[SOLD_TO_CUST_KEY],
        S.[PERSONEL_NUM_KEY],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[SERVC_ORDER_KEY],
        S.[SALES_AREA_KEY],
        S.[OPERATING_PLNT_KEY],
        S.[SERVC_ORDER_CREATE_DATE_KEY],
        S.[Account Indicator],
        S.[PARNT_CUST_KEY],
        S.[Reason for Variance Description],
        S.[Partial Final Confirm],
        S.[TCHNCL_COMPLETION_DATE_KEY],
        S.[SALES_ORDER_ID],
        S.[Accounting Indicator Description],
        S.[SERV_ORDER_INVOICE_CREATED_DATE_KEY],
        S.[OPERATING_PLANT_REGION_PLANT],
        S.[PLANNING_PLANT_REGION_PLANT],
        S.[PERSONNEL_PLANT_REGION_PLANT],
        S.[PARENT_CUST_KEY],
        S.[PAYER_CUST_KEY],
        S.[SHIP_TO_CUST_KEY],
        S.[BRAND_OWNER_CUST_KEY],
        S.[CUST_CONTACT_KEY],
        S.[PERSON_RESP_PERSONNEL_KEY],
        S.[CYLINDER_OWNER_CUST_KEY],
        S.[AGENCY_CUST_KEY],
        S.[PRINTER_CUST_KEY],
        S.[SUPPLIER_CUST_KEY],
        S.[EMPLOYEE_RESP_PERSONEL_KEY],
        S.[EMPLOYEE_RESP_2_PERSONEL_KEY],
        S.[FORWARDING_AGENT_VENDOR_KEY],
        S.[UNITIZATION_AGENT_VENDOR_KEY],
        S.[ORDER_FOR_CUSTOMER_ADDR_KEY],
        S.[SO_PARENT_CUST_KEY],
        S.[BILL_TO_CUST_KEY],
        S.[CUSTOMER_PROJECT_MANAGER_CUST_CONTACT_KEY],
        S.[SALES_PERSON_PERSONEL_KEY],
        S.[PRDT_VARIANT_CONFIG_KEY],
        S.[SOIPTNR_KEY]
    FROM 
        [GLOBAL_EDW].[Propelis].[TEMP_EDW_T_FACT_PRD_TMCONFS_CUR_D] S
    LEFT JOIN 
        [GLOBAL_EDW].[Propelis].[EDW_T_FACT_PRD_TMCONFS_CUR_D] T
    ON 
        T.[SEG_CD_KEY] = S.[SEG_CD_KEY] AND
        T.[Operation Task List ID] = S.[Operation Task List ID] AND
        T.[Operation Counter ID] = S.[Operation Counter ID] AND
        T.[Operation Confirmation ID] = S.[Operation Confirmation ID] AND
        T.[CMPNY_KEY] = S.[CMPNY_KEY] AND
        T.[Operation Confirm Counter] = S.[Operation Confirm Counter] AND
        T.[SERVC_ORDER_KEY] = S.[SERVC_ORDER_KEY]
    WHERE 
        -- Only select records from S that do not have a corresponding record in T
        T.[SEG_CD_KEY] IS NULL;
END

-- Execute the stored procedure to perform the update and insert operations
EXEC [Propelis].[Proc_EDW_T_FACT_PRD_TMCONFS_CUR_D]
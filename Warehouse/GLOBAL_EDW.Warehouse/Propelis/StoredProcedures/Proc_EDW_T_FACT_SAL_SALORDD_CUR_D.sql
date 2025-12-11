CREATE         PROCEDURE [Propelis].[Proc_EDW_T_FACT_SAL_SALORDD_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_SALORDD_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_SAL_SALORDD_CUR_D](
	    [SALES_ORDER_KEY],
        [Sales Order ID],
        [Sales Order Item ID],
        [DOCMT_DATE_KEY],
        [REQD_DLVRY_DATE_KEY],
        [CMPNY_KEY],
        [PLNT_KEY],
        [SOLDTO_CUST_KEY],
        [BILLTO_CUST_KEY],
        [PAYER_CUST_KEY],
        [SHIPTO_CUST_KEY],
        [BRAND_OWNER_CUST_KEY],
        [CUST_CONTACT_KEY],
        [SALES_PERSON_PERSONEL_KEY],
        [PERSON_RESPNSBLE_PERSONEL_KEY],
        [MATRL_KEY],
        [PROFT_CNTR_KEY],
        [CUST_CMPNY_KEY],
        [CUST_SALES_AREA_KEY],
        [SEG_KEY],
        [SALES_AREA_KEY],
        [DOCMT_CRNCY_KEY],
        [CMPNY_CRNCY_KEY],
        [GROUP_CRNCY_KEY],
        [SALES_ORDER_QTY],
        [SO Unit Of Measure],
        [DOCMT_CRNCY_NET_PRICE],
        [DOCMT_CRNCY_NET_VAL_AMT],
        [DOCMT_CRNCY_TAX_AMT],
        [CMPNY_CRNCY_NET_VAL_AMT],
        [GROUP_CRNCY_NET_VAL_AMT],
        [SO Shipping Point],
        [SO Reason For Rejection],
        [SO Overall Status],
        [SO Delivery Status],
        [SO Order Related Bill Status],
        [Sales Order Header Encoway Id],
        [SO Encoway Version],
        [SO Encoway Part],
        [SO Encoway Description],
        [SO Encoway Number],
        [SO Material Description],
        [SO Delivery Group],
        [QTY_NETTED],
        [SO Reason For Rejection Description],
        [SO Shipping Condition],
        [SO Inco Terms],
        [SO Inco Terms Description],
        [SO Inco Terms 2],
        [SO Billing Date],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [CYLINDER_OWNER_CUST_KEY],
        [SO Usage],
        [SO Higher-Level Item],
        [SO Alternative to Item],
        [MATRL_ENTERED_KEY],
        [SO Reference Document],
        [SO Reference Item],
        [SO Reference Status],
        [SO Total Reference Status],
        [SO Billing Block],
        [PERSON_RESPNSBLE_COST_CNTR_KEY],
        [PARNT_CUST_KEY],
        [CREATION_DATE_KEY],
        [SO Creation Timestamp],
        [SO Delivery Status Indicator],
        [SO Sales UoM],
        [SALES_TRGT_QTY],
        [SO Target Quantity UoM],
        [DLVRD_QTY],
        [SO Is Sales Schedule Line Created],
        [DOCMT_CHARS_ENCOWAY_JS_HDR_KEY],
        [EMPL_RESPNSBLE_2_KEY],
        [SO BOM Item Number],
        [CUST_MATRL_INFO_RCD_KEY],
        [SO Changed On Date],
        [SO Relevant For Billing],
        [SO Item Is Relevant For Delivery],
        [WBS_ELEMNT_KEY],
        [AGENCY_CUST_KEY],
        [PRINTER_CUST_KEY],
        [SUPLR_CUST_KEY],
        [SO Billing Status Of Delivery],
        [SO Approval],
        [SO Arrangement],
        [SO Delivery],
        [SO Fixed DET],
        [RSLTS_ANALYSIS_KEY],
        [SO Purchase Order Item Number],
        [SO Line Item System Status],
        [SO Overall Status Description],
        [Delivery Status Description],
        [SO Order Related Bill Status Description],
        [SO Shipping Condition Description],
        [SO Usage Description],
        [SO Reference Status Description],
        [SO Total Reference Status Description],
        [SO Billing Block Description],
        [SO Delivery Status Indicator Description],
        [SO Relevant For Billing Description],
        [SO Billing Status Of Delivery Description],
        [SO Results Analysis Description],
        [SO Internal Customer Reference Number],
        [Product Hierarchy],
        [SO Sold To Purchase Order Number],
        [SO Sold To Purchase Order Date],
        [SO Ship To Purchase Order Number],
        [SO Ship To Purchase Order Date],
        [SO Customer Condition Group 1],
        [SO Customer Condition Group 1 Description],
        [SO Customer Material Number],
        [EMPL_RESPNSBLE_KEY],
        [CUST_POJECT_MGR_KEY],
        [SO Item Category],
        [SO Item Category Description],
        [SHIP_TO_ADDR_KEY],
        [SO Delivery Priority Code],
        [SO Delivery Priority Description],
        [CREATED_BY],
        [UNITIZATION_AGENT_VNDR_KEY],
        [SO Designable E Services Flag],
        [SO Agreed Delivery Time],
        [SO Deceased Name],
        [SO Customer Purchase Order Type],
        [SO Customer Purchase Order Type Description],
        [ORDER_KEY],
        [SO Price Group],
        [SO Price Group Description],
        [CMLT_CONFIRMED_QTY_IN_BASE_UNIT],
        [SO Base Unit Of Measure],
		
	--Below columns are added for the Alias tables	
		
		[PROFCHZ_PROFT_CNTR_KEY],
        [PROFCVZ_PROFT_CNTR_KEY],
        [COMPANY_INFO_CMPNY_CD_KEY],
		[PERSON_RESPNSBLE_COST_CNTR_H_HEIR_KEY],
		[PERSON_RESPNSBLE_COST_CNTR_V_KEY],
        [SAL_SALORDH_INVOICE_CREATION_DATE_KEY],
        [SAL_SALORDH_CREATED_DATE_KEY],
        [PLANT_REGION_PLANT_ID],
		[FORWARDING_AGENT_VENDOR_KEY],
		[ORDER_FOR_CUSTOMER_ADDR_KEY],
		[SO_PARENT_CUST_KEY],
		[SOIPTNR_KEY],
        [V_D_SAL_PRDVCON_KEY]
        
	)
	SELECT
	    FACT.[SALES_ORDER_KEY],
        FACT.[SALES_ORDER_ID],
        FACT.[SALES_ORDER_ITM_ID],
        FACT.[DOCMT_DATE_KEY],
        FACT.[REQD_DLVRY_DATE_KEY],
        FACT.[CMPNY_KEY],
        FACT.[PLNT_KEY],
        FACT.[SOLDTO_CUST_KEY],
        FACT.[BILLTO_CUST_KEY],
        FACT.[PAYER_CUST_KEY],
        FACT.[SHIPTO_CUST_KEY],
        FACT.[BRAND_OWNER_CUST_KEY],
        FACT.[CUST_CONTACT_KEY],
        FACT.[SALES_PERSON_PERSONEL_KEY],
        FACT.[PERSON_RESPNSBLE_PERSONEL_KEY],
        FACT.[MATRL_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[CUST_CMPNY_KEY],
        FACT.[CUST_SALES_AREA_KEY],
        FACT.[SEG_KEY],
        FACT.[SALES_AREA_KEY],
        FACT.[DOCMT_CRNCY_KEY],
        FACT.[CMPNY_CRNCY_KEY],
        FACT.[GROUP_CRNCY_KEY],
        FACT.[SALES_ORDER_QTY],
        FACT.[SALES_ORDER_UOM],
        FACT.[DOCMT_CRNCY_NET_PRICE],
        FACT.[DOCMT_CRNCY_NET_VAL_AMT],
        FACT.[DOCMT_CRNCY_TAX_AMT],
        FACT.[CMPNY_CRNCY_NET_VAL_AMT],
        FACT.[GROUP_CRNCY_NET_VAL_AMT],
        FACT.[SHIPG_PNT],
        FACT.[RSN_FOR_REJN],
        FACT.[OVERLL_STATUS],
        FACT.[DLVRY_STATUS],
        FACT.[ORDER_RELD_BILL_STATUS],
        FACT.[ENCOWAY_ID],
        FACT.[ENCOWAY_VRSN],
        FACT.[ENCOWAY_PART],
        FACT.[ENCOWAY_DESC],
        FACT.[ENCOWAY_NUM],
        FACT.[MATRL_DESC],
        FACT.[DLVRY_GROUP],
        FACT.[QTY_NETTED],
        FACT.[RSN_FOR_REJN_DESC],
        FACT.[SHIPG_COND],
        FACT.[INCO_TRMS],
        FACT.[INCO_TRMS_DESC],
        FACT.[INCO_TRMS_2],
        FACT.[BILLING_DATE],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[ETL_UPDTD_TS],
        FACT.[CYLINDER_OWNER_CUST_KEY],
        FACT.[USAGE],
        FACT.[HIGHER_LVL_ITM],
        FACT.[ALTERNATIVE_TO_ITM],
        FACT.[MATRL_ENTERED_KEY],
        FACT.[REF_DOCMT],
        FACT.[REF_ITM],
        FACT.[REF_STATUS],
        FACT.[TOTAL_REF_STATUS],
        FACT.[BILLING_BLK],
        FACT.[PERSON_RESPNSBLE_COST_CNTR_KEY],
        FACT.[PARNT_CUST_KEY],
        FACT.[CREATION_DATE_KEY],
        FACT.[CREATION_TS],
        FACT.[DLVRY_STATUS_IND],
        FACT.[SALES_UOM],
        FACT.[SALES_TRGT_QTY],
        FACT.[TRGT_QTY_UOM],
        FACT.[DLVRD_QTY],
        FACT.[IS_SALES_SCHL_LINE_CREATED],
        FACT.[DOCMT_CHARS_ENCOWAY_JS_HDR_KEY],
        FACT.[EMPL_RESPNSBLE_2_KEY],
        FACT.[BOM_ITM_NUM],
        FACT.[CUST_MATRL_INFO_RCD_KEY],
        FACT.[CHANGED_ON_DATE],
        FACT.[RELEVANT_FOR_BILLING],
        FACT.[ITM_IS_RELEVANT_FOR_DLVRY],
        FACT.[WBS_ELEMNT_KEY],
        FACT.[AGENCY_CUST_KEY],
        FACT.[PRINTER_CUST_KEY],
        FACT.[SUPLR_CUST_KEY],
        FACT.[BILLING_STATUS_OF_DLVRY],
        FACT.[APPROVAL],
        FACT.[ARRANGEMENT],
        FACT.[DLVRY],
        FACT.[FIXED_DET],
        FACT.[RSLTS_ANALYSIS_KEY],
        FACT.[PURCHS_ORDER_ITM_NUM],
        FACT.[SALES_ORDER_LINE_ITM_SYS_STATUS],
        FACT.[OVERLL_STATUS_DESC],
        FACT.[DLVRY_STATUS_DESC],
        FACT.[ORDER_RELD_BILL_STATUS_DESC],
        FACT.[SHIPG_COND_DESC],
        FACT.[USAGE_DESC],
        FACT.[REF_STATUS_DESC],
        FACT.[TOTAL_REF_STATUS_DESC],
        FACT.[BILLING_BLK_DESC],
        FACT.[DLVRY_STATUS_IND_DESC],
        FACT.[RELEVANT_FOR_BILLING_DESC],
        FACT.[BILLING_STATUS_OF_DLVRY_DESC],
        FACT.[RSLTS_ANALYSIS_DESC],
        FACT.[INTERNL_CUST_REF_NUM],
        FACT.[PRDT_HIERCHY_CD],
        FACT.[SOLDTO_PURCHS_ORDER_NUM],
        FACT.[SOLDTO_PURCHS_ORDER_DATE],
        FACT.[SHIPTO_PURCHS_ORDER_NUM],
        FACT.[SHIPTO_PURCHS_ORDER_DATE],
        FACT.[CUST_COND_GROUP_1],
        FACT.[CUST_COND_GROUP_1_DESC],
        FACT.[CUST_MATRL_NUM],
        FACT.[EMPL_RESPNSBLE_KEY],
        FACT.[CUST_POJECT_MGR_KEY],
        FACT.[SALES_ORDER_ITM_CTGRY],
        FACT.[SALES_ORDER_ITM_CTGRY_DESC],
        FACT.[SHIP_TO_ADDR_KEY],
        FACT.[DLVRY_PRIORITY_CD],
        FACT.[DLVRY_PRIORITY_DESC],
        FACT.[CREATED_BY],
        FACT.[UNITIZATION_AGENT_VNDR_KEY],
        FACT.[DESIGNABLE_E_SERVICES_FLG],
        FACT.[AGREED_DLVRY_TM],
        FACT.[DECEASED_NAME],
        FACT.[CUST_PURCHS_ORDER_TYP],
        FACT.[CUST_PURCHS_ORDER_TYP_DESC],
        FACT.[ORDER_KEY],
        FACT.[PRICE_GROUP],
        FACT.[PRICE_GROUP_DESC],
        FACT.[CMLT_CONFIRMED_QTY_IN_BASE_UNIT],
        FACT.[BASE_UOM],
		
	--Below columns are added for the Alias tables
	
	    PROFCTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
		PROFCTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY,
		SALORDH.[Sales Order Company Code] AS COMPANY_INFO_CMPNY_CD_KEY,
		PER_RESP_CT_CNTR.COST_CNTR_KEY AS PERSON_RESPNSBLE_COST_CNTR_H_HEIR_KEY,
		PER_RESP_CT_CNTR.COST_CNTR_KEY AS PERSON_RESPNSBLE_COST_CNTR_V_KEY,
        CASE WHEN CONVERT(VARCHAR, SAL_SALORDH.[Sales Order Invoice Create Date],112) = '19000101'
	       OR SAL_SALORDH.[Sales Order Invoice Create Date] IS NULL THEN '-1' ELSE 
           CONVERT(VARCHAR, SAL_SALORDH.[Sales Order Invoice Create Date],112) END 
		    AS SAL_SALORDH_INVOICE_CREATION_DATE_KEY,
        CASE WHEN CONVERT(VARCHAR,SAL_SALORDH.[Sales Order Created Date],112) = '19000101'  
	        OR SAL_SALORDH.[Sales Order Created Date] IS NULL THEN '-1' ELSE 
            CONVERT(VARCHAR, SAL_SALORDH.[Sales Order Created Date],112) END
		    AS SAL_SALORDH_CREATED_DATE_KEY,
		PLANT.[Plant ID] AS PLANT_REGION_PLANT_ID,
		SOIPTNR.FORWARDING_AGENT_VNDR_KEY AS FORWARDING_AGENT_VENDOR_KEY,
		SOIPTNR.ORDER_FOR_CUST_KEY AS ORDER_FOR_CUSTOMER_ADDR_KEY,
		SOIPTNR.SO_PARNT_CUST_KEY AS SO_PARENT_CUST_KEY,
		CONCAT(FACT.SALES_ORDER_ID, '-', FACT.SALES_ORDER_ITM_ID) AS SOIPTNR_KEY,
        PRDVCON.[PRDT_VARIANT_CONFIG_KEY] AS V_D_SAL_PRDVCON_KEY
		
    FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_SAL_SALORDD_CUR_D] AS FACT
	
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
	    ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCMP_CUR_D] AS CUSTCMP
	    ON FACT.CUST_CMPNY_KEY = CUSTCMP.CUST_CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTSAL_CUR_D] AS CUSTSAL
	    ON FACT.CUST_SALES_AREA_KEY = CUSTSAL.CUST_SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS SALAREA
	    ON FACT.SALES_AREA_KEY = SALAREA.SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
	    ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] AS SEGMENT
	    ON FACT.SEG_KEY = SEGMENT.SEG_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
	    ON FACT.PLNT_KEY = PLANT.PLNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PROFCTR_CUR_D] AS PROFCTR
	    ON FACT.PROFT_CNTR_KEY = PROFCTR.PROFT_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
	    ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_DCHAREN_CUR_D] AS DCHAREN
	    ON FACT.DOCMT_CHARS_ENCOWAY_JS_HDR_KEY = DCHAREN.DOCMT_CHARS_ENCOWAY_JS_HDR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTMINR_CUR_D] AS CUSTMINR
	    ON FACT.CUST_MATRL_INFO_RCD_KEY = CUSTMINR.CUST_MATRL_INFO_RCD_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRS_WBSELEM_CUR_D] AS WBSELEM
	    ON FACT.WBS_ELEMNT_KEY = WBSELEM.WBS_ELEMNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PRTHRCY_CUR_D] AS PRTHRCY
	    ON FACT.[PRDT_HIERCHY_CD ] = PRTHRCY.[Product Hierarchy]
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_ADDRESS_CUR_D] AS ADDRESS
	    ON FACT.SHIP_TO_ADDR_KEY = ADDRESS.ADDR_KEY
		 
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PM_ORDER_CUR_D] AS PM_ORDER
	    ON FACT.ORDER_KEY = PM_ORDER.ORDER_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_B_SAL_SOIPTNR_CUR_D] AS SOIPTNR
	    ON FACT.SALES_ORDER_ID = SOIPTNR.SALES_ORDER_ID
        AND
        FACT.SALES_ORDER_ITM_ID = SOIPTNR.SALES_ORDER_ITM_ID
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[EDW_V_D_SAL_PRDVCON_CUR_D] AS PRDVCON
	    ON FACT.SALES_ORDER_ID = PRDVCON.SALES_ORDER_ID
        AND
        FACT.SALES_ORDER_ITM_ID = PRDVCON.SALES_ORDER_ITM_ID
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_CURRENCY] AS DOC_CURNCY
	    ON FACT.DOCMT_CRNCY_KEY = DOC_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[GROUP_CURRENCY] AS GRP_CURNCY
        ON FACT.GROUP_CRNCY_KEY = GRP_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[COMPANY_CURRENCY] AS	CMP_CURNCY
	    ON FACT.CMPNY_CRNCY_KEY = CMP_CURNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[DOCUMENT_DATE] AS DOC_DT
	    ON FACT.DOCMT_DATE_KEY = DOC_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[REQD_DELVRY_DATE] AS RE_DEL_DT
	    ON FACT.REQD_DLVRY_DATE_KEY = RE_DEL_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[CREATION_DATE] AS CRT_DT
	    ON FACT.CREATION_DATE_KEY = CRT_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[MATRL_ENTERED] AS MAT_ENT
	    ON FACT.MATRL_ENTERED_KEY = MAT_ENT.MATRL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[PERSON_RESPNSBLE_COST_CNTR] AS PER_RESP_CT_CNTR
	    ON FACT.PERSON_RESPNSBLE_COST_CNTR_KEY = PER_RESP_CT_CNTR.COST_CNTR_KEY;
END
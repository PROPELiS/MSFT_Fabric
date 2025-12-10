CREATE   PROCEDURE [Propelis].[Proc_EDW_T_FACT_CO_COPA3_CUR_D]
AS
BEGIN

    -- Step 1: Clear existing data from the target table
	
	TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_COPA3_CUR_D];
	
	-- Step 2: Insert refreshed data from mirror table with joins
	
	INSERT INTO [GLOBAL_EDW].[Propelis].[EDW_T_FACT_CO_COPA3_CUR_D](
        
		[COPA Currency Type],
        [COPA Record Type],
        [COPA Version],
        [COPA Period Year],
        [COPA Profit Segment],
        [COPA Subnumber],
        [COPA Accounting Clerk Description],
        [COPA Sub Brand],
        [COPA Brand],
        [COPA Product Hierarchy Code],
        [COPA Base UoM Description],
        [COPA Region],
        [COPA Industry Code 1],
        [COPA Industry Code 1 Description],
        [COPA Industry Code 2],
        [COPA Industry Code 2 Description],
        [COPA Accounting Clerk],
        [COPA Customer Group Description],
        [CNTRY_KEY],
        [COPA Customer Group 2],
        [COPA Customer Group 2 Description],
        [COPA Material Group],
        [COPA Material Group Description],
        [COPA Distribution Channel Description],
        [COPA Division],
        [COPA Division Description],
        [INDUS_KEY],
        [COPA Industry Description],
        [COPA Customer Group],
        [COPA Customer Account Assignment Group Description],
        [MATRL_SALES_KEY],
        [BRAND_OWNER_CUST_KEY],
        [COPA Sales Organization],
        [COPA Sales Organization Description],
        [COPA Distribution Channel],
        [Customer PO Box Postal Code],
        [COPA Sales Office],
        [COPA Sales Office Description],
        [COPA Sales Group],
        [COPA Sales Group Description],
        [COPA Customer Account Assignment Group],
        [CUST_KEY],
        [COPA Commission Group Description],
        [COPA Order Reason],
        [COPA Order Reason Description],
        [COPA Sales District],
        [COPA Sales District Description],
        [COPA Invoice Type Description],
        [COPA Order Number],
        [INCOMING_FREIGHT_AMT],
        [OUTGOING_TRNSPRT_AMT],
        [SUBCONTRACTING_SERVC_AMT],
        [COPA Sales Document Description],
        [COPA Ship To Zip Code],
        [COPA Plan Actual Indicator],
        [COPA Plan Actual Indicator Description],
        [WBS_ELEMNT_KEY],
        [COPA Cancelled Document],
        [COPA Cancelled Document Item],
        [COPA Ship To Telephone],
        [TRMS_OF_PYMT_KEY],
        [COPA Terms of Payment Description],
        [COPA Ship To Address],
        [COPA Ship To Country],
        [COPA Ship To Region],
        [ETL_UPDTD_TS],
        [COPA Currency Type Description],
        [COPA Created By],
        [COPA Controlling Area],
        [COPA Controlling Area Description],
        [COPA Ship To City],
        [BILLTO_CUST_KEY],
        [CUST_SERVC_REPRESENTATIVE_KEY],
        [GOODS_ISSUE_DATE_KEY],
        [COPA Created On Date],
        [ETL_SRC_SYS_CD],
        [ETL_CREATED_TS],
        [FRIEGHT_REVNU_AMT],
        [OVERHEAD_VARIANCES_AMT],
        [COPA Order Quantity UoM],
        [COPA Commission Group],
        [CUST_SALES_AREA_KEY],
        [SALES_REP_REPRESENTATIVE_KEY],
        [CASHDISC_WO_TAX_AMT],
        [DISTRBN_EXPENSE_AMT],
        [INTERCO_MAT_COST_AMT],
        [OUTSIDE_PURCHASES_AMT],
        [MCHINE_COSTS_AMT],
        [COPA Sales Document Type],
        [CLOSEOUT_REVNU_AMT],
        [DAMAGED_REVNU_AMT],
        [SUBSTITUTION_REVNU_AMT],
        [PROMOTION_REVNU_AMT],
        [EXCEPTION_REVNU_AMT],
        [PRENEED_REVNU_AMT],
        [SELLING_EXPENSE_AMT],
        [GENERAL_ADMIN_AMT],
        [PROFT_DISTRBN_AMT],
        [DIVISION_SERVICES_AMT],
        [CASHDISCOUNT_ON_INVC_AMT],
        [SELLING_PRICE_AMT],
        [CORPORATE_SERVICES_AMT],
        [REBATES_ON_INVC_AMT],
        [FREIGHT_COST_AMT],
        [REALIZED_FX_AP_AR_AMT],
        [LABOR_SPEND_VRNC_AMT],
        [OVERHEAD_SPEND_VRNC_AMT],
        [Direct Material Amount],
        [OVERHEAD_AMT],
        [REBATES_AMT],
        [PROD_LABOUR_VRNC_AMT],
        [PROD_MAT_VRNC_AMT],
        [OTHR_MAT_VRNC_AMT],
        [MATRL_COST_AMT],
        [OVERHEAD_COST_AMT],
        [DISCNT_ON_INVC_AMT],
        [IC_MARKUP_AMT],
        [DIR_LABOR_AMT],
        [LIST_PRICE_AMT],
        [PRICE_VARIANCES_AMT],
        [OTHR_VARIANCES_AMT],
        [RESOURCE_USAGE_VARIANCES_AMT],
        [CASH_DISCNT_AMT],
        [STK_VAL_AMT],
        [LABOR_COST_AMT],
        [COPA Sales Order Line Item],
        [SO_QTY],
        [REVNU_AMT],
        [LOTSIZE_VRNC_AMT],
        [PROD_COSTS_VRNC_AMT],
        [QTY_VARIANCES_AMT],
        [COPA Invoice Header Number],
        [COPA Invoice Item Number],
        [COPA Year],
        [COPA Period],
        [COPA Invoice Type],
        [SALES_ORDER_KEY],
        [COST_ELEMNT_KEY],
        [MATRL_KEY],
        [PLNT_KEY],
        [SALES_AREA_KEY],
        [SEG_KEY],
        [CRNCY_KEY],
        [PARNT_CUST_KEY],
        [RCD_TYP_DESC],
        [SHIPTO_CUST_KEY],
        [CUST_CMPNY_KEY],
        [PROFT_CNTR_KEY],
        [PRTNR_PROFT_CNTR_KEY],
        [COPA Document Number],
        [COPA Document Item Number],
        [BILLING_DATE_KEY],
        [PSTNG_DATE_KEY],
        [CMPNY_KEY],
        [SOLDTO_CUST_KEY],
	
		--Below columns are added for the Alias tables
		
		[COSTEHZ_COST_ELEMNT_KEY],
		[COSTEVZ_COST_ELEMNT_KEY],
		[Partner_Profit_Center_Horizontal_PROFT_CNTR_KEY],
		[Partner_Profit_Center_Vertical_PROFT_CNTR_KEY],
		[PROFCHZ_PROFT_CNTR_KEY],
		[PROFCVZ_PROFT_CNTR_KEY]
		
    )
    SELECT
        FACT.[CRNCY_TYP],
        FACT.[RCD_TYP],
        FACT.[VRSN],
        FACT.[PER_YR],
        FACT.[PROFT_SEG],
        FACT.[SUBNUMBER],
        FACT.[ACCTNG_CLERK_DESC],
        FACT.[SUB_BRAND],
        FACT.[BRAND],
        FACT.[PRDT_HIERCHY_CD],
        FACT.[BASE_UOM_DESC],
        FACT.[REGN],
        FACT.[INDUS_CD_1],
        FACT.[INDUS_CD_1_DESC],
        FACT.[INDUS_CD_2],
        FACT.[INDUS_CD_2_DESC],
        FACT.[ACCTNG_CLERK],
        FACT.[CUST_GROUP_DESC],
        FACT.[CNTRY_KEY],
        FACT.[CUST_GROUP_2],
        FACT.[CUST_GROUP_2_DESC],
        FACT.[MATRL_GROUP],
        FACT.[MATRL_GROUP_DESC],
        FACT.[DISTRBN_CHNL_DESC],
        FACT.[DIVISION],
        FACT.[DIVISION_DESC],
        FACT.[INDUS_KEY],
        FACT.[INDUS_DESC],
        FACT.[CUST_GROUP],
        FACT.[ACCT_ASSIGN_GROUP_DESC],
        FACT.[MATRL_SALES_KEY],
        FACT.[BRAND_OWNER_CUST_KEY],
        FACT.[SALES_ORGZTN],
        FACT.[SALES_ORG_DESC],
        FACT.[DISTRBN_CHNL],
        FACT.[POSTAL_CD],
        FACT.[SALES_OFFC],
        FACT.[SALES_OFFC_DESC],
        FACT.[SALES_GROUP],
        FACT.[SALES_GROUP_DESC],
        FACT.[CUST_ACCT_ASSIGN_GROUP],
        FACT.[CUST_KEY],
        FACT.[COMM_GROUP_DESC],
        FACT.[ORDER_RSN],
        FACT.[ORDER_RSN_DESC],
        FACT.[SALES_DISTR],
        FACT.[SALES_DISTR_DESC],
        FACT.[INVC_TYP_DESC],
        FACT.[ORDER_NUM],
        FACT.[INCOMING_FREIGHT_AMT],
        FACT.[OUTGOING_TRNSPRT_AMT],
        FACT.[SUBCONTRACTING_SERVC_AMT],
        FACT.[SALES_DOCMT_DESC],
        FACT.[SHIPTO_ZIP_CD],
        FACT.[PLAN_ACTUAL_IND],
        FACT.[PLAN_ACTUAL_IND_DESC],
        FACT.[WBS_ELEMNT_KEY],
        FACT.[CANCELED_DOCMT],
        FACT.[CANCELED_DOCMT_ITM],
        FACT.[SHIPTO_TEL],
        FACT.[TRMS_OF_PYMT_KEY],
        FACT.[TRMS_OF_PYMT_DESC],
        FACT.[SHIPTO_ADDR],
        FACT.[SHIPTO_CNTRY],
        FACT.[SHIPTO_REGN],
        FACT.[ETL_UPDTD_TS],
        FACT.[CRNCY_TYP_DESC],
        FACT.[CREATED_BY],
        FACT.[CONTROLLING_AREA],
        FACT.[CONTROLLING_AREA_DESC],
        FACT.[SHIPTO_CITY],
        FACT.[BILLTO_CUST_KEY],
        FACT.[CUST_SERVC_REPRESENTATIVE_KEY],
        FACT.[GOODS_ISSUE_DATE_KEY],
        FACT.[CREATED_ON_DATE],
        FACT.[ETL_SRC_SYS_CD],
        FACT.[ETL_CREATED_TS],
        FACT.[FRIEGHT_REVNU_AMT],
        FACT.[OVERHEAD_VARIANCES_AMT],
        FACT.[ORDER_QTY_UOM],
        FACT.[COMM_GROUP],
        FACT.[CUST_SALES_AREA_KEY],
        FACT.[SALES_REP_REPRESENTATIVE_KEY],
        FACT.[CASHDISC_WO_TAX_AMT],
        FACT.[DISTRBN_EXPENSE_AMT],
        FACT.[INTERCO_MAT_COST_AMT],
        FACT.[OUTSIDE_PURCHASES_AMT],
        FACT.[MCHINE_COSTS_AMT],
        FACT.[SALES_DOCMT_TYP],
        FACT.[CLOSEOUT_REVNU_AMT],
        FACT.[DAMAGED_REVNU_AMT],
        FACT.[SUBSTITUTION_REVNU_AMT],
        FACT.[PROMOTION_REVNU_AMT],
        FACT.[EXCEPTION_REVNU_AMT],
        FACT.[PRENEED_REVNU_AMT],
        FACT.[SELLING_EXPENSE_AMT],
        FACT.[GENERAL_ADMIN_AMT],
        FACT.[PROFT_DISTRBN_AMT],
        FACT.[DIVISION_SERVICES_AMT],
        FACT.[CASHDISCOUNT_ON_INVC_AMT],
        FACT.[SELLING_PRICE_AMT],
        FACT.[CORPORATE_SERVICES_AMT],
        FACT.[REBATES_ON_INVC_AMT],
        FACT.[FREIGHT_COST_AMT],
        FACT.[REALIZED_FX_AP_AR_AMT],
        FACT.[LABOR_SPEND_VRNC_AMT],
        FACT.[OVERHEAD_SPEND_VRNC_AMT],
        FACT.[DIR_MATRL_AMT],
        FACT.[OVERHEAD_AMT],
        FACT.[REBATES_AMT],
        FACT.[PROD_LABOUR_VRNC_AMT],
        FACT.[PROD_MAT_VRNC_AMT],
        FACT.[OTHR_MAT_VRNC_AMT],
        FACT.[MATRL_COST_AMT],
        FACT.[OVERHEAD_COST_AMT],
        FACT.[DISCNT_ON_INVC_AMT],
        FACT.[IC_MARKUP_AMT],
        FACT.[DIR_LABOR_AMT],
        FACT.[LIST_PRICE_AMT],
        FACT.[PRICE_VARIANCES_AMT],
        FACT.[OTHR_VARIANCES_AMT],
        FACT.[RESOURCE_USAGE_VARIANCES_AMT],
        FACT.[CASH_DISCNT_AMT],
        FACT.[STK_VAL_AMT],
        FACT.[LABOR_COST_AMT],
        FACT.[SALES_ORDER_LINE_ITM],
        FACT.[SO_QTY],
        FACT.[REVNU_AMT],
        FACT.[LOTSIZE_VRNC_AMT],
        FACT.[PROD_COSTS_VRNC_AMT],
        FACT.[QTY_VARIANCES_AMT],
        FACT.[INVC_HDR_NBR],
        FACT.[INVC_ITM_NBR],
        FACT.[YR],
        FACT.[PER],
        FACT.[INVC_TYP],
        FACT.[SALES_ORDER_KEY],
        FACT.[COST_ELEMNT_KEY],
        FACT.[MATRL_KEY],
        FACT.[PLNT_KEY],
        FACT.[SALES_AREA_KEY],
        FACT.[SEG_KEY],
        FACT.[CRNCY_KEY],
        FACT.[PARNT_CUST_KEY],
        FACT.[RCD_TYP_DESC],
        FACT.[SHIPTO_CUST_KEY],
        FACT.[CUST_CMPNY_KEY],
        FACT.[PROFT_CNTR_KEY],
        FACT.[PRTNR_PROFT_CNTR_KEY],
        FACT.[DOCMT_NUM],
        FACT.[DOCMT_ITM_NUM],
        FACT.[BILLING_DATE_KEY],
        FACT.[PSTNG_DATE_KEY],
        FACT.[CMPNY_KEY],
        FACT.[SOLDTO_CUST_KEY],

	--Below columns are added for the Alias tables

        COSTELM.COST_ELEMNT_KEY AS COSTEHZ_COST_ELEMNT_KEY,
        COSTELM.COST_ELEMNT_KEY AS COSTEVZ_COST_ELEMNT_KEY,
        PNTR_PRF_CNTR.PROFT_CNTR_KEY AS Partner_Profit_Center_Horizontal_PROFT_CNTR_KEY,
        PNTR_PRF_CNTR.PROFT_CNTR_KEY AS Partner_Profit_Center_Vertical_PROFT_CNTR_KEY,
        PROF_CNTR.PROFT_CNTR_KEY AS PROFCHZ_PROFT_CNTR_KEY,
        PROF_CNTR.PROFT_CNTR_KEY AS PROFCVZ_PROFT_CNTR_KEY	
			
	
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_F_CO_COPA3_CUR_D] AS FACT
	
	LEFT JOIN [GLOBAL_EDW].[Propelis].[BILLING_DATE] AS BILL_DT
	    ON FACT.BILLING_DATE_KEY = BILL_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[POSTING_DATE] AS PST_DT
	    ON FACT.PSTNG_DATE_KEY = PST_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[SOLD_TO_CUST] AS SD_TO_CT
	    ON FACT.SOLDTO_CUST_KEY = SD_TO_CT.CUST_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[PARENT_CUST] AS PRNT_CT
	    ON FACT.PARNT_CUST_KEY = PRNT_CT.CUST_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[SHIP_TO_CUST] AS SP_TO_CT
	    ON FACT.SHIPTO_CUST_KEY = SP_TO_CT.CUST_KEY

    LEFT JOIN [GLOBAL_EDW].[Propelis].[PARTNER_PROFIT_CENTER] AS PNTR_PRF_CNTR
	    ON FACT.PRTNR_PROFT_CNTR_KEY = PNTR_PRF_CNTR.PROFT_CNTR_KEY

    LEFT JOIN [GLOBAL_EDW].[Propelis].[PROFIT_CENTER] AS PROF_CNTR
	    ON FACT.PROFT_CNTR_KEY = PROF_CNTR.PROFT_CNTR_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[BILL_TO_CUST] AS BIL_TO_CT
	    ON FACT.BILLTO_CUST_KEY = BIL_TO_CT.CUST_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTOMR_CUR_D] AS CUSTOMR
	    ON FACT.CUST_KEY = CUSTOMR.CUST_KEY
	   
	LEFT JOIN [GLOBAL_EDW].[Propelis].[BRAND_OWNER_CUST] AS BRD_OWN_CT
	    ON FACT.BRAND_OWNER_CUST_KEY = BRD_OWN_CT.CUST_KEY
	   
	LEFT JOIN [GLOBAL_EDW].[Propelis].[GOODS_ISSUED_DATE] AS GD_ISSU_DT
	    ON FACT.GOODS_ISSUE_DATE_KEY = GD_ISSU_DT.DATE_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[SALES_REP] AS SA_REP
	    ON FACT.SALES_REP_REPRESENTATIVE_KEY = SA_REP.PERSONEL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[Propelis].[CUSTOMER_SERVICE_REP] AS CUST_SERV_REP
	    ON FACT.CUST_SERVC_REPRESENTATIVE_KEY = CUST_SERV_REP.PERSONEL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CMPNYCD_CUR_D] AS CMPNYCD
	    ON FACT.CMPNY_KEY = CMPNYCD.CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTCMP_CUR_D] AS CUSTCMP
	    ON FACT.CUST_CMPNY_KEY = CUSTCMP.CUST_CMPNY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_COSTELM_CUR_D] AS COSTELM
	    ON FACT.COST_ELEMNT_KEY = COSTELM.COST_ELEMNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATERIAL_CUR_D] AS MATERIAL
	    ON FACT.MATRL_KEY = MATERIAL.MATRL_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_PLANT_CUR_D] AS PLANT
	    ON FACT.PLNT_KEY = PLANT.PLNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALAREA_CUR_D] AS SALAREA
	    ON FACT.SALES_AREA_KEY = SALAREA.SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMENT_CUR_D] AS SEGMENT
	    ON FACT.SEG_KEY = SEGMENT.SEG_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CRNCY_CUR_D] AS CRNCY
	    ON FACT.CRNCY_KEY = CRNCY.CRNCY_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_SAL_SALORDH_CUR_D] AS SALORDH
	    ON FACT.SALES_ORDER_KEY = SALORDH.SALES_ORDER_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_PRS_WBSELEM_CUR_D] AS WBSELEM
	    ON FACT.WBS_ELEMNT_KEY = WBSELEM.WBS_ELEMNT_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_CUSTSAL_CUR_D] AS CUSTSAL
	    ON FACT.CUST_SALES_AREA_KEY = CUSTSAL.CUST_SALES_AREA_KEY
		
	LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_MST_MATSALS_CUR_D] AS MATSALS
	    ON FACT.MATRL_SALES_KEY = MATSALS.MATRL_SALES_KEY;
END
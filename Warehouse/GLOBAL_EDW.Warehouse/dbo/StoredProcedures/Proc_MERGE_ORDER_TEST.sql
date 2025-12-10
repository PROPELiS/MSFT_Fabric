CREATE   PROCEDURE [dbo].[Proc_MERGE_ORDER_TEST] (@LoadType VARCHAR(10))   -- 'FULL' or 'DELTA'
AS
BEGIN

    --------------------------------------------------------------
    -- FULL LOAD: TRUNCATE + INSERT
    --------------------------------------------------------------
    IF UPPER(@LoadType) = 'FULL'
    BEGIN
        
        TRUNCATE TABLE [GLOBAL_EDW].[dbo].[MERGE_ORDER_TEST];

        INSERT INTO [GLOBAL_EDW].[dbo].[MERGE_ORDER_TEST]
        (
            [ORDER_KEY],
            [Order Type],
            [Order Category],
            [Order Company Code],
            [Production Plant ID],
            [Planning Plant ID],
            [Material ID],
            [Order Profit Center ID],
            [Sales Order ID],
            [Sales Order Item Number],
            [Task List Type],
            [Order Create Date],
            [Order Close Date],
            [Project ID],
            [MRP Controller Code],
            [Reservation Number],
            [Scheduled Start Date],
            [Scheduled Finish Date],
            [Actual Start Date],
            [Actual Finish Date],
            [Planned Release Date],
            [Actual Release Date],
            [ETL_SRC_SYS_CD],
            [ETL_EFFECTV_BEGIN_DATE],
            [ETL_EFFECTV_END_DATE],
            [ETL_CURR_RCD_IND],
            [ETL_CREATED_TS],
            [ETL_UPDTD_TS],
            [Order Number],
            [Entered By],
            [Order Description],
            [WBS Element Internal],
            [Controlling Area],
            [Responsible Cost Center],
            [Order Currency Code],
            [Release Date],
            [Technical Completion Date],
            [Object Number],
            [Requested By],
            [Reason Code],
            [Error Severity],
            [Error Caught By],
            [Error Origin],
            [Error Type],
            [Error Description],
            [Reason For Change],
            [Description Of Change],
            [Main Work Center ID],
            [Costing Sheet],
            [Order Changed By],
            [Order Changed On],
            [Business Area],
            [Functional Area],
            [Object Class],
            [Tax Jurisdiction],
            [Overhead Key],
            [Basic End Date],
            [Basic Start Date],
            [Scheduled Release Date],
            [Plan Number Of Operation],
            [Exact Break Times],
            [Network Profile],
            [Order Priority],
            [Costing Variant Plan],
            [Costing Variant Actual],
            [Superior Network Number],
            [Superior Activity],
            [Responsible Planner Group],
            [Change Number],
            [Order Type Description],
            [Order Category Description],
            [Task List Type Description],
            [MRP Controller Code Description],
            [Controlling Area Description],
            [Business Area Description],
            [Functional Area Description],
            [Object Class Description],
            [External Order Number],
            [Person Responsible],
            [End Of Work Date],
            [Date Of Last Status Change],
            [Status Reached So Far],
            [Is Created],
            [Is Released],
            [Is Completed],
            [Is Closed],
            [Deletion Indicator]
        )
        SELECT 
            S.[ORDER_KEY],
            S.[Order Type],
            S.[Order Category],
            S.[Order Company Code],
            S.[Production Plant ID],
            S.[Planning Plant ID],
            S.[Material ID],
            S.[Order Profit Center ID],
            S.[Sales Order ID],
            S.[Sales Order Item Number],
            S.[Task List Type],
            S.[Order Create Date],
            S.[Order Close Date],
            S.[Project ID],
            S.[MRP Controller Code],
            S.[Reservation Number],
            S.[Scheduled Start Date],
            S.[Scheduled Finish Date],
            S.[Actual Start Date],
            S.[Actual Finish Date],
            S.[Planned Release Date],
            S.[Actual Release Date],
            S.[ETL_SRC_SYS_CD],
            S.[ETL_EFFECTV_BEGIN_DATE],
            S.[ETL_EFFECTV_END_DATE],
            S.[ETL_CURR_RCD_IND],
            S.[ETL_CREATED_TS],
            S.[ETL_UPDTD_TS],
            S.[Order Number],
            S.[Entered By],
            S.[Order Description],
            S.[WBS Element Internal],
            S.[Controlling Area],
            S.[Responsible Cost Center],
            S.[Order Currency Code],
            S.[Release Date],
            S.[Technical Completion Date],
            S.[Object Number],
            S.[Requested By],
            S.[Reason Code],
            S.[Error Severity],
            S.[Error Caught By],
            S.[Error Origin],
            S.[Error Type],
            S.[Error Description],
            S.[Reason For Change],
            S.[Description Of Change],
            S.[Main Work Center ID],
            S.[Costing Sheet],
            S.[Order Changed By],
            S.[Order Changed On],
            S.[Business Area],
            S.[Functional Area],
            S.[Object Class],
            S.[Tax Jurisdiction],
            S.[Overhead Key],
            S.[Basic End Date],
            S.[Basic Start Date],
            S.[Scheduled Release Date],
            S.[Plan Number Of Operation],
            S.[Exact Break Times],
            S.[Network Profile],
            S.[Order Priority],
            S.[Costing Variant Plan],
            S.[Costing Variant Actual],
            S.[Superior Network Number],
            S.[Superior Activity],
            S.[Responsible Planner Group],
            S.[Change Number],
            S.[Order Type Description],
            S.[Order Category Description],
            S.[Task List Type Description],
            S.[MRP Controller Code Description],
            S.[Controlling Area Description],
            S.[Business Area Description],
            S.[Functional Area Description],
            S.[Object Class Description],
            S.[External Order Number],
            S.[Person Responsible],
            S.[End Of Work Date],
            S.[Date Of Last Status Change],
            S.[Status Reached So Far],
            S.[Is Created],
            S.[Is Released],
            S.[Is Completed],
            S.[Is Closed],
            S.[Deletion Indicator]
        FROM [GLOBAL_EDW].[dbo].[EDW_T_D_PM_ORDER_CUR_D-TEST] AS S;

    END

    --------------------------------------------------------------
    -- DELTA MERGE
    --------------------------------------------------------------
    ELSE
    BEGIN

        MERGE INTO [GLOBAL_EDW].[dbo].[MERGE_ORDER_TEST] AS T
        USING 
        (
            SELECT 
			    S.[ORDER_KEY],
            S.[Order Type],
            S.[Order Category],
            S.[Order Company Code],
            S.[Production Plant ID],
            S.[Planning Plant ID],
            S.[Material ID],
            S.[Order Profit Center ID],
            S.[Sales Order ID],
            S.[Sales Order Item Number],
            S.[Task List Type],
            S.[Order Create Date],
            S.[Order Close Date],
            S.[Project ID],
            S.[MRP Controller Code],
            S.[Reservation Number],
            S.[Scheduled Start Date],
            S.[Scheduled Finish Date],
            S.[Actual Start Date],
            S.[Actual Finish Date],
            S.[Planned Release Date],
            S.[Actual Release Date],
            S.[ETL_SRC_SYS_CD],
            S.[ETL_EFFECTV_BEGIN_DATE],
            S.[ETL_EFFECTV_END_DATE],
            S.[ETL_CURR_RCD_IND],
            S.[ETL_CREATED_TS],
            S.[ETL_UPDTD_TS],
            S.[Order Number],
            S.[Entered By],
            S.[Order Description],
            S.[WBS Element Internal],
            S.[Controlling Area],
            S.[Responsible Cost Center],
            S.[Order Currency Code],
            S.[Release Date],
            S.[Technical Completion Date],
            S.[Object Number],
            S.[Requested By],
            S.[Reason Code],
            S.[Error Severity],
            S.[Error Caught By],
            S.[Error Origin],
            S.[Error Type],
            S.[Error Description],
            S.[Reason For Change],
            S.[Description Of Change],
            S.[Main Work Center ID],
            S.[Costing Sheet],
            S.[Order Changed By],
            S.[Order Changed On],
            S.[Business Area],
            S.[Functional Area],
            S.[Object Class],
            S.[Tax Jurisdiction],
            S.[Overhead Key],
            S.[Basic End Date],
            S.[Basic Start Date],
            S.[Scheduled Release Date],
            S.[Plan Number Of Operation],
            S.[Exact Break Times],
            S.[Network Profile],
            S.[Order Priority],
            S.[Costing Variant Plan],
            S.[Costing Variant Actual],
            S.[Superior Network Number],
            S.[Superior Activity],
            S.[Responsible Planner Group],
            S.[Change Number],
            S.[Order Type Description],
            S.[Order Category Description],
            S.[Task List Type Description],
            S.[MRP Controller Code Description],
            S.[Controlling Area Description],
            S.[Business Area Description],
            S.[Functional Area Description],
            S.[Object Class Description],
            S.[External Order Number],
            S.[Person Responsible],
            S.[End Of Work Date],
            S.[Date Of Last Status Change],
            S.[Status Reached So Far],
            S.[Is Created],
            S.[Is Released],
            S.[Is Completed],
            S.[Is Closed],
            S.[Deletion Indicator] 
            FROM [GLOBAL_EDW].[dbo].[EDW_T_D_PM_ORDER_CUR_D-TEST] AS S
        ) AS S
        ON T.[ORDER_KEY] = S.[ORDER_KEY]

        WHEN MATCHED AND
        (
            -- Compare ALL columns
            (
                T.[ORDER_KEY] <> ISNULL(S.[ORDER_KEY], '')
                OR T.[Order Type] <> ISNULL(S.[Order Type], '')
                OR T.[Order Category] <> ISNULL(S.[Order Category], '')
                OR T.[Order Company Code] <> ISNULL(S.[Order Company Code], '')
                OR T.[Production Plant ID] <> ISNULL(S.[Production Plant ID], '')
                OR T.[Planning Plant ID] <> ISNULL(S.[Planning Plant ID], '')
                OR T.[Material ID] <> ISNULL(S.[Material ID], '')
                OR T.[Order Profit Center ID] <> ISNULL(S.[Order Profit Center ID], '')
                OR T.[Sales Order ID] <> ISNULL(S.[Sales Order ID], '')
                OR T.[Sales Order Item Number] <> ISNULL(S.[Sales Order Item Number], '')
                OR T.[Task List Type] <> ISNULL(S.[Task List Type], '')
                OR T.[Order Create Date] <> ISNULL(S.[Order Create Date], '')
                OR T.[Order Close Date] <> ISNULL(S.[Order Close Date], '')
                OR T.[Project ID] <> ISNULL(S.[Project ID], '')
                OR T.[MRP Controller Code] <> ISNULL(S.[MRP Controller Code], '')
                OR T.[Reservation Number] <> ISNULL(S.[Reservation Number], '')
                OR T.[Scheduled Start Date] <> ISNULL(S.[Scheduled Start Date], '')
                OR T.[Scheduled Finish Date] <> ISNULL(S.[Scheduled Finish Date], '')
                OR T.[Actual Start Date] <> ISNULL(S.[Actual Start Date], '')
                OR T.[Actual Finish Date] <> ISNULL(S.[Actual Finish Date], '')
                OR T.[Planned Release Date] <> ISNULL(S.[Planned Release Date], '')
                OR T.[Actual Release Date] <> ISNULL(S.[Actual Release Date], '')
                OR T.[ETL_SRC_SYS_CD] <> ISNULL(S.[ETL_SRC_SYS_CD], '')
                OR T.[ETL_EFFECTV_BEGIN_DATE] <> ISNULL(S.[ETL_EFFECTV_BEGIN_DATE], '')
                OR T.[ETL_EFFECTV_END_DATE] <> ISNULL(S.[ETL_EFFECTV_END_DATE], '')
                OR T.[ETL_CURR_RCD_IND] <> ISNULL(S.[ETL_CURR_RCD_IND], '')
                OR T.[ETL_CREATED_TS] <> ISNULL(S.[ETL_CREATED_TS], '')
                OR T.[ETL_UPDTD_TS] <> ISNULL(S.[ETL_UPDTD_TS], '')
                OR T.[Order Number] <> ISNULL(S.[Order Number], '')
                OR T.[Entered By] <> ISNULL(S.[Entered By], '')
                OR T.[Order Description] <> ISNULL(S.[Order Description], '')
                OR T.[WBS Element Internal] <> ISNULL(S.[WBS Element Internal], '')
                OR T.[Controlling Area] <> ISNULL(S.[Controlling Area], '')
                OR T.[Responsible Cost Center] <> ISNULL(S.[Responsible Cost Center], '')
                OR T.[Order Currency Code] <> ISNULL(S.[Order Currency Code], '')
                OR T.[Release Date] <> ISNULL(S.[Release Date], '')
                OR T.[Technical Completion Date] <> ISNULL(S.[Technical Completion Date], '')
                OR T.[Object Number] <> ISNULL(S.[Object Number], '')
                OR T.[Requested By] <> ISNULL(S.[Requested By], '')
                OR T.[Reason Code] <> ISNULL(S.[Reason Code], '')
                OR T.[Error Severity] <> ISNULL(S.[Error Severity], '')
                OR T.[Error Caught By] <> ISNULL(S.[Error Caught By], '')
                OR T.[Error Origin] <> ISNULL(S.[Error Origin], '')
                OR T.[Error Type] <> ISNULL(S.[Error Type], '')
                OR T.[Error Description] <> ISNULL(S.[Error Description], '')
                OR T.[Reason For Change] <> ISNULL(S.[Reason For Change], '')
                OR T.[Description Of Change] <> ISNULL(S.[Description Of Change], '')
                OR T.[Main Work Center ID] <> ISNULL(S.[Main Work Center ID], '')
                OR T.[Costing Sheet] <> ISNULL(S.[Costing Sheet], '')
                OR T.[Order Changed By] <> ISNULL(S.[Order Changed By], '')
                OR T.[Order Changed On] <> ISNULL(S.[Order Changed On], '')
                OR T.[Business Area] <> ISNULL(S.[Business Area], '')
                OR T.[Functional Area] <> ISNULL(S.[Functional Area], '')
                OR T.[Object Class] <> ISNULL(S.[Object Class], '')
                OR T.[Tax Jurisdiction] <> ISNULL(S.[Tax Jurisdiction], '')
                OR T.[Overhead Key] <> ISNULL(S.[Overhead Key], '')
                OR T.[Basic End Date] <> ISNULL(S.[Basic End Date], '')
                OR T.[Basic Start Date] <> ISNULL(S.[Basic Start Date], '')
                OR T.[Scheduled Release Date] <> ISNULL(S.[Scheduled Release Date], '')
                OR T.[Plan Number Of Operation] <> ISNULL(S.[Plan Number Of Operation], '')
                OR T.[Exact Break Times] <> ISNULL(S.[Exact Break Times], '')
                OR T.[Network Profile] <> ISNULL(S.[Network Profile], '')
                OR T.[Order Priority] <> ISNULL(S.[Order Priority], '')
                OR T.[Costing Variant Plan] <> ISNULL(S.[Costing Variant Plan], '')
                OR T.[Costing Variant Actual] <> ISNULL(S.[Costing Variant Actual], '')
                OR T.[Superior Network Number] <> ISNULL(S.[Superior Network Number], '')
                OR T.[Superior Activity] <> ISNULL(S.[Superior Activity], '')
                OR T.[Responsible Planner Group] <> ISNULL(S.[Responsible Planner Group], '')
                OR T.[Change Number] <> ISNULL(S.[Change Number], '')
                OR T.[Order Type Description] <> ISNULL(S.[Order Type Description], '')
                OR T.[Order Category Description] <> ISNULL(S.[Order Category Description], '')
                OR T.[Task List Type Description] <> ISNULL(S.[Task List Type Description], '')
                OR T.[MRP Controller Code Description] <> ISNULL(S.[MRP Controller Code Description], '')
                OR T.[Controlling Area Description] <> ISNULL(S.[Controlling Area Description], '')
                OR T.[Business Area Description] <> ISNULL(S.[Business Area Description], '')
                OR T.[Functional Area Description] <> ISNULL(S.[Functional Area Description], '')
                OR T.[Object Class Description] <> ISNULL(S.[Object Class Description], '')
                OR T.[External Order Number] <> ISNULL(S.[External Order Number], '')
                OR T.[Person Responsible] <> ISNULL(S.[Person Responsible], '')
                OR T.[End Of Work Date] <> ISNULL(S.[End Of Work Date], '')
                OR T.[Date Of Last Status Change] <> ISNULL(S.[Date Of Last Status Change], '')
                OR T.[Status Reached So Far] <> ISNULL(S.[Status Reached So Far], '')
                OR T.[Is Created] <> ISNULL(S.[Is Created], '')
                OR T.[Is Released] <> ISNULL(S.[Is Released], '')
                OR T.[Is Completed] <> ISNULL(S.[Is Completed], '')
                OR T.[Is Closed] <> ISNULL(S.[Is Closed], '')
                OR T.[Deletion Indicator] <> ISNULL(S.[Deletion Indicator], '')
            )
        )
        THEN UPDATE SET
            -- updated all columns
            T.[ORDER_KEY] = S.[ORDER_KEY],
            T.[Order Type] = S.[Order Type],
            T.[Order Category] = S.[Order Category],
            T.[Order Company Code] = S.[Order Company Code],
            T.[Production Plant ID] = S.[Production Plant ID],
            T.[Planning Plant ID] = S.[Planning Plant ID],
            T.[Material ID] = S.[Material ID],
            T.[Order Profit Center ID] = S.[Order Profit Center ID],
            T.[Sales Order ID] = S.[Sales Order ID],
            T.[Sales Order Item Number] = S.[Sales Order Item Number],
            T.[Task List Type] = S.[Task List Type],
            T.[Order Create Date] = S.[Order Create Date],
            T.[Order Close Date] = S.[Order Close Date],
            T.[Project ID] = S.[Project ID],
            T.[MRP Controller Code] = S.[MRP Controller Code],
            T.[Reservation Number] = S.[Reservation Number],
            T.[Scheduled Start Date] = S.[Scheduled Start Date],
            T.[Scheduled Finish Date] = S.[Scheduled Finish Date],
            T.[Actual Start Date] = S.[Actual Start Date],
            T.[Actual Finish Date] = S.[Actual Finish Date],
            T.[Planned Release Date] = S.[Planned Release Date],
            T.[Actual Release Date] = S.[Actual Release Date],
            T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
            T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
            T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
            T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
            T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
            T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
            T.[Order Number] = S.[Order Number],
            T.[Entered By] = S.[Entered By],
            T.[Order Description] = S.[Order Description],
            T.[WBS Element Internal] = S.[WBS Element Internal],
            T.[Controlling Area] = S.[Controlling Area],
            T.[Responsible Cost Center] = S.[Responsible Cost Center],
            T.[Order Currency Code] = S.[Order Currency Code],
            T.[Release Date] = S.[Release Date],
            T.[Technical Completion Date] = S.[Technical Completion Date],
            T.[Object Number] = S.[Object Number],
            T.[Requested By] = S.[Requested By],
            T.[Reason Code] = S.[Reason Code],
            T.[Error Severity] = S.[Error Severity],
            T.[Error Caught By] = S.[Error Caught By],
            T.[Error Origin] = S.[Error Origin],
            T.[Error Type] = S.[Error Type],
            T.[Error Description] = S.[Error Description],
            T.[Reason For Change] = S.[Reason For Change],
            T.[Description Of Change] = S.[Description Of Change],
            T.[Main Work Center ID] = S.[Main Work Center ID],
            T.[Costing Sheet] = S.[Costing Sheet],
            T.[Order Changed By] = S.[Order Changed By],
            T.[Order Changed On] = S.[Order Changed On],
            T.[Business Area] = S.[Business Area],
            T.[Functional Area] = S.[Functional Area],
            T.[Object Class] = S.[Object Class],
            T.[Tax Jurisdiction] = S.[Tax Jurisdiction],
            T.[Overhead Key] = S.[Overhead Key],
            T.[Basic End Date] = S.[Basic End Date],
            T.[Basic Start Date] = S.[Basic Start Date],
            T.[Scheduled Release Date] = S.[Scheduled Release Date],
            T.[Plan Number Of Operation] = S.[Plan Number Of Operation],
            T.[Exact Break Times] = S.[Exact Break Times],
            T.[Network Profile] = S.[Network Profile],
            T.[Order Priority] = S.[Order Priority],
            T.[Costing Variant Plan] = S.[Costing Variant Plan],
            T.[Costing Variant Actual] = S.[Costing Variant Actual],
            T.[Superior Network Number] = S.[Superior Network Number],
            T.[Superior Activity] = S.[Superior Activity],
            T.[Responsible Planner Group] = S.[Responsible Planner Group],
            T.[Change Number] = S.[Change Number],
            T.[Order Type Description] = S.[Order Type Description],
            T.[Order Category Description] = S.[Order Category Description],
            T.[Task List Type Description] = S.[Task List Type Description],
            T.[MRP Controller Code Description] = S.[MRP Controller Code Description],
            T.[Controlling Area Description] = S.[Controlling Area Description],
            T.[Business Area Description] = S.[Business Area Description],
            T.[Functional Area Description] = S.[Functional Area Description],
            T.[Object Class Description] = S.[Object Class Description],
            T.[External Order Number] = S.[External Order Number],
            T.[Person Responsible] = S.[Person Responsible],
            T.[End Of Work Date] = S.[End Of Work Date],
            T.[Date Of Last Status Change] = S.[Date Of Last Status Change],
            T.[Status Reached So Far] = S.[Status Reached So Far],
            T.[Is Created] = S.[Is Created],
            T.[Is Released] = S.[Is Released],
            T.[Is Completed] = S.[Is Completed],
            T.[Is Closed] = S.[Is Closed],
            T.[Deletion Indicator] = S.[Deletion Indicator]

        WHEN NOT MATCHED BY TARGET
        THEN INSERT
        (
            [ORDER_KEY],
            [Order Type],
            [Order Category],
            [Order Company Code],
            [Production Plant ID],
            [Planning Plant ID],
            [Material ID],
            [Order Profit Center ID],
            [Sales Order ID],
            [Sales Order Item Number],
            [Task List Type],
            [Order Create Date],
            [Order Close Date],
            [Project ID],
            [MRP Controller Code],
            [Reservation Number],
            [Scheduled Start Date],
            [Scheduled Finish Date],
            [Actual Start Date],
            [Actual Finish Date],
            [Planned Release Date],
            [Actual Release Date],
            [ETL_SRC_SYS_CD],
            [ETL_EFFECTV_BEGIN_DATE],
            [ETL_EFFECTV_END_DATE],
            [ETL_CURR_RCD_IND],
            [ETL_CREATED_TS],
            [ETL_UPDTD_TS],
            [Order Number],
            [Entered By],
            [Order Description],
            [WBS Element Internal],
            [Controlling Area],
            [Responsible Cost Center],
            [Order Currency Code],
            [Release Date],
            [Technical Completion Date],
            [Object Number],
            [Requested By],
            [Reason Code],
            [Error Severity],
            [Error Caught By],
            [Error Origin],
            [Error Type],
            [Error Description],
            [Reason For Change],
            [Description Of Change],
            [Main Work Center ID],
            [Costing Sheet],
            [Order Changed By],
            [Order Changed On],
            [Business Area],
            [Functional Area],
            [Object Class],
            [Tax Jurisdiction],
            [Overhead Key],
            [Basic End Date],
            [Basic Start Date],
            [Scheduled Release Date],
            [Plan Number Of Operation],
            [Exact Break Times],
            [Network Profile],
            [Order Priority],
            [Costing Variant Plan],
            [Costing Variant Actual],
            [Superior Network Number],
            [Superior Activity],
            [Responsible Planner Group],
            [Change Number],
            [Order Type Description],
            [Order Category Description],
            [Task List Type Description],
            [MRP Controller Code Description],
            [Controlling Area Description],
            [Business Area Description],
            [Functional Area Description],
            [Object Class Description],
            [External Order Number],
            [Person Responsible],
            [End Of Work Date],
            [Date Of Last Status Change],
            [Status Reached So Far],
            [Is Created],
            [Is Released],
            [Is Completed],
            [Is Closed],
            [Deletion Indicator]
        )
        VALUES
        (
            S.[ORDER_KEY],
            S.[Order Type],
            S.[Order Category],
            S.[Order Company Code],
            S.[Production Plant ID],
            S.[Planning Plant ID],
            S.[Material ID],
            S.[Order Profit Center ID],
            S.[Sales Order ID],
            S.[Sales Order Item Number],
            S.[Task List Type],
            S.[Order Create Date],
            S.[Order Close Date],
            S.[Project ID],
            S.[MRP Controller Code],
            S.[Reservation Number],
            S.[Scheduled Start Date],
            S.[Scheduled Finish Date],
            S.[Actual Start Date],
            S.[Actual Finish Date],
            S.[Planned Release Date],
            S.[Actual Release Date],
            S.ETL_SRC_SYS_CD,
            S.ETL_EFFECTV_BEGIN_DATE,
            S.ETL_EFFECTV_END_DATE,
            S.ETL_CURR_RCD_IND,
            S.ETL_CREATED_TS,
            S.ETL_UPDTD_TS,
            S.[Order Number],
            S.[Entered By],
            S.[Order Description],
            S.[WBS Element Internal],
            S.[Controlling Area],
            S.[Responsible Cost Center],
            S.[Order Currency Code],
            S.[Release Date],
            S.[Technical Completion Date],
            S.[Object Number],
            S.[Requested By],
            S.[Reason Code],
            S.[Error Severity],
            S.[Error Caught By],
            S.[Error Origin],
            S.[Error Type],
            S.[Error Description],
            S.[Reason For Change],
            S.[Description Of Change],
            S.[Main Work Center ID],
            S.[Costing Sheet],
            S.[Order Changed By],
            S.[Order Changed On],
            S.[Business Area],
            S.[Functional Area],
            S.[Object Class],
            S.[Tax Jurisdiction],
            S.[Overhead Key],
            S.[Basic End Date],
            S.[Basic Start Date],
            S.[Scheduled Release Date],
            S.[Plan Number Of Operation],
            S.[Exact Break Times],
            S.[Network Profile],
            S.[Order Priority],
            S.[Costing Variant Plan],
            S.[Costing Variant Actual],
            S.[Superior Network Number],
            S.[Superior Activity],
            S.[Responsible Planner Group],
            S.[Change Number],
            S.[Order Type Description],
            S.[Order Category Description],
            S.[Task List Type Description],
            S.[MRP Controller Code Description],
            S.[Controlling Area Description],
            S.[Business Area Description],
            S.[Functional Area Description],
            S.[Object Class Description],
            S.[External Order Number],
            S.[Person Responsible],
            S.[End Of Work Date],
            S.[Date Of Last Status Change],
            S.[Status Reached So Far],
            S.[Is Created],
            S.[Is Released],
            S.[Is Completed],
            S.[Is Closed],
            S.[Deletion Indicator]
        );

    END;

END;
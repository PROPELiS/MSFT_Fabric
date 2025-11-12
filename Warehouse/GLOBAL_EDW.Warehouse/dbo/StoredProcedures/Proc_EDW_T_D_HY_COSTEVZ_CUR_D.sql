CREATE       PROCEDURE [dbo].[Proc_EDW_T_D_HY_COSTEVZ_CUR_D]
AS
BEGIN
    UPDATE T
    SET
        T.[COST_ELEMNT_KEY]  	= S.[COST_ELEMNT_KEY],
        T.[Cost Element Controlling Area ID VH]  	= S.[CONTROLLING_AREA_ID],
        T.[Cost Element Parent ID VH]  	= S.[PARNT_ID],
        T.[Cost Element Parent Description VH]  	= S.[PARNT_DESC],
        T.[Cost Element Child ID VH]  	= S.[CHILD_ID],
        T.[Cost Element Child Description VH]  	= S.[CHILD_DESC],
        T.[Cost Element Hierarchical Level VH]  	= S.[LVL],
        T.[Chart of Accounts]  = S.[CHART_OF_ACCTS]


 FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEVZ_CUR_D] AS T
    INNER JOIN [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTEVZ_CUR_D] AS S
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY];

    -- Step 2: Insert new records from mirror into target table
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEVZ_CUR_D] (
        [COST_ELEMNT_KEY],
        [Cost Element Controlling Area ID VH],
        [Cost Element Parent ID VH],
        [Cost Element Parent Description VH],
        [Cost Element Child ID VH],
        [Cost Element Child Description VH],
        [Cost Element Hierarchical Level VH],
        [Chart of Accounts]
)
    SELECT
       S.[COST_ELEMNT_KEY],
       S.[CONTROLLING_AREA_ID],
       S.[PARNT_ID],
       S.[PARNT_DESC],
       S.[CHILD_ID],
       S.[CHILD_DESC],
       S.[LVL],
       S.[CHART_OF_ACCTS]

    
	FROM [GLOBAL_EDW_QA].[GLOBAL_EDW].[EDW_T_D_HY_COSTEVZ_CUR_D] AS S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTEVZ_CUR_D] AS T
        ON T.[COST_ELEMNT_KEY] = S.[COST_ELEMNT_KEY]
    WHERE T.[COST_ELEMNT_KEY] IS NULL;
END
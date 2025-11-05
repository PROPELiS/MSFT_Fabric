CREATE     PROCEDURE [dbo].[Proc_EDW_T_D_FI_ICGLGRP_CUR_D]
AS
BEGIN

    ------------------------------------------------------------------
    -- Step 1: Load source data into a temp table with a hash key
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#SourceData') IS NOT NULL
        DROP TABLE #SourceData;

    SELECT 
        S.[GL_ID],
        S.[GL_DESC],
        S.[GL_ACCT_GROUP],
        S.[GL_ACCT_GROUP_DESC],
        S.[TYP],
        S.[REVAL_IND],
       
        HASHBYTES('SHA2_256', CONCAT(
            ISNULL(S.[GL_ID], ''), '|',
            ISNULL(S.[GL_DESC], ''), '|',
            ISNULL(S.[GL_ACCT_GROUP], ''), '|',
            ISNULL(S.[GL_ACCT_GROUP_DESC], ''), '|',
            ISNULL(S.[TYP], ''), '|',
            ISNULL(S.[REVAL_IND], '')
            
        )) AS HashKey
    INTO #SourceData
    FROM [GLOBAL_EDW_MIRROR].[dbo].[EDW_T_D_FI_ICGLGRP_CUR_D] AS S;

    ------------------------------------------------------------------
    -- Step 2: Update changed rows in target (hash mismatch)
    ------------------------------------------------------------------
    UPDATE T
    SET 
        [GL_ID] = S.[GL_ID],
        [General Ledger Description] = S.[GL_DESC],
        [General Ledger Account Group] = S.[GL_ACCT_GROUP],
        [General Ledger Account Group Description] = S.[GL_ACCT_GROUP_DESC],
        [General Ledger Type] = S.[TYP],
        [REVAL_IND] = S.[REVAL_IND]
        
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ICGLGRP_CUR_D] AS T
    INNER JOIN #SourceData AS S
        ON T.[GL_CD] = S.[GL_CD]
    WHERE HASHBYTES('SHA2_256', CONCAT(
            ISNULL(T.[GL_ID], ''), '|',
            ISNULL(T.[General Ledger Description], ''), '|',
            ISNULL(T.[General Ledger Account Group], ''), '|',
            ISNULL(T.[General Ledger Account Group Description], ''), '|',
            ISNULL(T.[General Ledger Type], ''), '|',
            ISNULL(T.[REVAL_IND], '')
        )) <> S.HashKey;
		
    ------------------------------------------------------------------
    -- Step 3: Insert only new rows not already in target
    ------------------------------------------------------------------
	
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ICGLGRP_CUR_D]
    (
        [GL_ID],
        [General Ledger Description],
        [General Ledger Account Group],
        [General Ledger Account Group Description],
        [General Ledger Type],
        [REVAL_IND]
       
    )
    SELECT 
        S.[GL_ID],
        S.[GL_DESC],
        S.[GL_ACCT_GROUP],
        S.[GL_ACCT_GROUP_DESC],
        S.[TYP],
        S.[REVAL_IND]
       
    FROM #SourceData AS S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ICGLGRP_CUR_D] AS T
        WHERE T.[GL_CD] = S.[GL_CD]
    );

    ------------------------------------------------------------------
    -- Step 4: Confirmation
    ------------------------------------------------------------------
    PRINT 'Incremental load completed – updated changed rows and inserted new rows.';

END;
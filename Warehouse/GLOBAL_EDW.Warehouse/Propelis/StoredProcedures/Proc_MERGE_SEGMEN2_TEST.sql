CREATE   PROCEDURE [Propelis].[Proc_MERGE_SEGMEN2_TEST](@LoadType VARCHAR(10))   -- 'FULL' or 'DELTA'
AS
BEGIN

    --------------------------------------------------------------
    -- FULL LOAD: TRUNCATE + INSERT
    --------------------------------------------------------------
    IF UPPER(@LoadType) = 'FULL'
    BEGIN
        
        TRUNCATE TABLE [GLOBAL_EDW].[dbo].[MERGE_SEGMEN2_TEST]; --TARGET TABLE NAME

        INSERT INTO [GLOBAL_EDW].[dbo].[MERGE_SEGMEN2_TEST] --TARGET TABLE NAME
        (
            [SEG_KEY],
			[ETL_UPDTD_TS],
			[ETL_CREATED_TS],
			[ETL_CURR_RCD_IND],
			[ETL_EFFECTV_END_DATE],
			[ETL_EFFECTV_BEGIN_DATE],    --TARGET TABLE COLUMN NAME
			[ETL_SRC_SYS_CD],
			[Segment],
			[Segment Name]
        )
        SELECT
            S.[SEG_KEY],
            S.[ETL_UPDTD_TS],
            S.[ETL_CREATED_TS],
			S.[ETL_CURR_RCD_IND],
			S.[ETL_EFFECTV_END_DATE],
			S.[ETL_EFFECTV_BEGIN_DATE],     --SOURCE TABLE COLUM NAME
			S.[ETL_SRC_SYS_CD],
			S.[Segment],
			S.[Segment Name]
        FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D-TEST] AS S; -- SOURCE TABLE NAME

    END

    --------------------------------------------------------------
    -- DELTA LOAD: MERGE WITHOUT TEMP TABLE
    --------------------------------------------------------------
    ELSE   -- Means DELTA Load
    BEGIN

        MERGE INTO [GLOBAL_EDW].[dbo].[MERGE_SEGMEN2_TEST] AS T --TARGET TABLE NAME
        USING (
            SELECT 
                S.[SEG_KEY],
                S.[ETL_UPDTD_TS],
                S.[ETL_CREATED_TS],
			    S.[ETL_CURR_RCD_IND],
			    S.[ETL_EFFECTV_END_DATE],
			    S.[ETL_EFFECTV_BEGIN_DATE],     --SOURCE TABLE COLUM NAME
			    S.[ETL_SRC_SYS_CD],
			    S.[Segment],
			    S.[Segment Name]

            FROM [GLOBAL_EDW].[dbo].[EDW_T_D_MST_SEGMEN2_CUR_D-TEST] AS S --SOURCE TABLE NAME
        ) AS S
            ON T.[SEG_KEY] = S.[SEG_KEY]

        WHEN MATCHED AND 
        (
               T.[SEG_KEY]                   <> ISNULL(S.[SEG_KEY],'')
            OR T.[ETL_UPDTD_TS]              <> ISNULL(S.[ETL_UPDTD_TS],'')
            OR T.[ETL_CREATED_TS]            <> ISNULL(S.[ETL_CREATED_TS],'')
			OR T.[ETL_CURR_RCD_IND]          <> ISNULL(S.[ETL_CURR_RCD_IND],'')
			OR T.[ETL_EFFECTV_END_DATE]      <> ISNULL(S.[ETL_EFFECTV_END_DATE],'')
			OR T.[ETL_EFFECTV_BEGIN_DATE]    <> ISNULL(S.[ETL_EFFECTV_BEGIN_DATE],'')
		    OR T.[ETL_SRC_SYS_CD]            <> ISNULL(S.[ETL_SRC_SYS_CD],'')
		    OR T.[Segment]                   <> ISNULL(S.[Segment],'')
			OR T.[Segment Name]              <> ISNULL(S.[Segment Name],'')
        )
        THEN UPDATE SET
               T.[SEG_KEY]                    = S.[SEG_KEY],
               T.[ETL_UPDTD_TS]               = S.[ETL_UPDTD_TS],
               T.[ETL_CREATED_TS]             = S.[ETL_CREATED_TS],
			   T.[ETL_CURR_RCD_IND]           = S.[ETL_CURR_RCD_IND],
			   T.[ETL_EFFECTV_END_DATE]       = S.[ETL_EFFECTV_END_DATE],
			   T.[ETL_EFFECTV_BEGIN_DATE]     = S.[ETL_EFFECTV_BEGIN_DATE],
			   T.[ETL_SRC_SYS_CD]             = S.[ETL_SRC_SYS_CD],
			   T.[Segment]                    = S.[Segment],
			   T.[Segment Name]               = S.[Segment Name]

        WHEN NOT MATCHED BY TARGET
        THEN INSERT
        (
            [SEG_KEY],
            [ETL_UPDTD_TS],
            [ETL_CREATED_TS],
            [ETL_CURR_RCD_IND],
            [ETL_EFFECTV_END_DATE],            --TARGET TABLE COLUMN NAME
            [ETL_EFFECTV_BEGIN_DATE],  
            [ETL_SRC_SYS_CD],
            [Segment],
            [Segment Name]	  
        )
        VALUES
        (
            S.[SEG_KEY],
			S.[ETL_UPDTD_TS],
			S.[ETL_CREATED_TS],
			S.[ETL_CURR_RCD_IND],
			S.[ETL_EFFECTV_END_DATE],
			S.[ETL_EFFECTV_BEGIN_DATE],         --SOURCE TABLE COLUM NAME
			S.[ETL_SRC_SYS_CD],
            S.[Segment],
			S.[Segment Name]	
        );

    END;

END;
CREATE   PROCEDURE [dbo].[Proc_EDW_T_D_FI_ASSETHR_CUR_D]
AS
BEGIN
    ----------------------------------------------------------------------
    -- Update existing records in target table
    ----------------------------------------------------------------------
    UPDATE T
    SET
        T.[Asset Company Code] = S.[CMPNY_CD],
        T.[Main Asset Number] = S.[MAIN_ASSET_NUM],
        T.[Asset Subnumber] = S.[ASSET_SUBNUMBER],
        T.[Asset Class] = S.[ASSET_CLASS],
        T.[Asset Class Description] = S.[ASSET_CLASS_DESC],
        T.[Asset Description] = S.[ASSET_DESC],
        T.[Asset Quantity] = S.[QTY],
        T.[Asset Base UOM] = S.[BASE_UOM],
        T.[Asset Capitalization Date] = S.[ASSET_CAPITALIZATION_DATE],
        T.[Asset First Acquistion Fiscal Year] = S.[FIRST_ACQUISTN_FISCAL_YR],
        T.[Asset First Acquistion Fiscal Period] = S.[FIRST_ACQUISTN_FISCAL_PER],
        T.[Asset Deactivation Date] = S.[DEACTIVATION_DATE],
        T.[ETL_SRC_SYS_CD] = S.[ETL_SRC_SYS_CD],
        T.[ETL_EFFECTV_BEGIN_DATE] = S.[ETL_EFFECTV_BEGIN_DATE],
        T.[ETL_EFFECTV_END_DATE] = S.[ETL_EFFECTV_END_DATE],
        T.[ETL_CURR_RCD_IND] = S.[ETL_CURR_RCD_IND],
        T.[ETL_CREATED_TS] = S.[ETL_CREATED_TS],
        T.[ETL_UPDTD_TS] = S.[ETL_UPDTD_TS],
        T.[Name Of Person Who Created The Asset] = S.[NAME_OF_PERSON_WHO_CREATED_THE],
        T.[Date On Which Asset Record was Created] = S.[DATE_ON_WHICH_RCD_WAS_CREATED],
        T.[Asset Value Date Of The First Posting] = S.[ASSET_VAL_DATE_OF_THE_FIRST_PST],
        T.[Asset Inventory Number] = S.[INVTRY_NUM],
        T.[Additional Asset Description] = S.[ADDITIONAL_ASSET_DESC],
        T.[Asset Serial Number] = S.[SERIAL_NUM],
        T.[Asset Cost Center] = S.[COST_CNTR],
        T.[Asset Plant] = S.[PLNT]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ASSETHR_CUR_D] T
    INNER JOIN [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_FI_ASSETHR_CUR_D] S
        ON T.[ASSET_KEY] = S.[ASSET_KEY];

    ----------------------------------------------------------------------
    -- Insert new records into target table
    ----------------------------------------------------------------------
    INSERT INTO [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ASSETHR_CUR_D] (
        [ASSET_KEY],
        [Asset Company Code],
        [Main Asset Number],
        [Asset Subnumber],
        [Asset Class],
        [Asset Class Description],
        [Asset Description],
        [Asset Quantity],
        [Asset Base UOM],
        [Asset Capitalization Date],
        [Asset First Acquistion Fiscal Year],
        [Asset First Acquistion Fiscal Period],
        [Asset Deactivation Date],
        [ETL_SRC_SYS_CD],
        [ETL_EFFECTV_BEGIN_DATE],
        [ETL_EFFECTV_END_DATE],
        [ETL_CURR_RCD_IND],
        [ETL_CREATED_TS],
        [ETL_UPDTD_TS],
        [Name Of Person Who Created The Asset],
        [Date On Which Asset Record was Created],
        [Asset Value Date Of The First Posting],
        [Asset Inventory Number],
        [Additional Asset Description],
        [Asset Serial Number],
        [Asset Cost Center],
        [Asset Plant]
    )
    SELECT
        S.[ASSET_KEY],
        S.[CMPNY_CD],
        S.[MAIN_ASSET_NUM],
        S.[ASSET_SUBNUMBER],
        S.[ASSET_CLASS],
        S.[ASSET_CLASS_DESC],
        S.[ASSET_DESC],
        S.[QTY],
        S.[BASE_UOM],
        S.[ASSET_CAPITALIZATION_DATE],
        S.[FIRST_ACQUISTN_FISCAL_YR],
        S.[FIRST_ACQUISTN_FISCAL_PER],
        S.[DEACTIVATION_DATE],
        S.[ETL_SRC_SYS_CD],
        S.[ETL_EFFECTV_BEGIN_DATE],
        S.[ETL_EFFECTV_END_DATE],
        S.[ETL_CURR_RCD_IND],
        S.[ETL_CREATED_TS],
        S.[ETL_UPDTD_TS],
        S.[NAME_OF_PERSON_WHO_CREATED_THE],
        S.[DATE_ON_WHICH_RCD_WAS_CREATED],
        S.[ASSET_VAL_DATE_OF_THE_FIRST_PST],
        S.[INVTRY_NUM],
        S.[ADDITIONAL_ASSET_DESC],
        S.[SERIAL_NUM],
        S.[COST_CNTR],
        S.[PLNT]
    FROM [GLOBAL_EDW_02_MIRROR].[GLOBAL_EDW].[EDW_T_D_FI_ASSETHR_CUR_D] S
    LEFT JOIN [GLOBAL_EDW].[dbo].[EDW_T_D_FI_ASSETHR_CUR_D] T
        ON T.[ASSET_KEY] = S.[ASSET_KEY]
    WHERE T.[ASSET_KEY] IS NULL;
END
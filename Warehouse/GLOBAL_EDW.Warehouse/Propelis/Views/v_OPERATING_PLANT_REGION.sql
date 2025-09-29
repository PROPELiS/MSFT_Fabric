-- Auto Generated (Do not modify) 0D772068C4F7D91FD8DEEE0D560321FBE45860002BDA1A2EA9AA421114FF919B
CREATE VIEW [Propelis].[v_OPERATING_PLANT_REGION]
    (
        [Operating Plant Business Function],
        [Operating Plant Region],
        [Operating Plant]
    ) AS
    SELECT
        [Business Function],
        [SGK Plant Region],
        [PLANT]
    FROM [GLOBAL_EDW].[dbo].[EDW_T_R_SGK_OPERREG_CUR_D];
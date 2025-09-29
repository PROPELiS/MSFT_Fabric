-- Auto Generated (Do not modify) CEF7A5D039610F0F4D1F8D9E40D72510EF1C299B51ACABB2490F3F15718B15C6
CREATE     view [Propelis].[V_PERSON_RESPNSBLE_COST_CNTR_H_HEIR] 
(
        [COST_CNTR_KEY],
    [Person Resp Cost Center Controlling Area ID HH],
    [Person Resp Cost Center Leaf level HH],
    [LVL_0_ID],
    [LVL_0_DESC],
    [Person Resp Cost Center Level 1 ID HH],
    [Person Resp Cost Center Level 1 Description HH],
    [Person Resp Cost Center Level 2 ID HH],
    [Person Resp Cost Center Level 2 Description HH],
    [Person Resp Cost Center Level 3 ID HH],
    [Person Resp Cost Center Level 3 Description HH],
    [Person Resp Cost Center Level 4 ID HH],
    [Person Resp Cost Center Level 4 Description HH],
    [Person Resp Cost Center Level 5 ID HH],
    [Person Resp Cost Center Level 5 Description HH],
    [Person Resp Cost Center Level 6 ID HH],
    [Person Resp Cost Center Level 6 Description HH],
    [Person Resp Cost Center Level 7 ID HH],
[Person Resp Cost Center Level 7 Description HH],
    [Person Resp Cost Center Level 8 ID HH],
    [Person Resp Cost Center Level 8 Description HH]

) AS
SELECT
    [COST_CNTR_KEY],
        [CONTROLLING_AREA_ID],
        [Cost Center ID Leaf level HH],
        [LVL_0_ID],
        [LVL_0_DESC],
        [Cost Center Level 1 ID HH],
        [Cost Center Level 1 Description HH],
        [Cost Center Level 2 ID HH],
        [Cost Center Level 2 Description HH],
        [Cost Center Level 3 ID HH],
        [Cost Center Level 3 Description HH],
        [Cost Center Level 4 ID HH],
        [Cost Center Level 4 Description HH],
        [Cost Center Level 5 ID HH],
        [Cost Center Level 5 Description HH],
        [Cost Center Level 6 ID HH],
        [Cost Center Level 6 Description HH],
        [Cost Center Level 7 ID HH],
        [Cost Center Level 7 Description HH],
        [Cost Center Level 8 ID HH],
        [Cost Center Level 8 Description HH]
FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCHZ_CUR_D];
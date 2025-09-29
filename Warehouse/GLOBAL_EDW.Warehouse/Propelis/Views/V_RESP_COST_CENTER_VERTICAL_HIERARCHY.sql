-- Auto Generated (Do not modify) 2B7F8FB51C13E71273ACE3B2904B222970BAD7BAE7B7D2A9E2FFC44BA4A1BC3A
CREATE       view [Propelis].[V_RESP_COST_CENTER_VERTICAL_HIERARCHY](
[COST_CNTR_KEY],
[Resp Cost Center Controlling Area ID VH],
[Resp Cost Center Parent ID VH],
[Resp Cost Center Parent Description VH],
[Resp Cost Center Child ID VH],
[Resp Cost Center Child Description VH],
[Resp Cost Center Hierarchical Level VH]
) AS
SELECT
[COST_CNTR_KEY],
[Cost Center Controlling Area ID VH],
[Cost Center Parent ID VH],
[Cost Center Parent Description VH],
[Cost Center Child ID VH],
[Cost Center Child Description VH],
[Cost Center Hierarchical Level VH]

FROM [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D];
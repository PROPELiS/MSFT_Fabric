-- Auto Generated (Do not modify) E03B6914BAC450A4D042404A4009AC4CE6C8C3EC08701058B56112F959BFB94B
CREATE  view [Propelis].[V_PARTNER_COST_CENTER_VERTICAL](
 [COST_CNTR_KEY],
[Partner Cost Center Controlling Area ID VH],
[Partner Cost Center Parent ID VH],
[Partner Cost Center Parent Description VH],
[Partner Cost Center Child ID VH],
[Partner Cost Center Child Description VH],
[Partner Cost Center Hierarchical Level VH]
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
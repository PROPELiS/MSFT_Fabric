-- Auto Generated (Do not modify) CB3DD0E2AD510829BC13DE2A4ACDAB923BB81893B556B258A0CD389445586D5F
CREATE   view [Propelis].[V_PERSON_RESPNSBLE_COST_CNTR_V_HEIR]
    (
        [COST_CNTR_KEY],
        [Person Resp Cost Center Controlling Area ID VH],
        [Person Resp Cost Center Parent ID VH],
        [Person Resp Cost Center Parent Description VH],
        [Person Resp Cost Center Child ID VH],
        [Person Resp Cost Center Child Description VH],
        [Person Resp Cost Center Hierarchical Level VH]
    )
as select 
        [COST_CNTR_KEY],
        [Cost Center Controlling Area ID VH],
        [Cost Center Parent ID VH],
        [Cost Center Parent Description VH],
        [Cost Center Child ID VH],
        [Cost Center Child Description VH],
        [Cost Center Hierarchical Level VH]
   from  [GLOBAL_EDW].[dbo].[EDW_T_D_HY_COSTCVZ_CUR_D];
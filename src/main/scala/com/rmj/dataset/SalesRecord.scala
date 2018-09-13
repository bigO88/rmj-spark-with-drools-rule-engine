package com.rmj.dataset

case class SalesRecord (
    Region:String,
    Country:String,
    ItemType:String,
    SalesChannel:String,
    OrderPriority:String,
    OrderDate:String,
    OrderID:Long,
    ShipDate:String,
    UnitsSold:String,
    UnitPrice:Double,
    UnitCost:Double,
    TotalRevenue:Double,
    TotalCost:Double,
    TotalProfit:Double
  )
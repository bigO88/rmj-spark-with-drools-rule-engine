package com.rmj.dataset

case class SalesRecord(
  var region: String,
  var country: String,
  var itemType: String,
  var salesChannel: String,
  var orderPriority: String,
  var orderDate: String,
  var orderID: Long,
  var shipDate: String,
  var unitsSold: String,
  var unitPrice: Double,
  var unitCost: Double,
  var totalRevenue: Double,
  var totalCost: Double,
  var totalProfit: Double,
  var total: Double
){

  def setTotal(x:Double)=this.total=x
  def getUnitCost():Double=this.unitCost
  def getTotalRevenue():Double= this.totalRevenue
  def getTotalCost():Double=this.totalCost
  def getTotalProfit():Double=this.totalProfit
}
package com.rmj.engine

object ScalaHashValue {


  def main(args: Array[String]): Unit = {
    val list = "Namibia,Sub-Saharan Africa,Namibia,Household,Offline,M,8/31/2015,897751939,10/12/2015,3604,668.27,502.54,2408445.08,1811154.16,597290.92".split(",").toList


    list.foreach(x=>{

      val hashCode = x.hashCode
      val partionId = Math.abs(hashCode%10)
      println("Partition of "+x+":  "+partionId+"   |   "+hashCode)

    })

    val amount=1000

    val price=333

    val totalCost = s"${amount+price}"

    println("TotalCost    :  "+totalCost)

  }
}

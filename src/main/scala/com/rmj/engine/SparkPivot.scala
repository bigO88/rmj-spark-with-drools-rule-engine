package com.rmj.engine


import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object SparkPivot {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Test pivot")
      .master("local[2]")
      .getOrCreate()

    val infile = args(0)  // Sample file here: /src/main/resources/sample_data.csv"
    val outpath = args(1)

    val rdd = spark.sparkContext.textFile(infile).map(x => Row.fromSeq(x.split(",")))

    val ss = StructType(Array(
      StructField("region", StringType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("itemType", StringType, nullable = false),
      StructField("salesChannel", StringType, nullable = false),
      StructField("orderPriority", StringType, nullable = false),
      StructField("orderDate", StringType, nullable = false),
      StructField("orderID", StringType, nullable = false),
      StructField("shipDate", StringType, nullable = false),
      StructField("unitsSold", StringType, nullable = false),
      StructField("unitPrice", StringType, nullable = false),
      StructField("unitCost", StringType, nullable = false),
      StructField("totalRevenue", StringType, nullable = false),
      StructField("totalCost", StringType, nullable = false),
      StructField("totalProfit", StringType, nullable = false)
    ))

    val queryname = "region,country,itemType,unitsSold".split(",").toList
    val groupedCols = "region,country".split(",").toList
    val df = spark.createDataFrame(rdd, schema = ss)

    val df1 = df.select(queryname.head, queryname.tail: _*)

    val df2 = df1.withColumn("unitsSoldN", df1("unitsSold").cast(LongType)).drop("unitsSold")

    val df3 = df2.groupBy(groupedCols.head,groupedCols.tail:_*).pivot("itemType").sum("unitsSoldN").filter( df2("region") === "Asia")

     df3.write.parquet(outpath)

    spark.stop()

  }
}

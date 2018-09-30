package com.rmj.engine


import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lead, lag}
import org.apache.spark.sql.expressions.Window


object SparkPivot {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val infile =  args(0)  // Sample file here: /src/main/resources/sample_data.csv"
    val outpath = args(1)

    val rdd = spark.sparkContext.textFile(infile).map(x => Row.fromSeq(x.split(",")))

    val inputRecordSchema = StructType(Array(
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

    val selectQuery = "region,country,itemType,salesChannel,orderDate,orderPriority,unitsSold,unitCost".split(",").toList

    val groupedCols = "region,country,salesChannel,orderPriority".split(",").toList

    val groupedCols1 = "region,country,itemType,salesChannel,orderPriority,orderDate".split(",").toList

    val partitionCols = "region,country,itemType,orderDate".split(",").toList

    val df = spark.createDataFrame(rdd, schema = inputRecordSchema)

    val df1 = df.select(selectQuery.head, selectQuery.tail: _*).filter(df("country") === "India")

    val getTotalAmount = udf {

      (x: Long, y: Double, z: String) =>
        z match {
          case "Online" => {
            if (x > 0) x * y - (x * y * 2) / 100 else 0
          }
          case _ => {
            if (x > 0) x * y else 0
          }
        }
    }

    val convertDateToYear = udf {
      x: String => x.split("/")(2)
    }

    val df2 = df1.withColumn("unitsSold", df1("unitsSold").cast(LongType))
      .withColumn("unitCost", df1("unitCost").cast(DoubleType))
      .withColumn("orderDate", convertDateToYear(df1("orderDate")))

    val df3 = df2.withColumn("totalAmount", getTotalAmount(df2("unitsSold"), df2("unitCost"), df2("salesChannel")))
      .drop("unitCost").drop("unitsSold").groupBy(groupedCols1.head, groupedCols1.tail: _*)
      .agg(sum("totalAmount").alias("totalAmount"))

    val window = org.apache.spark.sql.expressions.Window.partitionBy(partitionCols.head, partitionCols.tail: _*) orderBy ("orderDate")

    val df4 = df3.withColumn("pre_totalAmount", lag("totalAmount", 1, 0).over(window))

    val df5 = df4.groupBy(groupedCols.head, groupedCols.tail: _*)
      .pivot("itemType")
      .sum("totalAmount")

    df5.write.csv(outpath)

    val df6 = df4.withColumn("pre_totalAmount_1", lag("pre_totalAmount", 1, 0).over(window))

    df6.write.csv(outpath+"/pre-data")
    spark.stop()

  }
}

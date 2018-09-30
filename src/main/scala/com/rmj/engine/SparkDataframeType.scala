package com.rmj.engine


import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lead, lag}
import org.apache.spark.sql.expressions.Window


object SparkDataframeType {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val infile = args(0)  // Sample file here: /src/main/resources/sample_data.csv"
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

    val measures = "unitPrice,unitCost,totalRevenue,totalCost,totalProfit".split(",").toList

    val partitionCols = "region,country,itemType,orderDate".split(",").toList

    val convertDateToYear = udf {
      x: String => x.split("/")(2)
    }

    val convertStrToDouble = udf((x: Seq[String]) => x.map(_.toDouble))

    val df = spark.createDataFrame(rdd, schema = inputRecordSchema)

    val df1 = df.withColumn("measures", array(measures.head, measures.tail: _*))
      .drop("orderID", "shipDate", "unitsSold", "unitPrice", "unitCost", "totalRevenue", "totalCost", "totalProfit")
      .filter(df("country") === "India")
      .withColumn("orderDate", convertDateToYear(df("orderDate")))

    val df2 = df1.withColumn("measures", convertStrToDouble(df1("measures")))

    val window = Window.partitionBy(partitionCols.head, partitionCols.tail: _*) orderBy ("orderDate")

    val df3 = df2.withColumn("pre_measures", lag("measures", 1, null).over(window))

    df3.write.csv(outpath)

    spark.stop()

  }
}

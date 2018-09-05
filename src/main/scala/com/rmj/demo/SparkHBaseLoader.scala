package com.rmj.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable


object SparkHBaseLoader {


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("HBase fetch")
    val sc = new SparkContext(sparkConf)

    val conf = {
      HBaseConfiguration.create
    }

    val tableName = args(0)
    val outFilePath = args(1)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    rdd.saveAsTextFile(outFilePath)

    sc.stop()


  }


}

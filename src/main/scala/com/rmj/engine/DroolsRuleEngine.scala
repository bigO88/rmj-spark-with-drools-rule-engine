package com.rmj.engine

import com.rmj.dataset.SalesRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.kie.api.KieServices


object DroolsRuleEngine {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true).setAppName("drool integration with Spark").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val infile = args(0)  // Sample file here: /src/main/resources/sample_data.csv"

    val rdd = sc.textFile(infile).map { x =>
      SalesRecord(
        x.split(",")(0),
        x.split(",")(1),
        x.split(",")(2),
        x.split(",")(3),
        x.split(",")(4),
        x.split(",")(5),
        x.split(",")(6).toLong,
        x.split(",")(7),
        x.split(",")(8),
        x.split(",")(9).toDouble,
        x.split(",")(10).toDouble,
        x.split(",")(11).toDouble,
        x.split(",")(12).toDouble,
        x.split(",")(13).toDouble,
        0D
      )
    }

    rdd.filter(x => x.orderID > 897751939).foreach(println(_))

    val resultRDD = rdd.map { salesRecord =>

      val kieService = KieServices.Factory.get()
      val kieContainer = kieService.getKieClasspathContainer
      val kSession = kieContainer.newKieSession("ksession-rules")
      kSession.insert(salesRecord)
      kSession.fireAllRules()
      salesRecord
    }.filter(_.orderID > 897751939)

    resultRDD.foreach(x => println(x))

    sc.stop()

  }
}

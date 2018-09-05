val sparkContext = new SparkContext("local", "Simple App")
val hbaseConfiguration = (hbaseConfigFileName: String, tableName: String) => {
  val hbaseConfiguration = HBaseConfiguration.create()
  hbaseConfiguration.addResource(hbaseConfigFileName)
  hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, tableName)
  hbaseConfiguration
  }
val rdd = sparkContext.newAPIHadoopRDD(
  hbaseConfiguration("/usr/lib/hbase/conf/hbase-site.xml", "test"),
  classOf[TableInputFormat],
  classOf[ImmutableBytesWritable],
  classOf[Result]
)
import scala.collection.JavaConverters._
rdd
  .map(tuple => tuple._2)
  .map(result => result.getColumn("columnFamily".getBytes(), "columnQualifier".getBytes()))
  .map(keyValues => {
  keyValues.asScala.reduceLeft {
    (a, b) => if (a.getTimestamp > b.getTimestamp) a else b
  }.getValue
})

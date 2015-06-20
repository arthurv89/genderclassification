import org.apache.spark.{SparkContext, SparkConf}

trait Pipelines {
  val sparkContext = new SparkContext(new SparkConf().setAppName("Application").setMaster("local"))
}

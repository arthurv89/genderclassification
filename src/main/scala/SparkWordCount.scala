import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

  def main(args: Array[String]) {
    val tokenized: RDD[String] = sc.textFile("input/words.txt")
      .flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts: RDD[(String, Int)] = tokenized.map((_, 1))
      .reduceByKey(_ + _)

    System.out.println(wordCounts.collect.mkString(", "))
  }
}
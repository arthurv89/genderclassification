import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationRF extends GenderClassificationData {
  val gender = Map(
    "1 0 0" -> 0,
    "0 1 0" -> 1)

  val dataset = userToUnitVectorCategories.join(userToGender)
    .values
    .map((x: (Iterable[Double], String)) => {
      val label = gender(x._2)
      val features = x._1.toArray

      new LabeledPoint(label, Vectors.dense(features))
    })

//  userToUnitVectorCategories.saveAsTextFile("results/dump/userToUnitVectorCategories")

  override implicit lazy val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Gender classification using RF").setMaster("local"))

  val numClasses = 2
  def main(args: Array[String]) = new RandomForestExecutor(dataset, numClasses).start
}

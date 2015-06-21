package genderclassification.svm

import genderclassification.GenderClassificationData
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GenderClasificationSVM extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Gender classification using SVM").setMaster("local"))

  val gender = Map(
    "1 0 0" -> 0,
    "0 1 0" -> 1)

  val labeledDataset: RDD[LabeledPoint] = userToUnitVectorCategories
    .join(userToGender) // (User, (RelativeCategoryCount, Gender))
    .values // (RelativeCategoryCount, Gender)
    .map((x: (Iterable[Double], String)) => { // (LabeledPoint)
      val label = gender(x._2)
      val features = x._1.toArray

      new LabeledPoint(label, Vectors.dense(features))
    })

  val numClasses = 2
  def main(args: Array[String]) = new SVMExecutor(labeledDataset, numClasses).start()
}

package genderclassification.svm

import genderclassification.GenderClassificationData
import org.apache.spark.{SparkConf, SparkContext}

object GenderClasificationSVM extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using SVM")
      .setMaster("local"))

  def main(args: Array[String]) = new SVMExecutor(
    dataset = labeledDataset,
    numClasses = 2
  ).start()
}

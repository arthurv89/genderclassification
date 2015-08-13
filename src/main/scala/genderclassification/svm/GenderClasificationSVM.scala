package genderclassification.svm

import genderclassification.GenderClassificationData
import org.apache.spark.{SparkConf, SparkContext}

object GenderClasificationSVM {
  implicit val sc = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using SVM"))

  def main(args: Array[String]) = new SVMExecutor(
    dataset = GenderClassificationData.labeledDataset(sc),
    numClasses = 2
  ).start()
}

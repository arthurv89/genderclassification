package genderclassification.randomforest

import genderclassification.GenderClassificationData
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationRandomForest {
  implicit val sc = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using a Random Forest"))

  def main(args: Array[String]) = new RandomForestExecutor(
    dataset = GenderClassificationData.labeledDataset(sc),
    numClasses = 2
  ).start()
}

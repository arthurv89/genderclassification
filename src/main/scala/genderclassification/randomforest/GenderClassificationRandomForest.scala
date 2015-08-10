package genderclassification.randomforest

import genderclassification.GenderClassificationData
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationRandomForest extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using a Random Forest"))

  def main(args: Array[String]) = new RandomForestExecutor(
    dataset = labeledDataset
    ,
    numClasses = 2
  ).start()
}

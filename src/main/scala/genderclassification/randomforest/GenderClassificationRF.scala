package genderclassification.randomforest

import genderclassification.GenderClassificationData
import genderclassification.GenderDataset.labeledDataset
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationRF extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using a Random Forest")
      .setMaster("local"))

  def main(args: Array[String]) = new RandomForestExecutor(
    dataset = labeledDataset(userToUnitVectorCategories, userToGender),
    numClasses = 2
  ).start()
}

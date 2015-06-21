package genderclassification.naivebayes

import genderclassification.GenderClassificationData
import genderclassification.GenderDataset.labeledDataset
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationNaiveBayes extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using Naive bayes")
      .setMaster("local"))

  def main(args: Array[String]) = new NaiveBayesExecutor(
    dataset = labeledDataset(userToUnitVectorCategories, userToGender),
    numClasses = 2
  ).start()
}

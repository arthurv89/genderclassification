package genderclassification.naivebayes

import genderclassification.GenderClassificationData
import org.apache.spark.{SparkConf, SparkContext}

object GenderClassificationNaiveBayes {
  implicit val sc = new SparkContext(
    new SparkConf()
      .setAppName("Gender classification using Naive bayes"))

  def main(args: Array[String]) = new NaiveBayesExecutor(
    dataset = GenderClassificationData.labeledDataset(sc),
    numClasses = 2
  ).start()
}

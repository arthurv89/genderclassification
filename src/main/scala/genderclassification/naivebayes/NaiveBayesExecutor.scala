package genderclassification.naivebayes

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class NaiveBayesExecutor(dataset: RDD[LabeledPoint], numClasses: Int, seed: Long = 11L)(implicit sc: SparkContext) {
  // Split the data into Training and test Sets(30% Held out for Testing)
  val splits = dataset.randomSplit(Array(0.7, 0.3), seed)
  val (trainingData, testData) = (splits(0), splits(1))
  trainingData.cache()

  val outputLocation = "results/" + sc.appName + "/" + System.currentTimeMillis

  def start() = {
    val model = NaiveBayes.train(trainingData, lambda = 1.0)

    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
    println(accuracy)
  }
}

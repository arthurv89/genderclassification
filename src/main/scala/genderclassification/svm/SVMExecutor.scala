package genderclassification.svm

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class SVMExecutor(dataset: RDD[LabeledPoint], numClasses: Int, seed: Long = 11L)(implicit sc: SparkContext) {
  // Split the data into Training and test Sets(30% Held out for Testing)
  val splits = dataset.randomSplit(Array(0.7, 0.3), seed)
  val (trainingData, testData) = (splits(0), splits(1))
  trainingData.cache()

  def start() = {
    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(trainingData, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "results/dump/myModelPath")
  }
}

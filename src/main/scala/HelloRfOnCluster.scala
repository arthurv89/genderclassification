import java.util.UUID

import org.apache.spark._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable.ListBuffer

object HelloRfOnCluster extends Logging {
  val outputLocation = "results"

  val conf = new SparkConf().setAppName("HelloRfOnCluster").setMaster("local")
  val sc = new SparkContext(conf)

  // Load and parse the data file.
  // libsvm Style iris Data - http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale
  val dataset = MLUtils.loadLibSVMFile(sc, "iris.data")

  // Split the data into Training and test Sets(30% Held out for Testing)
  val splits = dataset.randomSplit(Array(0.7, 0.3))
  val (trainingData, testdata) = (splits(0), splits(1))

  val buf = new ListBuffer[String]

  def main(args: Array[String]): Unit = {
    buf.append("Start")

    trainClassifier()

    buf.append("End")

    val summaryRDD = sc.makeRDD[String](buf, 1)
    summaryRDD.saveAsTextFile(outputLocation + "/summary")
  }

  def trainClassifier() = {
    val startTime = System.currentTimeMillis

    // Train A RandomForest model.
    // Empty CategoricalFeaturesInfo Indicates all Features Are continuous.
    val numClasses = 4 // Iris data:. 3 Labels,(label + 1) value seems to BE needed
    val categoricalFeaturesInfo  =  Map [ Int, Int ] ( )
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "entropy"
    val numTrees = 100 // Use more in Practice.

    val results =
      for(maxDepth <- 1 to 20; maxBins <- 2 to 10 ) yield {
        val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        // Evaluate model on test instances and Compute test error
        val labelAndPredsRDD = testdata.zipWithIndex.map {
          case (current, index) =>
            val predictionResult = model.predict(current.features)
            (index, current.label, predictionResult, current.label == predictionResult) // Tuple4
        }

        val exectime = System.currentTimeMillis - startTime

        val testDataCount = testdata.count()
        val testErrCount = labelAndPredsRDD.filter(r => !r._4).count // R._4 =  4th element of tuple(Current.Label = =  PredictionResult)
        val testSuccessRate = 100 - (testErrCount.toDouble / testDataCount * 100)

        buf.append("RfClassifier Results:" + testSuccessRate + "% numTrees:" + numTrees + "maxDepth:" + maxDepth + "exectime(msec):" + exectime)
        buf.append("Test Data Count = " + testDataCount)
        buf.append("Test Error Count = " + testErrCount)
        buf.append("Test Success Rate(%) = " + testSuccessRate)
        buf.append("Learned classification Forest model: \n" + model.toDebugString)

        labelAndPredsRDD.map(x => x.toString()).saveAsTextFile(outputLocation + "/detail/" + System.currentTimeMillis + "-" + UUID.randomUUID() + "/details")

        (testSuccessRate, maxDepth, maxBins)
      }

    val sortedList = results.sortWith((x, y) => x._1 > y._1)

    buf.prepend("Results:\n\t" + sortedList.mkString("\n\t") + "\n\n")
  }
}
package examples

import genderclassification.GenderClassificationData
import genderclassification.randomforest.RandomForestExecutor
import org.apache.spark._
import org.apache.spark.mllib.util.MLUtils

object IrisRF extends GenderClassificationData {
  override implicit lazy val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("Iris random forest example")
      .setMaster("local"))

  def main(args: Array[String]) = new RandomForestExecutor(
    // libsvm Style iris Data - http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale
    dataset = MLUtils.loadLibSVMFile(sc, "input/iris.scale"),
    numClasses = 4)
  .start()
}
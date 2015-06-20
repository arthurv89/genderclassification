import org.apache.spark._
import org.apache.spark.mllib.util.MLUtils

object IrisRF extends Logging with GenderClassificationData {
  override implicit val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Iris random forest example").setMaster("local"))

  // libsvm Style iris Data - http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale
  def dataset = MLUtils.loadLibSVMFile(sc, "input/iris.scale")

  new RandomForestExecutor(dataset, 4)
}
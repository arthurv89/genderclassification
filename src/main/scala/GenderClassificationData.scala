import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait GenderClassificationData {
  implicit val sc: SparkContext

  val categories: RDD[String] = sc.textFile("input/distinct_category.txt")
  val categoriesToIndex: RDD[(String, Long)] = categories.zipWithIndex().cache()

  val userToProduct = sc.textFile("input/user_product_add_2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val userToGender: RDD[(String, String)] = sc.textFile("input/new_userId_gender.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val productToCategory = sc.textFile("input/product_to_category_lv2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val productToCategoryIndex: RDD[(String, Long)] = productToCategory
    .map(_.swap)
    .join(categoriesToIndex)
    .map(t => (t._2._1, t._2._2))

  val productToUser = userToProduct.map(_.swap)
  val activeUserToCategories: RDD[(String, Iterable[Long])] = productToUser.join(productToCategoryIndex)
    .values
    .groupByKey()

  val categoryIndex: RDD[Long] = sc.parallelize(0L to categoriesToIndex.count())
    .cache()

  val userAndCategoriesToZero: RDD[((String, Long), Int)] = userToProduct
    .keys
    .distinct
    .cartesian(categoryIndex)
    .map(x => ((x._1, x._2), 0))

  val userAndCategories = activeUserToCategories
    .flatMapValues(x => x)
    .map(x => ((x._1, x._2), 1))

  val userToCategories: RDD[(String, Iterable[((String, Long), Int)])] = userAndCategoriesToZero.union(userAndCategories)
    .reduceByKey(_ + _)
    .groupBy(_._1._1)

  val userToCategoryCounts: RDD[(String, Iterable[(Long, Int)])] = userToCategories.mapValues(categories => {
    categories.map(a => (a._1._2, a._2))
  })
  .sortBy(_._1)

  val userToUnitVectorCategories = userToCategoryCounts
    .mapValues(_.map(_._2))
    .mapValues(categories => {
      // Make unit vector
      val sum = categories.sum
      categories.map(x => {
        if(sum == 0) {
          0.0
        } else {
          x.toDouble / sum
        }
      })
    })
}

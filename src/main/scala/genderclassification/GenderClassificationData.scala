package genderclassification

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait GenderClassificationData {
  implicit val sc: SparkContext


  // Data sources
  val categories: RDD[String] = sc.textFile("input/distinct_category.txt")
  val userToProduct: RDD[(String, String)] = sc.textFile("input/user_product_add_2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val userToGender: RDD[(String, String)] = sc.textFile("input/new_userId_gender.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val productToCategory: RDD[(String, String)] = sc.textFile("input/product_to_category_lv2.txt").map(_.split("\t")).map(x => (x(0), x(1)))

  val categoriesToIndex: RDD[(String, Long)] = categories.zipWithIndex().cache()
  val categoryIndex: RDD[Long] = sc.parallelize(0L to categoriesToIndex.count())
    .cache()

  val productToCategoryIndex: RDD[(String, Long)] = productToCategory
    .map(_.swap) // (Category, Product)
    .join(categoriesToIndex) // (Category, (Product, CategoryIndex)
    .map(t => (t._2._1, t._2._2)) // (Product, CategoryIndex)

  val activeUserToCategoryIndexes: RDD[(String, Iterable[Long])] = userToProduct
    .map(_.swap) // (Product, User)
    .join(productToCategoryIndex) // (Product, (User, CategoryIndex))
    .values // (User, CategoryIndex)
    .groupByKey() // (User, [CategoryIndex])

  val userToUnitVectorCategories: RDD[(String, Iterable[Double])] = {
    val userAndCategoriesToZero = userToProduct
      .keys // (User)
      .distinct()
      .cartesian(categoryIndex) // (User, CategoryIndex)
      .map(x => ((x._1, x._2), 0)) // ((User, CategoryIndex), 0)

    val userAndCategories = activeUserToCategoryIndexes
      .flatMapValues(x => x) // (User, CategoryIndex)
      .map(x => ((x._1, x._2), 1)) // ((User, CategoryIndex), 1)

    userAndCategoriesToZero.union(userAndCategories)
      .reduceByKey(_ + _) // (User, CategoryIndex) -> count
      .groupBy(_._1._1) // (User, CategoryIndex) -> [count]
      .mapValues(_.map(a => (a._1._2, a._2))) //  (User -> (CategoryIndex, Count))
      .sortBy(_._1)
      .mapValues(x => {  //  (User -> [Ordered CountUnitVector])
        // Make unit vector
        val counts = x.map(_._2) // Ordered counts
        val countSum = counts.sum
        counts.map(x => {
          if(countSum == 0) 0.0
          else              x.toDouble / countSum
        })
      })
  }
}

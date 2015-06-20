import org.apache.spark.rdd.RDD

trait GenderClassificationData extends Pipelines {
  val userToProduct = sparkContext.textFile("input/user_product_add_2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val userToGender = sparkContext.textFile("input/new_userId_gender.txt").flatMap(_.split("\t")).map(x => (x(0), x(1)))
  val productToCategory = sparkContext.textFile("input/product_to_category_lv2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val categories: RDD[String] = sparkContext.textFile("input/distinct_category.txt")
  val categoriesToIndex: Map[String, Long] = categories.zipWithIndex.collect.toMap

  val productToUser = userToProduct.map(x => (x._2, x._1))
  val userToCategories: RDD[(String, Iterable[String])] = productToUser.join(productToCategory).values
    .groupByKey

  val userToUnitVectorCategories: RDD[(String, List[Double]] = userToCategories.map(t => {
    val relativeCount: List[Int] = t._2
      .groupBy(a => a) // Group: Category -> [Category]
      .mapValues(_.size) // Category -> count
      .map(x => { // CategoryIndex -> count
        val categoryIndex = categoriesToIndex.get(x._1).get
        (categoryIndex, x._2)
      })
      .toList
      .sortBy(_._1) // Sort by index
      .map(_._2) // Only retain count

    // Make unit vector
    val sum = relativeCount.sum
    val unitVector: List[Double] = relativeCount.map(_.toDouble / sum)

    (t._1, unitVector)
  })

//  val userToNull = userGenderLines.
}

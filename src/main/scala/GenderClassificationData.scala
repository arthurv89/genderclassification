

trait GenderClassificationData extends Pipelines {
  val userToProduct = sparkContext.textFile("input/user_product_add_2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  val userToGender = sparkContext.textFile("input/new_userId_gender.txt").flatMap(_.split("\t")).map(x => (x(0), x(1)))
  val productToCategory = sparkContext.textFile("input/product_to_category_lv2.txt").map(_.split("\t")).map(x => (x(0), x(1)))
  //  val categoryLines = sparkContext.textFile("input/distinct_category.txt")

  val productToUser = userToProduct.map(x => (x._2, x._1))
  val userToCategory = productToUser.join(productToCategory).values.groupByKey

//  val userToNull = userGenderLines.
}

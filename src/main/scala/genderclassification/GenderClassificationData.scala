package genderclassification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

trait GenderClassificationData {
  implicit val sc: SparkContext

  def readCsv(file: String) = sc.textFile(file)
    .filter(!_.charAt(0).isLetter)
    .map(_.split(",", 2) match { case Array(x, y) => (x,y) })

  // Data sources
  val category_product = readCsv("input/acc_dataset/category_product.csv") // CategoryId, GlobalId
  val category_shop = readCsv("input/acc_dataset/category_shop.csv") // CategoryId, ShopName
  val order_user = readCsv("input/acc_dataset/order_customer.csv") // OrderId, UserId
  val order_product = readCsv("input/acc_dataset/order_product.csv") // OrderId, GlobalId
  val user_gender = readCsv("input/acc_dataset/user_gender.csv") // UserId, Gender

  val shop_shopIndex: RDD[(String, Long)] = category_shop.values.zipWithIndex().cache()
  val shopIndex: RDD[Long] = sc.parallelize(0L to shop_shopIndex.count())
    .cache()

  val productToShopIndex: RDD[(String, Long)] = category_shop
    .join(category_product) // (Category, (Product, Shop))
    .values // (Shop, Product)
    .join(shop_shopIndex) // (Shop, (Product, ShopIndex)
    .values // (Product, ShopIndex)

  val activeUserToShopIndexes: RDD[(String, Long)] = order_product
    .join(order_user) // (Order, (Product, User))
    .values // (Product, User)
    .join(productToShopIndex) // (Product, (User, ShopIndex))
    .values // (User, ShopIndex)

  val userToUnitVectorCategories: RDD[(String, Iterable[Double])] = {
    val userAndCategoriesToZero = order_user
      .values // (User)
      .distinct()
      .cartesian(shopIndex) // (User, ShopIndex)
      .map(x => (x, 0)) // ((User, ShopIndex), 0)

//      : RDD[((String, Iterable[Long]), Int)]
    val userAndCategories = activeUserToShopIndexes
      .map(x => (x, 1)) // ((User, ShopIndex), 1)

    userAndCategoriesToZero.union(userAndCategories)
      .reduceByKey(_ + _) // (User, ShopIndex) -> count
      .groupBy(_._1._1) // (User, ShopIndex) -> [count]
      .mapValues(_.map(a => (a._1._2, a._2))) //  (User -> (ShopIndex, Count))
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


  val labeledDataset = userToUnitVectorCategories
    .join(user_gender) // (User, (RelativeCategoryCount, Gender))
    .values // (RelativeCategoryCount, Gender)
    .map((x: (Iterable[Double], String)) => { // (LabeledPoint)
      val label = if(x._2 == "1 0 0") 0 else 1
      val features = x._1.toArray

      new LabeledPoint(label, Vectors.dense(features))
  })
}

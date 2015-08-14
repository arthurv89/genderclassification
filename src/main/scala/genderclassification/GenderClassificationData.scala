package genderclassification

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.Map

object GenderClassificationData {
  def labeledDataset(sc: SparkContext) = {
    val readCsv = (file: String) => sc.textFile(file)
      .filter(!_.charAt(0).isLetter)
      .map(_.split(",", 2) match { case Array(x, y) => (x, y) })
      .filter(t => !t._1.isEmpty && !t._2.isEmpty)

    val joinISL = (d1: RDD[(Int, String)], d2: Broadcast[Map[Int, Long]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinSL = (d1: RDD[(String, Long)], d2: Broadcast[Map[String, Long]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinSLI = (d1: RDD[(String, Long)], d2: Broadcast[Map[String, Int]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinLL = (d1: RDD[(Long, Long)], d2: Broadcast[Map[Long, Long]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinLLI = (d1: RDD[(Long, Long)], d2: Broadcast[Map[Long, Int]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinLIB = (d1: RDD[(Long, Int)], d2: Broadcast[Map[Long, Boolean]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))

    val joinILS = (d1: RDD[(Int, Long)], d2: Broadcast[Map[Int, String]]) => d1
      .flatMap(x => d2.value.get(x._1).map(v => (x._2, v)))



    // Data sources
    val category_product = readCsv("input/acc_dataset/category_product.csv").map(t => (t._1.toInt, t._2.toLong)) // CategoryId, GlobalId
    val category_shop = readCsv("input/acc_dataset/category_shop.csv").map(t => (t._1.toInt, t._2)) // CategoryId, ShopName
    val order_user = readCsv("input/acc_dataset/order_customer.csv").map(t => (t._1.toLong, t._2.toLong)).sample(false, 0.0001) // OrderId, UserId
    val order_product = readCsv("input/acc_dataset/order_product.csv").map(t => (t._1.toLong, t._2.toLong)) // OrderId, GlobalId
    val user_gender = readCsv("input/acc_dataset/user_gender.csv").map(t => (t._1.toLong, if (t._2 == "M") true else false)) // UserId, Gender

    val shop_shopIndex = category_shop.values.zipWithIndex().map(x => (x._1, x._2.toInt))

    val category_shopBc = sc.broadcast(category_shop.collectAsMap())
    val shop_product = joinILS(category_product, category_shopBc).map(_.swap) // (Shop, Product)
    category_shopBc.unpersist()
    category_product.unpersist()

    val shop_shopIndexBc = sc.broadcast(shop_shopIndex.collectAsMap())
    val product_shopIndex = joinSLI(shop_product, shop_shopIndexBc) // (Product, ShopIndex)
    shop_shopIndexBc.unpersist()
    shop_product.unpersist()

    val order_userBc = sc.broadcast(order_user.collectAsMap())
    val product_user = joinLL(order_product, order_userBc) // (Product, User)
    order_userBc.unpersist()
    order_product.unpersist()

    val product_shopIndexBc = sc.broadcast(product_shopIndex.collectAsMap())
    val activeUser_shopIndexes = joinLLI(product_user, product_shopIndexBc) // (User, ShopIndex)
    product_shopIndexBc.unpersist()
    product_user.unpersist()

    val user_genderBc = sc.broadcast(user_gender.collectAsMap())
    val shopIndex_gender = joinLIB(activeUser_shopIndexes, user_genderBc) // (shopIndex, gender)
    user_genderBc.unpersist()
    activeUser_shopIndexes.unpersist()

    shopIndex_gender
      .map((_, 1)) // ((shopIndex, gender), 1)
      .reduceByKey(_ + _) // ((shopIndex, gender), count)
      .map{ case ((shopIndex, gender), count) => (gender, (shopIndex, count))}
      .groupByKey() // (gender, [(shopIndex, count)])
      .mapValues(v => v
          .toList // [(shopIndex, count)]
          .sortBy(_._1) // Sort on shopIndex
          .map { case (shopIndex, count) =>
              if (count == 0) 0.0
              else count.toDouble / v.map(_._2).sum
          }
      )
      .map{ case (gender, counts) => new LabeledPoint(
          label = if (gender) 0 else 1,
          features = Vectors.dense(counts.toArray))
      }
  }
}
